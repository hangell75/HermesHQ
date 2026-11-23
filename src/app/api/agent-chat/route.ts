import { NextRequest, NextResponse } from 'next/server';
import { prisma } from '@/lib/prisma';

export const dynamic = 'force-dynamic';
export const revalidate = 0;

interface AgentChatRequest {
  agentId: string;
  message: string;
  history?: Array<{ role: 'user' | 'assistant'; content: string }>;
}

// Chat is executed by the hermes-bridge as `hermes -p <agentId> -z <prompt>`.
// Agent runs take minutes, so the request must never be held open across the
// reverse proxy (nginx proxy_read_timeout is 90s and would answer with a 504
// HTML page, breaking JSON parsing client-side). POST only enqueues and
// returns the request id; the client polls GET for status until done/failed.
const PROFILE_ID = /^[a-z0-9][a-z0-9_-]*$/i;

function buildPrompt(message: string, history: AgentChatRequest['history'] = []): string {
  const turns = (history || [])
    .slice(-6)
    .map((h) => `${h.role === 'user' ? 'User' : 'Assistant'}: ${h.content}`)
    .join('\n');
  return turns ? `${turns}\nUser: ${message}` : message;
}

export async function POST(request: NextRequest): Promise<NextResponse<{ requestId: string; status: string } | { error: string }>> {
  try {
    const { agentId, message, history = [] } = (await request.json()) as AgentChatRequest;

    if (!agentId || !message) {
      return NextResponse.json({ error: 'Missing agentId or message' }, { status: 400 });
    }
    if (!PROFILE_ID.test(agentId)) {
      return NextResponse.json({ error: `Unknown agent: ${agentId}` }, { status: 400 });
    }

    const row = await prisma.agentRequest.create({
      data: {
        origin: 'web',
        kind: 'chat',
        title: message.slice(0, 80),
        prompt: buildPrompt(message, history),
        agentId: agentId.toLowerCase(),
        sideEffecting: false,
        status: 'queued',
      },
    });

    return NextResponse.json({ requestId: row.id, status: 'queued' }, { status: 202 });
  } catch (error) {
    console.error('Agent chat error:', error);
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 });
  }
}

export async function GET(request: NextRequest): Promise<NextResponse<{ status: string; reply?: string; error?: string } | { error: string }>> {
  try {
    const id = request.nextUrl.searchParams.get('id');
    if (!id) {
      return NextResponse.json({ error: 'Missing id' }, { status: 400 });
    }
    const r = await prisma.agentRequest.findUnique({ where: { id } });
    if (!r) {
      return NextResponse.json({ error: `Unknown request: ${id}` }, { status: 404 });
    }
    if (r.status === 'done') return NextResponse.json({ status: r.status, reply: r.result || '' });
    if (r.status === 'failed') return NextResponse.json({ status: r.status, error: r.error || 'Agent run failed' });
    return NextResponse.json({ status: r.status });
  } catch (error) {
    console.error('Agent chat status error:', error);
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 });
  }
}
