import { NextRequest, NextResponse } from 'next/server';
import { prisma } from '@/lib/prisma';

export const dynamic = 'force-dynamic';
export const revalidate = 0;

interface AgentChatRequest {
  agentId: string;
  message: string;
  history?: Array<{ role: 'user' | 'assistant'; content: string }>;
}

interface AgentChatResponse {
  reply: string;
  agentId: string;
}

// Chat is executed by the hermes-bridge as `hermes -p <agentId> -z <prompt>`;
// this route only enqueues an AgentRequest and waits for the bridge to write the result.
const MAX_WAIT_MS = 300_000; // must exceed the bridge RUN_TIMEOUT_MS + poll interval
const PROFILE_ID = /^[a-z0-9][a-z0-9_-]*$/i;

function buildPrompt(message: string, history: AgentChatRequest['history'] = []): string {
  const turns = (history || [])
    .slice(-6)
    .map((h) => `${h.role === 'user' ? 'User' : 'Assistant'}: ${h.content}`)
    .join('\n');
  return turns ? `${turns}\nUser: ${message}` : message;
}

export async function POST(request: NextRequest): Promise<NextResponse<AgentChatResponse | { error: string }>> {
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

    const deadline = Date.now() + MAX_WAIT_MS;
    while (Date.now() < deadline) {
      const r = await prisma.agentRequest.findUnique({ where: { id: row.id } });
      if (!r) break;
      if (r.status === 'done') return NextResponse.json({ reply: r.result || '', agentId });
      if (r.status === 'failed') return NextResponse.json({ error: r.error || 'Agent run failed' }, { status: 502 });
      if (r.status === 'rejected') return NextResponse.json({ error: 'Request rejected' }, { status: 400 });
      await new Promise((res) => setTimeout(res, 2000));
    }
    return NextResponse.json({ error: 'Timed out waiting for agent' }, { status: 504 });
  } catch (error) {
    console.error('Agent chat error:', error);
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 });
  }
}
