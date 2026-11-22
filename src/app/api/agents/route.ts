import { NextResponse } from "next/server";
import { prisma } from "@/lib/prisma";

export const dynamic = "force-dynamic";
export const revalidate = 0;

// Default agent roster
const DEFAULT_AGENTS = [
  {
    id: "max",
    name: "Max",
    emoji: "\uD83D\uDC3A",
    role: "Chief of Staff \u00B7 Orchestrator",
    status: "online",
    tasksCompleted: 0,
    totalCost: 0,
    recentActivity: [],
  },
  {
    id: "sage",
    name: "Sage",
    emoji: "\uD83C\uDF3F",
    role: "X Content Specialist \u00B7 Trend Scout",
    status: "idle",
    tasksCompleted: 0,
    totalCost: 0,
    recentActivity: [],
  },
  {
    id: "knox",
    name: "Knox",
    emoji: "\uD83D\uDD10",
    role: "Trading Operations \u00B7 Bot Monitor",
    status: "idle",
    tasksCompleted: 0,
    totalCost: 0,
    recentActivity: [],
  },
  {
    id: "nova",
    name: "Nova",
    emoji: "\u2B50",
    role: "YouTube Strategy \u00B7 Content Research",
    status: "idle",
    tasksCompleted: 0,
    totalCost: 0,
    recentActivity: [],
  },
  {
    id: "pixel",
    name: "Pixel",
    emoji: "\uD83C\uDFA8",
    role: "Web App Specialist \u00B7 Product Ideas",
    status: "idle",
    tasksCompleted: 0,
    totalCost: 0,
    recentActivity: [],
  },
];

export async function GET() {
  try {
    // Roster is driven by the hermes-bridge mirror of Hermes profiles (role prefix "Hermes").
    const states = await prisma.agentState.findMany({
      where: { role: { startsWith: "Hermes" } },
      orderBy: { name: "asc" },
    });

    const agents = states.map((s) => ({
      id: s.id,
      name: s.name,
      emoji: s.emoji || "\u{1F916}",
      role: s.role || "Hermes \u00B7 agent",
      status: s.status || "offline",
      tasksCompleted: s.tasksCompleted || 0,
      totalCost: s.totalCost || 0,
      currentTask: s.currentTask || undefined,
      lastActive: s.lastActive || undefined,
      recentActivity: Array.isArray(s.recentActivity) ? s.recentActivity : [],
    }));

    return NextResponse.json(agents, {
      headers: { "Cache-Control": "no-store, no-cache, must-revalidate" },
    });
  } catch (error) {
    console.error("Agents API error:", error);
    return NextResponse.json([], { status: 200 });
  }
}

// POST to update agent state (called by cron jobs)
export async function POST(request: Request) {
  try {
    const body = await request.json();
    const { agentId, action, status, currentTask } = body;

    if (!agentId) {
      return NextResponse.json({ error: "agentId required" }, { status: 400 });
    }

    // Find the default agent info for name/emoji/role
    const defaultAgent = DEFAULT_AGENTS.find((a) => a.id === agentId);

    // Get existing state or create defaults
    let existing = await prisma.agentState.findUnique({ where: { id: agentId } });

    const recentActivity = (existing?.recentActivity as any[]) || [];
    const newRecentActivity = action
      ? [
          { timestamp: new Date().toISOString(), action },
          ...recentActivity.slice(0, 19),
        ]
      : recentActivity;

    const updatedState = await prisma.agentState.upsert({
      where: { id: agentId },
      update: {
        ...(status ? { status } : {}),
        ...(currentTask !== undefined ? { currentTask } : {}),
        lastActive: new Date(),
        ...(action
          ? {
              recentActivity: newRecentActivity,
              tasksCompleted: (existing?.tasksCompleted || 0) + 1,
            }
          : {}),
      },
      create: {
        id: agentId,
        name: defaultAgent?.name || agentId,
        emoji: defaultAgent?.emoji,
        role: defaultAgent?.role,
        status: status || "idle",
        currentTask: currentTask || null,
        lastActive: new Date(),
        tasksCompleted: action ? 1 : 0,
        totalCost: 0,
        recentActivity: newRecentActivity,
      },
    });

    return NextResponse.json({ ok: true, agent: updatedState });
  } catch (error) {
    console.error("Agent update error:", error);
    return NextResponse.json({ error: "Failed to update" }, { status: 500 });
  }
}
