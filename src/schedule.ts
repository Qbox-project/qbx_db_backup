import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";

export const STARTUP_GRACE_MS = 60_000;
export const STATE_FILE_NAME = ".state.json";
export const HOUR_MS = 3_600_000;

export function nextRunAt(lastRunAt: number | null, intervalHours: number, now: number): number {
  if (lastRunAt !== null) {
    const due = lastRunAt + intervalHours * HOUR_MS;
    if (due > now) return due;
  }
  return now + STARTUP_GRACE_MS;
}

export async function readLastRunAt(dir: string): Promise<number | null> {
  try {
    const raw = await readFile(path.join(dir, STATE_FILE_NAME), "utf8");
    const parsed = JSON.parse(raw) as { lastRunAt?: unknown };
    const value = parsed?.lastRunAt;
    return typeof value === "number" && Number.isFinite(value) ? value : null;
  } catch {
    return null;
  }
}

export async function writeLastRunAt(dir: string, lastRunAt: number): Promise<void> {
  await mkdir(dir, { recursive: true });
  await writeFile(path.join(dir, STATE_FILE_NAME), `${JSON.stringify({ lastRunAt })}\n`, "utf8");
}
