import { describe, expect, it } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import {
  HOUR_MS,
  nextRunAt,
  readLastRunAt,
  STARTUP_GRACE_MS,
  STATE_FILE_NAME,
  writeLastRunAt,
} from "./schedule";

const NOW = Date.UTC(2026, 8, 6, 12, 0, 0);

async function withTempDir(body: (dir: string) => Promise<void>): Promise<void> {
  const dir = await mkdtemp(path.join(tmpdir(), "qbx-schedule-"));
  try {
    await body(dir);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}

describe("nextRunAt", () => {
  it("keeps a due time that is still in the future", () => {
    expect(nextRunAt(NOW - HOUR_MS, 24, NOW)).toBe(NOW - HOUR_MS + 24 * HOUR_MS);
  });

  it("uses the startup grace period when the last run is unknown", () => {
    expect(nextRunAt(null, 24, NOW)).toBe(NOW + STARTUP_GRACE_MS);
  });

  it("uses the startup grace period when the interval already elapsed", () => {
    expect(nextRunAt(NOW - 48 * HOUR_MS, 24, NOW)).toBe(NOW + STARTUP_GRACE_MS);
  });

  it("uses the startup grace period when the run is due exactly now", () => {
    expect(nextRunAt(NOW - HOUR_MS, 1, NOW)).toBe(NOW + STARTUP_GRACE_MS);
  });

  it("honours an hourly interval", () => {
    expect(nextRunAt(NOW, 1, NOW)).toBe(NOW + HOUR_MS);
  });
});

describe("run state", () => {
  it("round-trips the last run timestamp", async () => {
    await withTempDir(async (dir) => {
      await writeLastRunAt(dir, NOW);
      expect(await readLastRunAt(dir)).toBe(NOW);
    });
  });

  it("returns null when the state file is missing or corrupt", async () => {
    await withTempDir(async (dir) => {
      expect(await readLastRunAt(dir)).toBeNull();
      await writeFile(path.join(dir, STATE_FILE_NAME), "{not json");
      expect(await readLastRunAt(dir)).toBeNull();
      await writeFile(path.join(dir, STATE_FILE_NAME), '{"lastRunAt":"soon"}');
      expect(await readLastRunAt(dir)).toBeNull();
    });
  });
});
