import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import type { SpoolJob, SpoolProgress } from "./spool";
import { forkSpool } from "./spool-process";

const job: SpoolJob = {
  target: { host: "127.0.0.1", port: 3306, user: "root", password: "", database: "db", ssl: false },
  binary: { command: "mariadb-dump", version: "test", isMariaDb: true, source: "path" },
  attempt: { includeMysqlOnly: false, includeRoutines: true },
  path: "unused.zip",
  entryName: "db.sql",
  zipLevel: 1,
  maxMbPerSecond: 0,
};

const children: Record<string, string> = {
  succeeds: `process.once("message", (job) => {
    process.send({ type: "progress", bytesSql: 10, bytesZip: 4 });
    process.send({ type: "done", result: { bytesSql: 20, bytesZip: 8, sha256: job.entryName, warnings: [] } }, () => process.exit(0));
  });`,
  fails: `process.once("message", () => {
    process.send({ type: "failed", message: "Got error: 1045: Access denied" }, () => process.exit(1));
  });`,
  crashes: `process.stderr.write("out of memory"); process.exit(3);`,
  waits: `process.once("disconnect", () => process.exit(1)); setInterval(() => {}, 1000);`,
};

let dir: string;

beforeAll(async () => {
  dir = await mkdtemp(path.join(tmpdir(), "qbx-spool-process-"));
  for (const [name, body] of Object.entries(children)) {
    await writeFile(path.join(dir, `${name}.cjs`), body);
  }
});

afterAll(async () => {
  await rm(dir, { recursive: true, force: true });
});

describe("forkSpool", () => {
  it("resolves with the child's result and forwards progress", async () => {
    const progress: SpoolProgress[] = [];
    const spool = forkSpool(path.join(dir, "succeeds.cjs"), job, (p) => progress.push(p));
    const result = await spool.done;
    expect(result).toEqual({ bytesSql: 20, bytesZip: 8, sha256: "db.sql", warnings: [] });
    expect(progress).toEqual([{ bytesSql: 10, bytesZip: 4 }]);
  });

  it("rejects with the failure the child reports", async () => {
    const spool = forkSpool(path.join(dir, "fails.cjs"), job);
    await expect(spool.done).rejects.toThrow("Got error: 1045: Access denied");
  });

  it("rejects with stderr when the child exits without reporting", async () => {
    const spool = forkSpool(path.join(dir, "crashes.cjs"), job);
    await expect(spool.done).rejects.toThrow("out of memory");
  });

  it("stops a running child", async () => {
    const spool = forkSpool(path.join(dir, "waits.cjs"), job);
    const settled = spool.done.catch((failure: Error) => failure.message);
    await spool.stop();
    expect(await settled).toMatch(/exited/);
  });
});
