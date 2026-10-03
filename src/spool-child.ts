import { rm } from "node:fs/promises";
import { constants, setPriority } from "node:os";
import { type SpoolJob, type SpoolProgress, spoolDump } from "./spool";
import type { SpoolMessage } from "./spool-process";

const PROGRESS_INTERVAL_MS = 1000;

// On Linux this only renices the calling thread, so it must run before the thread pool starts.
try {
  setPriority(constants.priority.PRIORITY_BELOW_NORMAL);
} catch {}

function send(message: SpoolMessage): Promise<void> {
  return new Promise((resolve) => {
    if (!process.connected) return resolve();
    process.send?.(message, () => resolve());
  });
}

process.once("message", (job: SpoolJob) => {
  let lastProgress = 0;
  const report = (progress: SpoolProgress) => {
    const now = Date.now();
    if (now - lastProgress < PROGRESS_INTERVAL_MS) return;
    lastProgress = now;
    void send({ type: "progress", ...progress });
  };

  const run = spoolDump(job, report);

  process.once("disconnect", () => {
    run.kill();
    void run.done
      .catch(() => {})
      .then(() => rm(job.path, { force: true }))
      .finally(() => process.exit(1));
  });

  run.done.then(
    async (result) => {
      await send({ type: "done", result });
      process.exit(0);
    },
    async (failure: unknown) => {
      await send({
        type: "failed",
        message: failure instanceof Error ? failure.message : String(failure),
      });
      process.exit(1);
    },
  );
});
