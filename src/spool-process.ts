import { type ChildProcess, fork } from "node:child_process";
import { constants, setPriority } from "node:os";
import { isSandboxChildProcessError, SANDBOX_CHILD_PROCESS_ERROR } from "./dump";
import type { SpoolJob, SpoolProgress, SpoolResult } from "./spool";

export type SpoolMessage =
  | ({ type: "progress" } & SpoolProgress)
  | { type: "done"; result: SpoolResult }
  | { type: "failed"; message: string };

export type SpoolProcess = {
  done: Promise<SpoolResult>;
  stop: () => Promise<void>;
};

const STDERR_LIMIT = 4 * 1024;
const STOP_GRACE_MS = 5_000;

export function forkSpool(
  script: string,
  job: SpoolJob,
  onProgress?: (progress: SpoolProgress) => void,
): SpoolProcess {
  let child: ChildProcess;
  try {
    child = fork(script, [], { stdio: ["ignore", "ignore", "pipe", "ipc"] });
  } catch (failure) {
    const message = failure instanceof Error ? failure.message : String(failure);
    throw new Error(isSandboxChildProcessError(message) ? SANDBOX_CHILD_PROCESS_ERROR : message);
  }

  let stderr = "";
  child.stderr?.on("data", (chunk) => {
    stderr = (stderr + String(chunk)).slice(-STDERR_LIMIT);
  });

  const exited = new Promise<void>((resolve) => {
    child.once("exit", () => resolve());
    child.once("error", () => resolve());
  });

  const done = new Promise<SpoolResult>((resolve, reject) => {
    child.on("message", (message: SpoolMessage) => {
      if (message.type === "progress") {
        onProgress?.({ bytesSql: message.bytesSql, bytesZip: message.bytesZip });
      } else if (message.type === "done") {
        resolve(message.result);
      } else {
        reject(new Error(message.message));
      }
    });
    child.once("error", (failure) =>
      reject(new Error(`Failed to start the backup process: ${failure.message}`)),
    );
    child.once("exit", (code, signal) =>
      reject(new Error(stderr.trim() || `Backup process exited with ${signal ?? `code ${code}`}`)),
    );
  });
  void done.catch(() => {});

  child.once("spawn", () => {
    if (child.pid === undefined) return;
    try {
      setPriority(child.pid, constants.priority.PRIORITY_BELOW_NORMAL);
    } catch {}
  });
  child.send(job);

  return {
    done,
    stop: async () => {
      if (child.exitCode !== null || child.signalCode !== null) return;
      // Disconnecting lets the child kill its dump and delete the partial zip before exiting.
      if (child.connected) child.disconnect();
      const forced = setTimeout(() => child.kill("SIGKILL"), STOP_GRACE_MS);
      await exited;
      clearTimeout(forced);
    },
  };
}
