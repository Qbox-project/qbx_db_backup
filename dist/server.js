"use strict";
var __create = Object.create;
var __defProp = Object.defineProperty;
var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __getProtoOf = Object.getPrototypeOf;
var __hasOwnProp = Object.prototype.hasOwnProperty;
var __copyProps = (to, from, except, desc) => {
  if (from && typeof from === "object" || typeof from === "function") {
    for (let key of __getOwnPropNames(from))
      if (!__hasOwnProp.call(to, key) && key !== except)
        __defProp(to, key, { get: () => from[key], enumerable: !(desc = __getOwnPropDesc(from, key)) || desc.enumerable });
  }
  return to;
};
var __toESM = (mod, isNodeMode, target) => (target = mod != null ? __create(__getProtoOf(mod)) : {}, __copyProps(
  // If the importer is in node compatibility mode or this is not an ESM
  // file that has been converted to a CommonJS file using a Babel-
  // compatible transform (i.e. "__esModule" has not been set), then set
  // "default" to the CommonJS "module.exports" for node compatibility.
  isNodeMode || !mod || !mod.__esModule ? __defProp(target, "default", { value: mod, enumerable: true }) : target,
  mod
));

// src/server.ts
var import_node_path7 = __toESM(require("node:path"), 1);

// src/api.ts
var REQUEST_TIMEOUT_MS = 3e4;
var POOL_EXCEEDED_FALLBACK = "Backup is larger than the storage pool for this plan";
var AgentApi = class {
  constructor(options) {
    this.options = options;
  }
  options;
  async heartbeat(payload) {
    const response = await this.send("POST", "/api/agent/backups/heartbeat", payload);
    return await response.json();
  }
  async createJob(trigger) {
    const response = await this.send("POST", "/api/agent/backups/jobs", { trigger }, [409, 429]);
    if (response.status === 429) {
      const body = await readBody(response);
      return { status: "throttled", nextAllowedAt: body.nextAllowedAt ?? null };
    }
    if (response.status === 409) {
      const body = await readBody(response);
      return { status: "busy", jobId: body.jobId ?? "unknown" };
    }
    return { status: "created", job: await response.json() };
  }
  async requestUpload(jobId, payload) {
    const response = await this.send(
      "POST",
      `/api/agent/backups/${encodeURIComponent(jobId)}/upload`,
      payload,
      [413]
    );
    if (response.status === 413) {
      const body = await readBody(response);
      throw new Error(body.error ?? POOL_EXCEEDED_FALLBACK);
    }
    return await response.json();
  }
  async progress(jobId, payload) {
    await this.send("POST", `/api/agent/backups/${encodeURIComponent(jobId)}/progress`, payload);
  }
  async complete(jobId, payload) {
    const response = await this.send(
      "POST",
      `/api/agent/backups/${encodeURIComponent(jobId)}/complete`,
      payload
    );
    return await response.json();
  }
  async send(method, route, body, expected = []) {
    const response = await fetch(`${this.options.baseUrl}${route}`, {
      method,
      headers: {
        Authorization: `Bearer ${this.options.token}`,
        "X-Qbox-Agent": `qbx_db_backup/${this.options.version}`,
        "Content-Type": "application/json"
      },
      body: body === void 0 ? void 0 : JSON.stringify(body),
      signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS)
    });
    if (response.status === 401) throw new Error("Agent token rejected by the Qbox API");
    if (!response.ok && !expected.includes(response.status)) {
      const text = (await response.text().catch(() => "")).slice(0, 300);
      throw new Error(
        `Qbox API ${method} ${route} failed with HTTP ${response.status}${text ? `: ${text}` : ""}`
      );
    }
    return response;
  }
};
async function readBody(response) {
  try {
    return await response.json();
  } catch {
    return {};
  }
}

// src/backup.ts
var import_node_path2 = __toESM(require("node:path"), 1);

// src/connection-string.ts
var KEY_ALIASES = {
  user: "user",
  userid: "user",
  uid: "user",
  username: "user",
  password: "password",
  pwd: "password",
  host: "host",
  server: "host",
  "data source": "host",
  hostname: "host",
  port: "port",
  database: "database",
  "initial catalog": "database",
  dbname: "database",
  ssl: "ssl"
};
var TRUTHY = /* @__PURE__ */ new Set(["true", "1", "yes", "y", "on", "require", "required"]);
var URI_PATTERN = /^[a-z][a-z0-9+.-]*:\/\//i;
var DEFAULT_PORT = 3306;
function parseConnectionString(raw) {
  const value = raw.trim();
  if (value.length === 0) {
    throw new Error(
      "Connection string is empty: set mysql_connection_string or qbx_db_backup_connection_string"
    );
  }
  return URI_PATTERN.test(value) ? parseUri(value) : parseKeyValue(value);
}
function describeTarget(target) {
  return `${target.user}@${target.host}:${target.port}/${target.database}`;
}
function parseUri(value) {
  let url;
  try {
    url = new URL(value);
  } catch {
    throw new Error("Connection string is not a valid mysql:// URI");
  }
  const scheme = url.protocol.replace(/:$/, "").toLowerCase();
  if (scheme !== "mysql" && scheme !== "mariadb") {
    throw new Error(
      `Connection string scheme "${scheme}" is not supported: expected mysql:// or mariadb://`
    );
  }
  const ssl = url.searchParams.get("ssl");
  return finalize({
    host: url.hostname,
    port: url.port,
    user: decode(url.username),
    password: decode(url.password),
    database: decode(url.pathname.replace(/^\//, "")),
    ssl: ssl === null ? "" : ssl
  });
}
function parseKeyValue(value) {
  const fields = {};
  for (const segment of value.split(";")) {
    const trimmed = segment.trim();
    if (trimmed.length === 0) continue;
    const separator = trimmed.indexOf("=");
    if (separator < 0) continue;
    const field = KEY_ALIASES[trimmed.slice(0, separator).trim().toLowerCase()];
    if (field === void 0) continue;
    fields[field] = unquote(trimmed.slice(separator + 1).trim());
  }
  return finalize({
    host: fields.host ?? "",
    port: fields.port ?? "",
    user: fields.user ?? "",
    password: fields.password ?? "",
    database: fields.database ?? "",
    ssl: fields.ssl ?? ""
  });
}
function finalize(raw) {
  if (raw.host.length === 0) throw new Error("Connection string is missing the database host");
  if (raw.user.length === 0) throw new Error("Connection string is missing the database user");
  if (raw.database.length === 0) {
    throw new Error("Connection string is missing the database name");
  }
  const port = Number.parseInt(raw.port, 10);
  return {
    host: raw.host,
    port: Number.isFinite(port) && port > 0 ? port : DEFAULT_PORT,
    user: raw.user,
    password: raw.password,
    database: raw.database,
    ssl: TRUTHY.has(raw.ssl.toLowerCase())
  };
}
function decode(value) {
  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}
function unquote(value) {
  const quote = value.charAt(0);
  if (value.length >= 2 && (quote === "'" || quote === '"') && value.endsWith(quote)) {
    return value.slice(1, -1);
  }
  return value;
}

// src/dump.ts
var import_node_child_process = require("node:child_process");
var import_node_fs = require("node:fs");
var import_promises = require("node:fs/promises");
var import_node_path = __toESM(require("node:path"), 1);
var MISSING_BINARY_ERROR = "Failed to start dump command: no mysqldump/mariadb-dump binary available (set qbx_db_backup_dump_bin)";
var SANDBOX_CHILD_PROCESS_ERROR = 'Child processes are blocked by the FiveM sandbox: add add_unsafe_child_process_permission "qbx_db_backup" to server.cfg before ensure qbx_db_backup and restart the server';
var TRANSIENT_RETRY_ATTEMPTS = 3;
var STDERR_LIMIT = 64 * 1024;
var ROUTINE_PRIVILEGE_PATTERN = /access denied for user .* to database|show events|show function status|show procedure status|show create function|show create procedure|mysql\.proc|mysql\.routines/i;
var AUTH_FAILURE_PATTERN = /access denied for user[^\n]*\(using password:/i;
function isTableDefinitionChangedError(stderr) {
  const n = stderr.toLowerCase();
  return n.includes("error 1412") || n.includes("(1412)") || n.includes("table definition has changed");
}
function isTransientMysqldumpError(stderr) {
  const n = stderr.toLowerCase();
  return n.includes("error 2013") || n.includes("error 2006") || n.includes("(2013)") || n.includes("(2006)") || n.includes("lost connection to mysql server") || n.includes("lost connection to server") || n.includes("mysql server has gone away") || n.includes("econnreset") || n.includes("broken pipe") || isTableDefinitionChangedError(stderr);
}
function transientRetryDelayMs(stderr, attempt) {
  return (isTableDefinitionChangedError(stderr) ? 3e4 : 750) * attempt;
}
function hasUnsupportedMysqlSpecificArgError(stderr) {
  const n = stderr.toLowerCase();
  return n.includes("unknown variable") && (n.includes("column-statistics") || n.includes("set-gtid-purged"));
}
function isSandboxChildProcessError(message) {
  return /allow-child-process|child spawn not allowed|ERR_ACCESS_DENIED/i.test(message);
}
function isAuthFailureError(stderr) {
  const n = stderr.toLowerCase();
  return AUTH_FAILURE_PATTERN.test(stderr) || n.includes("error 1045") || n.includes("(1045)");
}
function isRoutinePrivilegeError(stderr) {
  return !isAuthFailureError(stderr) && ROUTINE_PRIVILEGE_PATTERN.test(stderr);
}
function planRetry(stderr, state, attempt) {
  if (state.includeMysqlOnly && hasUnsupportedMysqlSpecificArgError(stderr)) {
    return {
      state: { ...state, includeMysqlOnly: false },
      delayMs: 0,
      consumesAttempt: false
    };
  }
  if (state.includeRoutines && isRoutinePrivilegeError(stderr)) {
    return {
      state: { ...state, includeRoutines: false },
      delayMs: 0,
      consumesAttempt: false
    };
  }
  if (attempt < TRANSIENT_RETRY_ATTEMPTS && isTransientMysqldumpError(stderr)) {
    return { state, delayMs: transientRetryDelayMs(stderr, attempt), consumesAttempt: true };
  }
  return null;
}
function bundledBinaryPath(resourceDir, platform, arch) {
  if (platform === "win32" && arch === "x64") {
    return import_node_path.default.join(resourceDir, "bin", "win64", "mariadb-dump.exe");
  }
  if (platform === "linux" && arch === "x64") {
    return import_node_path.default.join(resourceDir, "bin", "linux-x64", "mariadb-dump");
  }
  return null;
}
function candidateOrder(input) {
  const candidates = [];
  if (input.explicitPath !== void 0 && input.explicitPath.length > 0) {
    candidates.push(input.explicitPath);
  }
  if (input.bundled !== null) candidates.push(input.bundled);
  candidates.push("mariadb-dump", "mysqldump");
  return candidates;
}
var cachedBinary = null;
async function detectDumpBinary(options) {
  const explicitPath = options.explicitPath !== void 0 && options.explicitPath.length > 0 ? options.explicitPath : void 0;
  const key = `${explicitPath ?? ""}\0${options.resourceDir}`;
  if (cachedBinary !== null && cachedBinary.key === key) return cachedBinary.value;
  const bundled = bundledBinaryPath(options.resourceDir, process.platform, process.arch);
  const candidates = candidateOrder({ explicitPath, bundled });
  const explicitCount = explicitPath === void 0 ? 0 : 1;
  for (let index = 0; index < candidates.length; index += 1) {
    const command = candidates[index];
    if (command === void 0) continue;
    const source = index < explicitCount ? "explicit" : index === explicitCount && bundled !== null ? "bundled" : "path";
    if (source === "bundled") await makeExecutable(command);
    const version = await captureVersion(command);
    if (version === null) continue;
    return remember(key, command, version, source);
  }
  for (const command of windowsCandidates()) {
    const version = await captureVersion(command);
    if (version === null) continue;
    return remember(key, command, version, "windows-probe");
  }
  throw new Error(MISSING_BINARY_ERROR);
}
function remember(key, command, version, source) {
  const value = { command, version, isMariaDb: /mariadb/i.test(version), source };
  cachedBinary = { key, value };
  return value;
}
async function makeExecutable(target) {
  if (process.platform !== "linux") return;
  await (0, import_promises.chmod)(target, 493).catch(() => void 0);
}
function spawnChecked(command, args, options) {
  try {
    return (0, import_node_child_process.spawn)(command, args, options);
  } catch (failure) {
    const message = failure instanceof Error ? failure.message : String(failure);
    throw new Error(isSandboxChildProcessError(message) ? SANDBOX_CHILD_PROCESS_ERROR : message);
  }
}
function captureVersion(command) {
  return new Promise((resolve) => {
    const child = spawnChecked(command, ["--version"], { stdio: ["ignore", "pipe", "pipe"] });
    let out = "";
    child.stdout.on("data", (chunk) => {
      out += String(chunk);
    });
    child.stderr.on("data", (chunk) => {
      out += String(chunk);
    });
    child.on("error", () => resolve(null));
    child.on("close", (code) => resolve(code === 0 ? out.trim() : null));
  });
}
function windowsCandidates() {
  if (process.platform !== "win32") return [];
  const programFiles = "C:\\Program Files";
  const found = [];
  for (const entry of listDir(programFiles)) {
    if (/^MariaDB/i.test(entry)) {
      found.push(import_node_path.default.join(programFiles, entry, "bin", "mariadb-dump.exe"));
    }
  }
  const mysqlRoot = import_node_path.default.join(programFiles, "MySQL");
  for (const entry of listDir(mysqlRoot)) {
    if (/^MySQL Server/i.test(entry)) {
      found.push(import_node_path.default.join(mysqlRoot, entry, "bin", "mysqldump.exe"));
    }
  }
  found.push("C:\\xampp\\mysql\\bin\\mysqldump.exe");
  return found;
}
function listDir(dir) {
  try {
    return (0, import_node_fs.readdirSync)(dir);
  } catch {
    return [];
  }
}

// src/log.ts
function info(...parts) {
  console.log(...parts);
}
function warn(...parts) {
  console.warn("WARNING:", ...parts);
}
function error(...parts) {
  console.error("ERROR:", ...parts);
}
function errorMessage(value) {
  return value instanceof Error ? value.message : String(value);
}

// src/spool-process.ts
var import_node_child_process2 = require("node:child_process");
var import_node_os = require("node:os");
var STDERR_LIMIT2 = 4 * 1024;
var STOP_GRACE_MS = 5e3;
function forkSpool(script, job, onProgress) {
  let child;
  try {
    child = (0, import_node_child_process2.fork)(script, [], { stdio: ["ignore", "ignore", "pipe", "ipc"] });
  } catch (failure) {
    const message = failure instanceof Error ? failure.message : String(failure);
    throw new Error(isSandboxChildProcessError(message) ? SANDBOX_CHILD_PROCESS_ERROR : message);
  }
  let stderr = "";
  child.stderr?.on("data", (chunk) => {
    stderr = (stderr + String(chunk)).slice(-STDERR_LIMIT2);
  });
  const exited = new Promise((resolve) => {
    child.once("exit", () => resolve());
    child.once("error", () => resolve());
  });
  const done = new Promise((resolve, reject) => {
    child.on("message", (message) => {
      if (message.type === "progress") {
        onProgress?.({ bytesSql: message.bytesSql, bytesZip: message.bytesZip });
      } else if (message.type === "done") {
        resolve(message.result);
      } else {
        reject(new Error(message.message));
      }
    });
    child.once(
      "error",
      (failure) => reject(new Error(`Failed to start the backup process: ${failure.message}`))
    );
    child.once(
      "exit",
      (code, signal) => reject(new Error(stderr.trim() || `Backup process exited with ${signal ?? `code ${code}`}`))
    );
  });
  void done.catch(() => {
  });
  child.once("spawn", () => {
    if (child.pid === void 0) return;
    try {
      (0, import_node_os.setPriority)(child.pid, import_node_os.constants.priority.PRIORITY_BELOW_NORMAL);
    } catch {
    }
  });
  child.send(job);
  return {
    done,
    stop: async () => {
      if (child.exitCode !== null || child.signalCode !== null) return;
      if (child.connected) child.disconnect();
      const forced = setTimeout(() => child.kill("SIGKILL"), STOP_GRACE_MS);
      await exited;
      clearTimeout(forced);
    }
  };
}

// src/backup.ts
var running = false;
var cancelActive = null;
function isBackupRunning() {
  return running;
}
function cancelRunningBackup(reason = "Backup cancelled") {
  cancelActive?.(new Error(reason));
}
function sanitizeSegment(value) {
  const sanitized = value.replace(/[^a-zA-Z0-9._-]/g, "_");
  return sanitized.length > 0 ? sanitized : "backup";
}
function formatBackupStamp(date) {
  const pad = (value) => String(value).padStart(2, "0");
  const day = `${date.getUTCFullYear()}-${pad(date.getUTCMonth() + 1)}-${pad(date.getUTCDate())}`;
  const time = `${pad(date.getUTCHours())}-${pad(date.getUTCMinutes())}-${pad(date.getUTCSeconds())}`;
  return `${day}_${time}`;
}
function buildBackupNames(database, date) {
  const stem = `${sanitizeSegment(database)}-${formatBackupStamp(date)}Z`;
  return { zipName: `${stem}.zip`, entryName: `${stem}.sql` };
}
var DestinationError = class extends Error {
  constructor(cause) {
    super(errorMessage(cause), { cause });
    this.name = "DestinationError";
  }
};
async function runBackup(input) {
  if (running) return { busy: true };
  running = true;
  const startedAt = Date.now();
  let cancelled = null;
  let active = null;
  const cancel = (reason) => {
    cancelled ??= reason;
    const current = active;
    void current?.stop().then(current.abort).catch(() => {
    });
  };
  cancelActive = cancel;
  const timeoutMs = input.timeoutMs ?? input.config.timeoutMinutes * 6e4;
  const deadline = setTimeout(() => {
    cancel(new Error(`Backup exceeded the time limit (${Math.round(timeoutMs / 6e4)} min)`));
  }, timeoutMs);
  const onAbort = () => cancel(new Error("Backup cancelled"));
  input.signal?.addEventListener("abort", onAbort, { once: true });
  try {
    const target = parseConnectionString(input.config.connectionString);
    const binary = await detectDumpBinary({
      explicitPath: input.config.dumpBin.length > 0 ? input.config.dumpBin : void 0,
      resourceDir: input.config.resourceDir
    });
    const entryName = input.entryName ?? buildBackupNames(target.database, new Date(startedAt)).entryName;
    let state = { includeMysqlOnly: !binary.isMariaDb, includeRoutines: true };
    let attempt = 0;
    for (; ; ) {
      attempt += 1;
      if (cancelled !== null) throw cancelled;
      let bytesSql = 0;
      let bytesZip = 0;
      let phase = "dump";
      const report = () => input.onProgress?.({ phase, bytesSql, bytesZip });
      const sink = await input.sink.open();
      const spool = forkSpool(
        import_node_path2.default.join(input.config.resourceDir, "dist", "spool.js"),
        {
          target,
          binary,
          attempt: state,
          path: sink.spoolPath,
          entryName,
          zipLevel: input.config.zipLevel,
          maxMbPerSecond: input.config.maxMbPerSecond
        },
        (progress) => {
          bytesSql = progress.bytesSql;
          bytesZip = progress.bytesZip;
          report();
        }
      );
      active = { stop: spool.stop, abort: sink.abort };
      try {
        if (cancelled !== null) throw cancelled;
        const spooled = await spool.done;
        bytesSql = spooled.bytesSql;
        bytesZip = spooled.bytesZip;
        phase = "upload";
        report();
        const written = await sink.finish(spooled).catch((failure) => {
          throw new DestinationError(failure);
        });
        return {
          busy: false,
          sizeBytes: written.bytesZip,
          rawBytes: spooled.bytesSql,
          sha256: written.sha256,
          durationMs: Date.now() - startedAt,
          warnings: spooled.warnings,
          database: target.database,
          dumpBinary: binary,
          ...written.location === void 0 ? {} : { location: written.location }
        };
      } catch (failure) {
        await spool.stop();
        await sink.abort().catch(() => {
        });
        if (cancelled !== null) throw cancelled;
        const plan = planRetry(errorMessage(failure), state, attempt);
        if (plan === null) throw failure;
        state = plan.state;
        if (!plan.consumesAttempt) attempt -= 1;
        if (plan.delayMs > 0) await delay(plan.delayMs);
      } finally {
        active = null;
      }
    }
  } finally {
    clearTimeout(deadline);
    input.signal?.removeEventListener("abort", onAbort);
    cancelActive = null;
    running = false;
  }
}
function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// src/config.ts
var RESOURCE_VERSION = "1.1.0";
var DEFAULT_API_BASE = "https://dashboard.qbox.re";
var MIN_POLL_SECONDS = 60;
var DEFAULT_POLL_SECONDS = 300;
var DEFAULT_INTERVAL_HOURS = 1;
var DEFAULT_LOCAL_KEEP = 7;
var DEFAULT_MAX_MB_PER_SECOND = 10;
function loadConfig(source, defaults) {
  const shared = source("mysql_connection_string", "").trim();
  const override = source("qbx_db_backup_connection_string", "").trim();
  const token = source("qbx_db_backup_token", "").trim();
  const apiBase = source("qbx_db_backup_api", DEFAULT_API_BASE).trim();
  const localDir = source("qbx_db_backup_local_dir", defaults.localDir).trim();
  const interval = toInt(
    source("qbx_db_backup_interval_hours", String(DEFAULT_INTERVAL_HOURS)),
    DEFAULT_INTERVAL_HOURS
  );
  const s3Endpoint = source("qbx_db_backup_s3_endpoint", "").trim();
  const s3Bucket = source("qbx_db_backup_s3_bucket", "").trim();
  const s3Region = source(
    "qbx_db_backup_s3_region",
    s3Endpoint.includes("r2.cloudflarestorage.com") ? "auto" : "us-east-1"
  ).trim();
  const s3AccessKeyId = (source("qbx_db_backup_s3_access_key_id", "") || source("qbx_db_backup_s3_key", "")).trim();
  const s3SecretAccessKey = (source("qbx_db_backup_s3_secret_access_key", "") || source("qbx_db_backup_s3_secret", "")).trim();
  const s3PathStyleRaw = source("qbx_db_backup_s3_force_path_style", "").trim();
  const s3Prefix = source("qbx_db_backup_s3_prefix", "").trim();
  const s3Keep = Math.max(0, toInt(source("qbx_db_backup_s3_keep", "0"), 0));
  const s3MaxAgeDays = Math.max(0, toInt(source("qbx_db_backup_s3_max_age_days", "0"), 0));
  const forcePathStyle = s3PathStyleRaw === "1" ? true : s3PathStyleRaw === "0" ? false : s3Endpoint.length > 0;
  const s3 = {
    endpoint: s3Endpoint.length > 0 ? s3Endpoint : void 0,
    bucket: s3Bucket,
    region: s3Region.length > 0 ? s3Region : "us-east-1",
    accessKeyId: s3AccessKeyId,
    secretAccessKey: s3SecretAccessKey,
    forcePathStyle,
    prefix: s3Prefix,
    keepCount: s3Keep,
    maxAgeDays: s3MaxAgeDays
  };
  const localMaxAgeDays = Math.max(
    0,
    toInt(source("qbx_db_backup_local_max_age_days", source("qbx_db_backup_max_age_days", "0")), 0)
  );
  const minFreeDiskMb = Math.max(0, toInt(source("qbx_db_backup_min_free_disk_mb", "0"), 0));
  let mode = "local";
  if (token.length > 0) {
    mode = "qbx";
  } else if (isS3Configured(s3)) {
    mode = "s3";
  }
  return {
    connectionString: override.length > 0 ? override : shared,
    token,
    apiBase: (apiBase.length > 0 ? apiBase : DEFAULT_API_BASE).replace(/\/+$/, ""),
    localDir: localDir.length > 0 ? localDir : defaults.localDir,
    resourceDir: defaults.resourceDir,
    keepLocal: source("qbx_db_backup_keep_local", "0").trim() === "1",
    localKeep: Math.max(
      1,
      toInt(source("qbx_db_backup_local_keep", String(DEFAULT_LOCAL_KEEP)), DEFAULT_LOCAL_KEEP)
    ),
    localMaxAgeDays,
    minFreeDiskMb,
    pollSeconds: Math.max(
      MIN_POLL_SECONDS,
      toInt(
        source("qbx_db_backup_poll_seconds", String(DEFAULT_POLL_SECONDS)),
        DEFAULT_POLL_SECONDS
      )
    ),
    intervalHours: interval === 0 ? 0 : Math.max(1, interval),
    intervalClamped: interval !== 0 && interval < 1,
    dumpBin: source("qbx_db_backup_dump_bin", "").trim(),
    zipLevel: clamp(toInt(source("qbx_db_backup_zip_level", "6"), 6), 1, 9),
    maxMbPerSecond: Math.max(
      0,
      toInt(
        source("qbx_db_backup_max_mb_per_second", String(DEFAULT_MAX_MB_PER_SECOND)),
        DEFAULT_MAX_MB_PER_SECOND
      )
    ),
    timeoutMinutes: Math.max(1, toInt(source("qbx_db_backup_timeout_minutes", "120"), 120)),
    mode,
    s3
  };
}
function isS3Configured(s3) {
  return s3.bucket.length > 0 && s3.accessKeyId.length > 0 && s3.secretAccessKey.length > 0;
}
var URI_PASSWORD = /^([a-z][a-z0-9+.-]*:\/\/[^/@]*?:)[^/]*(@[^/@]*)/i;
var KEY_VALUE_PASSWORD = /(\b(?:password|pwd)\s*=\s*)([^;]*)/gi;
function redactConnectionString(value) {
  if (value.length === 0) return "";
  const redactedUri = value.replace(URI_PASSWORD, "$1***$2");
  if (redactedUri !== value) return redactedUri;
  return value.replace(KEY_VALUE_PASSWORD, "$1***");
}
function redactSecret(secret) {
  if (secret.length === 0) return "";
  if (secret.length <= 6) return "***";
  return `${secret.slice(0, 3)}***${secret.slice(-3)}`;
}
function toInt(value, fallback) {
  const parsed = Number.parseInt(value.trim(), 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}
function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

// src/retention.ts
var import_promises2 = require("node:fs/promises");
var import_node_path3 = __toESM(require("node:path"), 1);
var BACKUP_NAME_PATTERN = /^[A-Za-z0-9._-]+-(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})Z\.zip$/;
var S3_DELETE_BATCH_SIZE = 1e3;
function isOwnBackupFile(name) {
  const base = import_node_path3.default.basename(name);
  return BACKUP_NAME_PATTERN.test(base);
}
function parseBackupTimestamp(name) {
  const base = import_node_path3.default.basename(name);
  const match = BACKUP_NAME_PATTERN.exec(base);
  if (!match?.[1]) return null;
  const parts = match[1].split("_");
  const datePart = parts[0];
  const timePart = parts[1];
  if (!datePart || !timePart) return null;
  const [y, m, d] = datePart.split("-").map((v) => Number.parseInt(v, 10));
  const [hh, mm, ss] = timePart.split("-").map((v) => Number.parseInt(v, 10));
  if (y === void 0 || m === void 0 || d === void 0 || hh === void 0 || mm === void 0 || ss === void 0 || Number.isNaN(y) || Number.isNaN(m) || Number.isNaN(d) || Number.isNaN(hh) || Number.isNaN(mm) || Number.isNaN(ss)) {
    return null;
  }
  return Date.UTC(y, m - 1, d, hh, mm, ss);
}
function selectPrunableEntries(entries, options) {
  const now = options.now ?? Date.now();
  const minKeep = Math.max(1, options.minKeep ?? 1);
  const valid = entries.filter((e) => isOwnBackupFile(e.name) && Number.isFinite(e.timestamp)).sort((a, b) => a.timestamp - b.timestamp);
  if (valid.length <= minKeep) {
    return [];
  }
  const prunableSet = /* @__PURE__ */ new Set();
  if (options.maxCount !== void 0 && options.maxCount > 0) {
    const keep = Math.max(minKeep, options.maxCount);
    if (valid.length > keep) {
      const countPrunable = valid.slice(0, valid.length - keep);
      for (const item of countPrunable) {
        prunableSet.add(item);
      }
    }
  }
  if (options.maxAgeMs !== void 0 && options.maxAgeMs > 0) {
    const cutoff = now - options.maxAgeMs;
    const maxAgeEligible = valid.slice(0, valid.length - minKeep);
    for (const item of maxAgeEligible) {
      if (item.timestamp < cutoff) {
        prunableSet.add(item);
      }
    }
  }
  if (options.minFreeDiskBytes !== void 0 && options.minFreeDiskBytes > 0 && options.currentFreeDiskBytes !== void 0) {
    let currentFree = options.currentFreeDiskBytes;
    for (const item of prunableSet) {
      currentFree += item.sizeBytes ?? 0;
    }
    if (currentFree < options.minFreeDiskBytes) {
      const spaceEligible = valid.slice(0, valid.length - minKeep);
      for (const item of spaceEligible) {
        if (currentFree >= options.minFreeDiskBytes) break;
        if (!prunableSet.has(item)) {
          prunableSet.add(item);
          currentFree += item.sizeBytes ?? 0;
        }
      }
    }
  }
  const neverPrune = new Set(valid.slice(valid.length - minKeep));
  return valid.filter((item) => prunableSet.has(item) && !neverPrune.has(item));
}
async function getFreeDiskBytes(dir) {
  try {
    const stats = await (0, import_promises2.statfs)(dir);
    return Number(BigInt(stats.bavail) * BigInt(stats.bsize));
  } catch {
    return null;
  }
}
async function pruneLocalDirectory(dir, policy) {
  const filenames = await (0, import_promises2.readdir)(dir).catch(() => []);
  const entries = [];
  for (const name of filenames) {
    if (!isOwnBackupFile(name)) continue;
    const ts = parseBackupTimestamp(name);
    if (ts === null) continue;
    const fullPath = import_node_path3.default.join(dir, name);
    let sizeBytes = 0;
    try {
      const fileStat = await (0, import_promises2.stat)(fullPath);
      sizeBytes = fileStat.size;
    } catch {
    }
    entries.push({ name, timestamp: ts, sizeBytes });
  }
  let freeBytes;
  if (policy.minFreeDiskMb !== void 0 && policy.minFreeDiskMb > 0) {
    const free = await getFreeDiskBytes(dir);
    if (free !== null) freeBytes = free;
  }
  const options = {
    maxCount: policy.keepCount,
    maxAgeMs: policy.maxAgeDays !== void 0 && policy.maxAgeDays > 0 ? policy.maxAgeDays * 864e5 : void 0,
    minFreeDiskBytes: policy.minFreeDiskMb !== void 0 && policy.minFreeDiskMb > 0 ? policy.minFreeDiskMb * 1024 * 1024 : void 0,
    currentFreeDiskBytes: freeBytes
  };
  const prunable = selectPrunableEntries(entries, options);
  const deletedFiles = [];
  let freedBytes = 0;
  for (const item of prunable) {
    try {
      await (0, import_promises2.rm)(import_node_path3.default.join(dir, item.name), { force: true });
      deletedFiles.push(item.name);
      freedBytes += item.sizeBytes ?? 0;
    } catch {
    }
  }
  return {
    deletedFiles,
    freedBytes,
    remainingCount: entries.length - deletedFiles.length
  };
}
async function pruneS3Bucket(client, policy) {
  const prefix = policy.prefix ?? "";
  const objects = [];
  let continuationToken;
  do {
    const page = await client.listObjectsV2({ prefix, continuationToken });
    objects.push(...page.objects);
    continuationToken = page.isTruncated ? page.nextContinuationToken : void 0;
  } while (continuationToken !== void 0);
  const entries = [];
  for (const obj of objects) {
    const filename = import_node_path3.default.basename(obj.key);
    if (!isOwnBackupFile(filename)) continue;
    const ts = parseBackupTimestamp(filename) ?? obj.lastModified.getTime();
    entries.push({
      name: filename,
      key: obj.key,
      timestamp: ts,
      sizeBytes: obj.sizeBytes
    });
  }
  const options = {
    maxCount: policy.keepCount,
    maxAgeMs: policy.maxAgeDays !== void 0 && policy.maxAgeDays > 0 ? policy.maxAgeDays * 864e5 : void 0
  };
  const prunable = selectPrunableEntries(entries, options);
  if (prunable.length === 0) {
    return { deletedKeys: [], errors: [] };
  }
  const keysToDelete = prunable.map((p) => p.key);
  const result = { deletedKeys: [], errors: [] };
  for (let i = 0; i < keysToDelete.length; i += S3_DELETE_BATCH_SIZE) {
    const batch = await client.deleteObjects(keysToDelete.slice(i, i + S3_DELETE_BATCH_SIZE));
    result.deletedKeys.push(...batch.deletedKeys);
    result.errors.push(...batch.errors);
  }
  return result;
}

// src/s3/client.ts
var import_node_crypto2 = require("node:crypto");
var import_node_fs2 = require("node:fs");
var import_promises3 = require("node:fs/promises");
var import_node_http = require("node:http");
var import_node_https = require("node:https");
var import_node_stream = require("node:stream");

// src/s3/signer.ts
var import_node_crypto = require("node:crypto");
var UNRESERVED = /[A-Za-z0-9_\-~.]/;
function uriEncode(input, encodeSlash = false) {
  let result = "";
  for (const byte of Buffer.from(input, "utf8")) {
    const char = String.fromCharCode(byte);
    if (UNRESERVED.test(char) || char === "/" && !encodeSlash) {
      result += char;
    } else {
      result += `%${byte.toString(16).toUpperCase().padStart(2, "0")}`;
    }
  }
  return result;
}
function formatBasicDate(date) {
  const pad = (n) => String(n).padStart(2, "0");
  const year = date.getUTCFullYear();
  const month = pad(date.getUTCMonth() + 1);
  const day = pad(date.getUTCDate());
  const hours = pad(date.getUTCHours());
  const minutes = pad(date.getUTCMinutes());
  const seconds = pad(date.getUTCSeconds());
  const dateScope = `${year}${month}${day}`;
  const isoDate = `${dateScope}T${hours}${minutes}${seconds}Z`;
  return { isoDate, dateScope };
}
function sha256Hex(data) {
  return (0, import_node_crypto.createHash)("sha256").update(data).digest("hex");
}
function getSigningKey(secretKey, dateScope, region, service) {
  const kDate = (0, import_node_crypto.createHmac)("sha256", `AWS4${secretKey}`).update(dateScope).digest();
  const kRegion = (0, import_node_crypto.createHmac)("sha256", kDate).update(region).digest();
  const kService = (0, import_node_crypto.createHmac)("sha256", kRegion).update(service).digest();
  return (0, import_node_crypto.createHmac)("sha256", kService).update("aws4_request").digest();
}
function signS3Request(options) {
  const targetUrl = typeof options.url === "string" ? new URL(options.url) : options.url;
  const now = options.now ?? /* @__PURE__ */ new Date();
  const { isoDate, dateScope } = formatBasicDate(now);
  const service = options.service ?? "s3";
  const payloadHash = options.payloadHash ?? sha256Hex("");
  const method = options.method.toUpperCase();
  const headers = {
    ...options.headers,
    host: targetUrl.host,
    "x-amz-date": isoDate,
    "x-amz-content-sha256": payloadHash
  };
  const headerKeys = Object.keys(headers).map((k) => k.toLowerCase()).sort();
  const canonicalHeadersList = [];
  for (const key of headerKeys) {
    const rawVal = headers[key] ?? "";
    const cleanVal = rawVal.trim().replace(/\s+/g, " ");
    canonicalHeadersList.push(`${key}:${cleanVal}
`);
  }
  const canonicalHeaders = canonicalHeadersList.join("");
  const signedHeaders = headerKeys.join(";");
  const canonicalUri = uriEncode(decodeURIComponent(targetUrl.pathname || "/"), false);
  const queryEntries = [];
  targetUrl.searchParams.forEach((value, key) => {
    queryEntries.push([uriEncode(key, true), uriEncode(value, true)]);
  });
  queryEntries.sort(([aKey, aVal], [bKey, bVal]) => {
    if (aKey !== bKey) return aKey < bKey ? -1 : 1;
    return aVal < bVal ? -1 : 1;
  });
  const canonicalQueryString = queryEntries.map(([k, v]) => `${k}=${v}`).join("&");
  const canonicalRequest = [
    method,
    canonicalUri,
    canonicalQueryString,
    canonicalHeaders,
    signedHeaders,
    payloadHash
  ].join("\n");
  const canonicalRequestHash = sha256Hex(canonicalRequest);
  const credentialScope = `${dateScope}/${options.region}/${service}/aws4_request`;
  const stringToSign = ["AWS4-HMAC-SHA256", isoDate, credentialScope, canonicalRequestHash].join(
    "\n"
  );
  const signingKey = getSigningKey(options.secretAccessKey, dateScope, options.region, service);
  const signature = (0, import_node_crypto.createHmac)("sha256", signingKey).update(stringToSign).digest("hex");
  const authorization = `AWS4-HMAC-SHA256 Credential=${options.accessKeyId}/${credentialScope}, SignedHeaders=${signedHeaders}, Signature=${signature}`;
  return {
    method,
    url: targetUrl.toString(),
    headers: {
      ...headers,
      authorization
    }
  };
}

// src/s3/client.ts
var S3Error = class extends Error {
  constructor(code, message, statusCode, rawBody) {
    super(`S3 Error [${code}]: ${message}`);
    this.code = code;
    this.statusCode = statusCode;
    this.rawBody = rawBody;
    this.name = "S3Error";
  }
  code;
  statusCode;
  rawBody;
};
var DEFAULT_TIMEOUT_MS = 6e4;
var MIB = 1024 * 1024;
var DEFAULT_MULTIPART_THRESHOLD_BYTES = 100 * MIB;
var DEFAULT_PART_SIZE_BYTES = 32 * MIB;
var MAX_PARTS = 1e4;
var PART_ATTEMPTS = 3;
var MAX_RESPONSE_BYTES = 16 * MIB;
var BODY_SLICE_BYTES = 64 * 1024;
var HASH_SLICE_BYTES = MIB;
var S3Client = class {
  bucket;
  region;
  endpoint;
  forcePathStyle;
  accessKeyId;
  secretAccessKey;
  timeoutMs;
  multipartThresholdBytes;
  partSizeBytes;
  constructor(options) {
    if (!options.bucket || options.bucket.trim().length === 0) {
      throw new Error("S3 bucket name is required");
    }
    if (!options.accessKeyId || options.accessKeyId.trim().length === 0) {
      throw new Error("S3 access key ID is required");
    }
    if (!options.secretAccessKey || options.secretAccessKey.trim().length === 0) {
      throw new Error("S3 secret access key is required");
    }
    this.bucket = options.bucket.trim();
    this.accessKeyId = options.accessKeyId.trim();
    this.secretAccessKey = options.secretAccessKey.trim();
    this.region = options.region?.trim() || "us-east-1";
    this.endpoint = options.endpoint?.trim() || void 0;
    this.timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS;
    this.multipartThresholdBytes = options.multipartThresholdBytes ?? DEFAULT_MULTIPART_THRESHOLD_BYTES;
    this.partSizeBytes = options.partSizeBytes ?? DEFAULT_PART_SIZE_BYTES;
    this.forcePathStyle = options.forcePathStyle ?? (this.endpoint !== void 0 && this.endpoint.length > 0);
  }
  buildUrl(key = "", query) {
    let baseUrl;
    const cleanKey = uriEncode(key.replace(/^\/+/, ""), false);
    if (this.endpoint) {
      const ep = this.endpoint.replace(/\/+$/, "");
      if (this.forcePathStyle) {
        baseUrl = cleanKey.length > 0 ? `${ep}/${this.bucket}/${cleanKey}` : `${ep}/${this.bucket}`;
      } else {
        const parsed = new URL(ep);
        baseUrl = `${parsed.protocol}//${this.bucket}.${parsed.host}${parsed.pathname.replace(/\/+$/, "")}/${cleanKey}`;
      }
    } else {
      if (this.forcePathStyle) {
        baseUrl = cleanKey.length > 0 ? `https://s3.${this.region}.amazonaws.com/${this.bucket}/${cleanKey}` : `https://s3.${this.region}.amazonaws.com/${this.bucket}`;
      } else {
        baseUrl = cleanKey.length > 0 ? `https://${this.bucket}.s3.${this.region}.amazonaws.com/${cleanKey}` : `https://${this.bucket}.s3.${this.region}.amazonaws.com/`;
      }
    }
    const url = new URL(baseUrl);
    if (query) {
      for (const [k, v] of Object.entries(query)) {
        if (v !== void 0) {
          url.searchParams.set(k, v);
        }
      }
    }
    return url;
  }
  async putObject(key, body, sizeBytes, payloadSha256, options) {
    const url = this.buildUrl(key);
    const contentType = options?.contentType ?? "application/zip";
    const customHeaders = {
      "content-type": contentType,
      "content-length": String(sizeBytes)
    };
    if (options?.metadata) {
      for (const [mKey, mVal] of Object.entries(options.metadata)) {
        customHeaders[`x-amz-meta-${mKey.toLowerCase()}`] = mVal;
      }
    }
    const signed = signS3Request({
      method: "PUT",
      url,
      headers: customHeaders,
      payloadHash: payloadSha256,
      accessKeyId: this.accessKeyId,
      secretAccessKey: this.secretAccessKey,
      region: this.region
    });
    const response = await this.executeRequest(signed, body);
    const etag = response.headers.etag?.replace(/"/g, "") ?? "";
    return { etag };
  }
  /** A single PUT is capped at 5 GB by S3 and restarts from zero on any network blip. */
  async putFile(key, filePath, sizeBytes, payloadSha256) {
    if (sizeBytes <= this.multipartThresholdBytes) {
      return this.putObject(key, (0, import_node_fs2.createReadStream)(filePath), sizeBytes, payloadSha256);
    }
    const partSize = Math.max(this.partSizeBytes, Math.ceil(sizeBytes / MAX_PARTS));
    const uploadId = await this.createMultipartUpload(key);
    const file = await (0, import_promises3.open)(filePath, "r");
    try {
      const parts = [];
      for (let offset = 0; offset < sizeBytes; offset += partSize) {
        const length = Math.min(partSize, sizeBytes - offset);
        const chunk = Buffer.alloc(length);
        const { bytesRead } = await file.read(chunk, 0, length, offset);
        if (bytesRead !== length) {
          throw new Error(`Backup zip changed size during upload (read ${bytesRead} of ${length})`);
        }
        const partNumber = parts.length + 1;
        parts.push({ partNumber, etag: await this.uploadPart(key, uploadId, partNumber, chunk) });
      }
      return await this.completeMultipartUpload(key, uploadId, parts);
    } catch (failure) {
      await this.abortMultipartUpload(key, uploadId).catch(() => {
      });
      throw failure;
    } finally {
      await file.close();
    }
  }
  async createMultipartUpload(key) {
    const response = await this.send("POST", key, { uploads: "" }, void 0, {
      "content-type": "application/zip"
    });
    const uploadId = /<UploadId>(.*?)<\/UploadId>/s.exec(response.body)?.[1]?.trim();
    if (!uploadId) throw new Error("S3 did not return an UploadId for the multipart upload");
    return unescapeXml(uploadId);
  }
  async uploadPart(key, uploadId, partNumber, chunk) {
    const query = { partNumber: String(partNumber), uploadId };
    for (let attempt = 1; ; attempt += 1) {
      try {
        const response = await this.send("PUT", key, query, chunk);
        const etag = response.headers.etag;
        if (typeof etag !== "string" || etag.length === 0) {
          throw new Error(`S3 did not return an ETag for part ${partNumber}`);
        }
        return etag;
      } catch (failure) {
        const rejected = failure instanceof S3Error && failure.statusCode >= 400 && failure.statusCode < 500;
        if (rejected || attempt >= PART_ATTEMPTS) throw failure;
      }
    }
  }
  async completeMultipartUpload(key, uploadId, parts) {
    const partsXml = parts.map(
      (part) => `<Part><PartNumber>${part.partNumber}</PartNumber><ETag>${escapeXml(part.etag)}</ETag></Part>`
    ).join("");
    const body = Buffer.from(
      `<?xml version="1.0" encoding="UTF-8"?><CompleteMultipartUpload>${partsXml}</CompleteMultipartUpload>`,
      "utf8"
    );
    const response = await this.send("POST", key, { uploadId }, body, {
      "content-type": "application/xml"
    });
    if (response.body.includes("<Error>")) {
      throw parseS3ErrorXml(response.body, response.statusCode);
    }
    const etag = /<ETag>(.*?)<\/ETag>/s.exec(response.body)?.[1]?.trim() ?? "";
    return { etag: unescapeXml(etag).replace(/"/g, "") };
  }
  async abortMultipartUpload(key, uploadId) {
    await this.send("DELETE", key, { uploadId });
  }
  async send(method, key, query, body, headers = {}) {
    const signed = signS3Request({
      method,
      url: this.buildUrl(key, query),
      headers: body ? { ...headers, "content-length": String(body.length) } : headers,
      payloadHash: body ? await hashInSlices(body) : sha256Hex(""),
      accessKeyId: this.accessKeyId,
      secretAccessKey: this.secretAccessKey,
      region: this.region
    });
    return this.executeRequest(signed, body);
  }
  async listObjectsV2(options) {
    const query = { "list-type": "2" };
    if (options?.prefix) query.prefix = options.prefix;
    if (options?.continuationToken) query["continuation-token"] = options.continuationToken;
    if (options?.maxKeys) query["max-keys"] = String(options.maxKeys);
    const url = this.buildUrl("", query);
    const signed = signS3Request({
      method: "GET",
      url,
      payloadHash: sha256Hex(""),
      accessKeyId: this.accessKeyId,
      secretAccessKey: this.secretAccessKey,
      region: this.region
    });
    const response = await this.executeRequest(signed);
    return parseListObjectsV2Xml(response.body);
  }
  async deleteObjects(keys) {
    if (keys.length === 0) {
      return { deletedKeys: [], errors: [] };
    }
    const objectsXml = keys.map((k) => `<Object><Key>${escapeXml(k)}</Key></Object>`).join("");
    const xmlBody = `<?xml version="1.0" encoding="UTF-8"?><Delete><Quiet>false</Quiet>${objectsXml}</Delete>`;
    const bodyBuffer = Buffer.from(xmlBody, "utf8");
    const payloadHash = sha256Hex(bodyBuffer);
    const url = this.buildUrl("", { delete: "" });
    const signed = signS3Request({
      method: "POST",
      url,
      headers: {
        "content-type": "application/xml",
        "content-length": String(bodyBuffer.length),
        // AWS rejects multi-object deletes without it (MissingContentMD5).
        "content-md5": (0, import_node_crypto2.createHash)("md5").update(bodyBuffer).digest("base64")
      },
      payloadHash,
      accessKeyId: this.accessKeyId,
      secretAccessKey: this.secretAccessKey,
      region: this.region
    });
    const response = await this.executeRequest(signed, bodyBuffer);
    return parseDeleteResultXml(response.body, keys);
  }
  async testConnection() {
    await this.listObjectsV2({ maxKeys: 1 });
    return { ok: true, bucket: this.bucket, region: this.region };
  }
  executeRequest(signed, body) {
    const target = new URL(signed.url);
    const send = target.protocol === "http:" ? import_node_http.request : import_node_https.request;
    return new Promise((resolve, reject) => {
      const idle = setTimeout(() => {
        const stalled = new Error(`S3 request stalled for ${this.timeoutMs}ms`);
        req.destroy(stalled);
        reject(stalled);
      }, this.timeoutMs);
      const settle = (finish) => (value) => {
        clearTimeout(idle);
        finish(value);
      };
      const fail = settle(reject);
      const req = send(
        target,
        {
          method: signed.method,
          headers: signed.headers
        },
        (res) => {
          const chunks = [];
          let received = 0;
          res.on("data", (chunk) => {
            idle.refresh();
            received += chunk.length;
            if (received > MAX_RESPONSE_BYTES) {
              const oversized = new Error(`S3 response exceeded ${MAX_RESPONSE_BYTES} bytes`);
              req.destroy(oversized);
              fail(oversized);
              return;
            }
            chunks.push(chunk);
          });
          res.on("error", fail);
          res.on("end", () => {
            const statusCode = res.statusCode ?? 0;
            const resBody = Buffer.concat(chunks).toString("utf8");
            if (statusCode >= 200 && statusCode < 300) {
              settle(resolve)({ statusCode, headers: res.headers, body: resBody });
              return;
            }
            fail(parseS3ErrorXml(resBody, statusCode));
          });
        }
      );
      req.on("error", fail);
      if (body) {
        const source = Buffer.isBuffer(body) ? import_node_stream.Readable.from(sliceBuffer(body)) : body;
        source.on("error", (streamErr) => {
          req.destroy(streamErr);
          fail(streamErr);
        });
        source.on("data", () => idle.refresh());
        source.pipe(req);
      } else {
        req.end();
      }
    });
  }
};
async function hashInSlices(buffer) {
  const hash = (0, import_node_crypto2.createHash)("sha256");
  for (let offset = 0; offset < buffer.length; offset += HASH_SLICE_BYTES) {
    if (offset > 0) await new Promise((resolve) => setImmediate(resolve));
    hash.update(buffer.subarray(offset, offset + HASH_SLICE_BYTES));
  }
  return hash.digest("hex");
}
function* sliceBuffer(buffer) {
  for (let offset = 0; offset < buffer.length; offset += BODY_SLICE_BYTES) {
    yield buffer.subarray(offset, offset + BODY_SLICE_BYTES);
  }
}
var XML_ENTITIES = {
  amp: "&",
  lt: "<",
  gt: ">",
  quot: '"',
  apos: "'"
};
function unescapeXml(str) {
  return str.replace(/&(?:#x([0-9a-f]+)|#(\d+)|(\w+));/gi, (entity, hex, decimal, name) => {
    if (hex !== void 0) return String.fromCodePoint(Number.parseInt(hex, 16));
    if (decimal !== void 0) return String.fromCodePoint(Number.parseInt(decimal, 10));
    return XML_ENTITIES[name.toLowerCase()] ?? entity;
  });
}
function decodeText(raw) {
  return raw === void 0 ? void 0 : unescapeXml(raw.trim());
}
function escapeXml(str) {
  return str.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&apos;");
}
function parseS3ErrorXml(xml, statusCode) {
  const codeMatch = /<Code>(.*?)<\/Code>/s.exec(xml);
  const messageMatch = /<Message>(.*?)<\/Message>/s.exec(xml);
  const code = codeMatch?.[1]?.trim() ?? `HTTP_${statusCode}`;
  const message = decodeText(messageMatch?.[1]) ?? (xml.slice(0, 200).trim() || `Request failed with HTTP status ${statusCode}`);
  return new S3Error(code, message, statusCode, xml);
}
function parseListObjectsV2Xml(xml) {
  const isTruncated = /<IsTruncated>(true|false)<\/IsTruncated>/i.exec(xml)?.[1]?.toLowerCase() === "true";
  const nextToken = decodeText(
    /<NextContinuationToken>(.*?)<\/NextContinuationToken>/s.exec(xml)?.[1]
  );
  const objects = [];
  const contentsRegex = /<Contents>(.*?)<\/Contents>/gs;
  let match = null;
  while (true) {
    match = contentsRegex.exec(xml);
    if (!match) break;
    const itemXml = match[1] ?? "";
    const key = decodeText(/<Key>(.*?)<\/Key>/s.exec(itemXml)?.[1]);
    const lastModifiedRaw = /<LastModified>(.*?)<\/LastModified>/s.exec(itemXml)?.[1]?.trim();
    const sizeRaw = /<Size>(\d+)<\/Size>/s.exec(itemXml)?.[1]?.trim();
    const etag = (/ <ETag>(.*?)<\/ETag>/s.exec(itemXml)?.[1] ?? /<ETag>(.*?)<\/ETag>/s.exec(itemXml)?.[1]?.trim() ?? "").replace(/"/g, "");
    if (key && lastModifiedRaw) {
      objects.push({
        key,
        lastModified: new Date(lastModifiedRaw),
        sizeBytes: sizeRaw ? Number.parseInt(sizeRaw, 10) : 0,
        etag
      });
    }
  }
  return {
    objects,
    isTruncated,
    nextContinuationToken: nextToken
  };
}
function parseDeleteResultXml(xml, fallbackKeys) {
  const deletedKeys = [];
  const errors = [];
  const deletedRegex = /<Deleted>(.*?)<\/Deleted>/gs;
  let dMatch = null;
  while (true) {
    dMatch = deletedRegex.exec(xml);
    if (!dMatch) break;
    const k = decodeText(/<Key>(.*?)<\/Key>/s.exec(dMatch[1] ?? "")?.[1]);
    if (k) deletedKeys.push(k);
  }
  const errorRegex = /<Error>(.*?)<\/Error>/gs;
  let eMatch = null;
  while (true) {
    eMatch = errorRegex.exec(xml);
    if (!eMatch) break;
    const eXml = eMatch[1] ?? "";
    const k = decodeText(/<Key>(.*?)<\/Key>/s.exec(eXml)?.[1]) ?? "";
    const code = /<Code>(.*?)<\/Code>/s.exec(eXml)?.[1]?.trim() ?? "Unknown";
    const msg = decodeText(/<Message>(.*?)<\/Message>/s.exec(eXml)?.[1]) ?? "";
    errors.push({ key: k, code, message: msg });
  }
  if (deletedKeys.length === 0 && errors.length === 0) {
    return { deletedKeys: [...fallbackKeys], errors: [] };
  }
  return { deletedKeys, errors };
}

// src/s3/sink.ts
var import_promises5 = require("node:fs/promises");
var import_node_path5 = __toESM(require("node:path"), 1);

// src/zip-sink.ts
var import_node_fs3 = require("node:fs");
var import_promises4 = require("node:fs/promises");
var import_node_http2 = require("node:http");
var import_node_https2 = require("node:https");
var import_node_path4 = __toESM(require("node:path"), 1);
var LocalFileSink = class {
  constructor(filePath) {
    this.filePath = filePath;
  }
  filePath;
  async open() {
    await (0, import_promises4.mkdir)(import_node_path4.default.dirname(this.filePath), { recursive: true });
    const partPath = `${this.filePath}.part`;
    const filePath = this.filePath;
    return {
      spoolPath: partPath,
      finish: async (spooled) => {
        await (0, import_promises4.rename)(partPath, filePath);
        return { ...spooled, location: filePath };
      },
      abort: () => (0, import_promises4.rm)(partPath, { force: true })
    };
  }
};
var UploadSink = class {
  constructor(options) {
    this.options = options;
  }
  options;
  async open() {
    const root = this.options.tmpDir;
    await (0, import_promises4.mkdir)(root, { recursive: true });
    const dir = await (0, import_promises4.mkdtemp)(import_node_path4.default.join(root, "qbx-db-backup-"));
    const spoolPath = import_node_path4.default.join(dir, "backup.zip");
    const upload = this.options;
    return {
      spoolPath,
      finish: async (result) => {
        try {
          if (upload.maxBytes !== void 0 && result.bytesZip > upload.maxBytes) {
            throw new Error(
              `Backup zip is ${result.bytesZip} bytes, over the ${upload.maxBytes} byte limit for this job`
            );
          }
          const target = await upload.resolveUpload({
            sizeBytes: result.bytesZip,
            sha256: result.sha256
          });
          await putFile(target.url, spoolPath, result.bytesZip, target.headers ?? {});
          const location = await keepLocalCopy(spoolPath, upload.keepLocalPath);
          return location === null ? result : { ...result, location };
        } finally {
          await (0, import_promises4.rm)(dir, { recursive: true, force: true });
        }
      },
      abort: () => (0, import_promises4.rm)(dir, { recursive: true, force: true })
    };
  }
};
async function keepLocalCopy(spoolPath, destination) {
  if (destination === void 0 || destination.length === 0) return null;
  await (0, import_promises4.mkdir)(import_node_path4.default.dirname(destination), { recursive: true });
  const partPath = `${destination}.part`;
  await (0, import_promises4.copyFile)(spoolPath, partPath);
  await (0, import_promises4.rename)(partPath, destination);
  return destination;
}
function putFile(url, filePath, size, headers) {
  const target = new URL(url);
  const send = target.protocol === "http:" ? import_node_http2.request : import_node_https2.request;
  return new Promise((resolve, reject) => {
    const req = send(
      target,
      {
        method: "PUT",
        headers: {
          "Content-Type": "application/zip",
          ...headers,
          "Content-Length": String(size)
        }
      },
      (res) => {
        const chunks = [];
        res.on("data", (chunk) => {
          if (chunks.length < 32) chunks.push(chunk);
        });
        res.on("error", reject);
        res.on("end", () => {
          const status = res.statusCode ?? 0;
          if (status >= 200 && status < 300) {
            resolve();
            return;
          }
          const body2 = Buffer.concat(chunks).toString("utf8").slice(0, 300);
          reject(new Error(`Upload failed with HTTP ${status}${body2 ? `: ${body2}` : ""}`));
        });
      }
    );
    req.on("error", reject);
    const body = (0, import_node_fs3.createReadStream)(filePath);
    body.on("error", reject);
    body.pipe(req);
  });
}

// src/s3/sink.ts
var S3Sink = class {
  constructor(options) {
    this.options = options;
  }
  options;
  async open() {
    const root = this.options.tmpDir;
    await (0, import_promises5.mkdir)(root, { recursive: true });
    const dir = await (0, import_promises5.mkdtemp)(import_node_path5.default.join(root, "qbx-s3-sink-"));
    const spoolPath = import_node_path5.default.join(dir, "backup.zip");
    const opts = this.options;
    return {
      spoolPath,
      finish: async (result) => {
        try {
          if (opts.maxBytes !== void 0 && result.bytesZip > opts.maxBytes) {
            throw new Error(
              `Backup zip is ${result.bytesZip} bytes, exceeding the ${opts.maxBytes} byte limit`
            );
          }
          await opts.client.putFile(opts.s3Key, spoolPath, result.bytesZip, result.sha256);
          const localCopy = await keepLocalCopy(spoolPath, opts.keepLocalPath);
          const s3Location = `s3://${opts.client.bucket}/${opts.s3Key}`;
          return localCopy === null ? { ...result, location: s3Location } : { ...result, location: `${s3Location} (+ local: ${localCopy})` };
        } finally {
          await (0, import_promises5.rm)(dir, { recursive: true, force: true });
        }
      },
      abort: () => (0, import_promises5.rm)(dir, { recursive: true, force: true })
    };
  }
};

// src/schedule.ts
var import_promises6 = require("node:fs/promises");
var import_node_path6 = __toESM(require("node:path"), 1);
var STARTUP_GRACE_MS = 6e4;
var STATE_FILE_NAME = ".state.json";
var HOUR_MS = 36e5;
function nextRunAt(lastRunAt, intervalHours, now) {
  if (lastRunAt !== null) {
    const due = lastRunAt + intervalHours * HOUR_MS;
    if (due > now) return due;
  }
  return now + STARTUP_GRACE_MS;
}
async function readLastRunAt(dir) {
  try {
    const raw = await (0, import_promises6.readFile)(import_node_path6.default.join(dir, STATE_FILE_NAME), "utf8");
    const parsed = JSON.parse(raw);
    const value = parsed?.lastRunAt;
    return typeof value === "number" && Number.isFinite(value) ? value : null;
  } catch {
    return null;
  }
}
async function writeLastRunAt(dir, lastRunAt) {
  await (0, import_promises6.mkdir)(dir, { recursive: true });
  await (0, import_promises6.writeFile)(import_node_path6.default.join(dir, STATE_FILE_NAME), `${JSON.stringify({ lastRunAt })}
`, "utf8");
}

// src/server.ts
var PROGRESS_INTERVAL_MS = 1e4;
var MAX_TIMER_MS = 2147483e3;
var SIZE_UNITS = ["B", "KB", "MB", "GB", "TB"];
var config;
var api = null;
var s3Client = null;
var dumpBinary = null;
var pollTimer = null;
var scheduleTimer = null;
var scheduledAt = null;
var lastHeartbeat = null;
var lastOutcome = "no backup has run yet";
function resourceDirectory() {
  try {
    return GetResourcePath(GetCurrentResourceName());
  } catch {
    return process.cwd();
  }
}
function targetLabel() {
  try {
    return describeTarget(parseConnectionString(config.connectionString));
  } catch {
    return redactConnectionString(config.connectionString) || "<not configured>";
  }
}
function databaseName() {
  try {
    return parseConnectionString(config.connectionString).database;
  } catch {
    return "";
  }
}
function describeBinary(binary) {
  return `${binary.command} (${binary.version}) [${binary.source}]`;
}
function formatLocalTime(at) {
  return new Date(at).toLocaleString();
}
function formatBytes(value) {
  let size = Math.max(0, value);
  let unit = 0;
  while (size >= 1024 && unit < SIZE_UNITS.length - 1) {
    size /= 1024;
    unit += 1;
  }
  return `${unit === 0 ? size : size.toFixed(1)} ${SIZE_UNITS[unit] ?? "B"}`;
}
function parseTimestamp(value) {
  if (value === null || value.length === 0) return null;
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : null;
}
function detect() {
  return detectDumpBinary({
    explicitPath: config.dumpBin.length > 0 ? config.dumpBin : void 0,
    resourceDir: config.resourceDir
  });
}
async function refreshDumpBinary() {
  try {
    dumpBinary = await detect();
    info(`dump binary: ${describeBinary(dumpBinary)}`);
  } catch (failure) {
    dumpBinary = null;
    warn(errorMessage(failure));
  }
}
function describeOutcome(outcome) {
  if (outcome.busy) return "a backup is already running";
  const warnings = outcome.warnings.length > 0 ? ` warnings=${outcome.warnings.join("; ")}` : "";
  return `ok zip=${formatBytes(outcome.sizeBytes)} sql=${formatBytes(outcome.rawBytes)} in ${Math.round(outcome.durationMs / 1e3)}s${outcome.location ? ` -> ${outcome.location}` : ""}${warnings}`;
}
function describeSchedule() {
  if (config.intervalHours <= 0) return "disabled";
  const next = scheduledAt === null ? "not scheduled" : formatLocalTime(scheduledAt);
  return `every ${config.intervalHours} h, next at ${next}`;
}
function describeRetention() {
  const parts = [`local_keep=${config.localKeep}`];
  if (config.localMaxAgeDays > 0) parts.push(`local_max_age=${config.localMaxAgeDays}d`);
  if (config.minFreeDiskMb > 0) parts.push(`min_free_disk=${config.minFreeDiskMb}MB`);
  if (isS3Configured(config.s3)) {
    if (config.s3.keepCount > 0) parts.push(`s3_keep=${config.s3.keepCount}`);
    if (config.s3.maxAgeDays > 0) parts.push(`s3_max_age=${config.s3.maxAgeDays}d`);
  }
  return parts.join(", ");
}
async function pruneLocal() {
  try {
    const res = await pruneLocalDirectory(config.localDir, {
      keepCount: config.localKeep,
      maxAgeDays: config.localMaxAgeDays,
      minFreeDiskMb: config.minFreeDiskMb
    });
    if (res.deletedFiles.length > 0) {
      info(
        `pruned ${res.deletedFiles.length} old local backup(s) (${formatBytes(res.freedBytes)} freed), ${res.remainingCount} remaining`
      );
    }
  } catch (failure) {
    warn(`local retention pruning warning: ${errorMessage(failure)}`);
  }
}
async function pruneS3() {
  if (s3Client === null) return;
  if (config.s3.keepCount <= 0 && config.s3.maxAgeDays <= 0) return;
  try {
    const res = await pruneS3Bucket(s3Client, {
      prefix: config.s3.prefix,
      keepCount: config.s3.keepCount,
      maxAgeDays: config.s3.maxAgeDays
    });
    if (res.deletedKeys.length > 0) {
      info(
        `pruned ${res.deletedKeys.length} expired backup(s) from S3 bucket "${s3Client.bucket}"`
      );
    }
    if (res.errors.length > 0) {
      warn(`S3 retention delete error: ${res.errors.map((e) => e.message).join("; ")}`);
    }
  } catch (failure) {
    warn(`S3 retention pruning warning: ${errorMessage(failure)}`);
  }
}
async function runLocalBackup() {
  const target = parseConnectionString(config.connectionString);
  const names = buildBackupNames(target.database, /* @__PURE__ */ new Date());
  const sink = new LocalFileSink(import_node_path7.default.join(config.localDir, names.zipName));
  const outcome = await runBackup({ config, sink, entryName: names.entryName });
  lastOutcome = describeOutcome(outcome);
  info(lastOutcome);
  if (!outcome.busy) await pruneLocal();
  return outcome;
}
async function runS3Backup() {
  if (s3Client === null) throw new Error("S3 client is not configured");
  const target = parseConnectionString(config.connectionString);
  const names = buildBackupNames(target.database, /* @__PURE__ */ new Date());
  const key = config.s3.prefix ? `${config.s3.prefix.replace(/\/+$/, "")}/${names.zipName}` : names.zipName;
  const sink = new S3Sink({
    client: s3Client,
    s3Key: key,
    tmpDir: import_node_path7.default.join(config.localDir, ".tmp"),
    keepLocalPath: config.keepLocal ? import_node_path7.default.join(config.localDir, names.zipName) : void 0
  });
  info(`starting S3 upload -> s3://${s3Client.bucket}/${key}`);
  const outcome = await runBackup({
    config,
    sink,
    entryName: names.entryName,
    timeoutMs: config.timeoutMinutes * 6e4
  });
  lastOutcome = describeOutcome(outcome);
  info(lastOutcome);
  if (!outcome.busy) {
    await pruneS3();
    if (config.keepLocal) await pruneLocal();
  }
  return outcome;
}
async function runJob(job) {
  if (api === null) return;
  const client = api;
  const sink = new UploadSink({
    resolveUpload: async (upload) => {
      const ticket = await client.requestUpload(job.jobId, upload);
      return { url: ticket.uploadUrl, headers: ticket.uploadHeaders };
    },
    tmpDir: import_node_path7.default.join(config.localDir, ".tmp"),
    keepLocalPath: config.keepLocal ? import_node_path7.default.join(config.localDir, job.fileName) : void 0
  });
  let lastPost = 0;
  info(`starting Qbox backup job ${job.jobId} -> ${job.fileName}`);
  try {
    const outcome = await runBackup({
      config,
      sink,
      entryName: job.entryName,
      timeoutMs: Math.min(job.timeoutMs, config.timeoutMinutes * 6e4),
      onProgress: (progress) => {
        const now = Date.now();
        if (now - lastPost < PROGRESS_INTERVAL_MS) return;
        lastPost = now;
        void client.progress(job.jobId, progress).catch(() => {
        });
      }
    });
    lastOutcome = describeOutcome(outcome);
    info(lastOutcome);
    if (outcome.busy) return;
    if (config.keepLocal) await pruneLocal();
    const result = await client.complete(job.jobId, {
      ok: true,
      sizeBytes: outcome.sizeBytes,
      rawBytes: outcome.rawBytes,
      sha256: outcome.sha256,
      durationMs: outcome.durationMs,
      warnings: outcome.warnings
    });
    info(`job ${job.jobId}: ${result.status}`);
    if (result.status === "failed" && result.error) {
      lastOutcome = `failed: ${result.error}`;
      error(result.error);
    }
  } catch (failure) {
    lastOutcome = `failed: ${errorMessage(failure)}`;
    error(lastOutcome);
    await client.complete(job.jobId, { ok: false, error: errorMessage(failure) }).catch((reportFailure) => error(errorMessage(reportFailure)));
    throw failure;
  }
}
async function requestJob(trigger) {
  if (api === null) return null;
  const created = await api.createJob(trigger).catch((failure) => {
    throw new DestinationError(failure);
  });
  if (created.status === "throttled") {
    const allowedAt = parseTimestamp(created.nextAllowedAt);
    info(
      `next backup allowed at ${allowedAt === null ? "a later time" : formatLocalTime(allowedAt)}`
    );
    return allowedAt;
  }
  if (created.status === "busy") {
    info(`a backup job (${created.jobId}) is still in progress, skipping this run`);
    return null;
  }
  await runJob(created.job);
  return null;
}
async function runS3WithLocalFallback() {
  try {
    await runS3Backup();
  } catch (failure) {
    if (!(failure instanceof DestinationError)) throw failure;
    warn(`S3 upload failed (${errorMessage(failure)}); falling back to local storage`);
    await runLocalBackup();
  }
}
async function executeBackupPipeline(trigger) {
  if (isBackupRunning()) {
    info("a backup is already running");
    return null;
  }
  if (config.mode === "qbx") {
    try {
      return await requestJob(trigger);
    } catch (failure) {
      if (!(failure instanceof DestinationError)) throw failure;
      const next = s3Client === null ? "local storage" : "S3";
      warn(`Qbox dashboard upload failed (${errorMessage(failure)}); falling back to ${next}`);
    }
    if (s3Client === null) await runLocalBackup();
    else await runS3WithLocalFallback();
    return null;
  }
  if (config.mode === "s3") await runS3WithLocalFallback();
  else await runLocalBackup();
  return null;
}
function armSchedule(at) {
  if (scheduleTimer !== null) clearTimeout(scheduleTimer);
  scheduledAt = at;
  const delay2 = Math.max(0, at - Date.now());
  scheduleTimer = setTimeout(
    delay2 > MAX_TIMER_MS ? () => armSchedule(at) : () => void runScheduled(),
    Math.min(delay2, MAX_TIMER_MS)
  );
}
async function runScheduled() {
  scheduleTimer = null;
  const startedAt = Date.now();
  let nextAt = startedAt + config.intervalHours * HOUR_MS;
  if (isBackupRunning()) {
    info("a backup is already running, skipping this scheduled run");
    armSchedule(nextAt);
    return;
  }
  await writeLastRunAt(config.localDir, startedAt).catch((failure) => {
    warn(errorMessage(failure));
  });
  try {
    const allowedAt = await executeBackupPipeline("scheduled");
    if (allowedAt !== null) nextAt = allowedAt;
  } catch (failure) {
    lastOutcome = `failed: ${errorMessage(failure)}`;
    error(lastOutcome);
  }
  armSchedule(nextAt);
}
async function startSchedule() {
  const lastRunAt = await readLastRunAt(config.localDir);
  armSchedule(nextRunAt(lastRunAt, config.intervalHours, Date.now()));
  info(`scheduled backups: ${describeSchedule()}`);
}
async function pollOnce() {
  if (api === null) return;
  lastHeartbeat = await api.heartbeat({
    version: RESOURCE_VERSION,
    platform: `${process.platform}-${process.arch}`,
    dumpBinary,
    database: databaseName(),
    intervalHours: config.intervalHours
  });
}
function printUsage() {
  info("usage: qbx_db_backup <run|status|test|version>");
}
function commandStatus() {
  info(`mode: ${config.mode}`);
  info(`target: ${targetLabel()}`);
  info(`state: ${isBackupRunning() ? "busy" : "idle"}`);
  info(`dump binary: ${dumpBinary ? describeBinary(dumpBinary) : "not found"}`);
  info(`schedule: ${describeSchedule()}`);
  info(`retention: ${describeRetention()}`);
  if (isS3Configured(config.s3)) {
    const ep = config.s3.endpoint ?? `s3.${config.s3.region}.amazonaws.com`;
    info(
      `s3 storage: bucket="${config.s3.bucket}" endpoint="${ep}" region="${config.s3.region}" key="${config.s3.accessKeyId}" secret="${redactSecret(config.s3.secretAccessKey)}"`
    );
  }
  if (lastHeartbeat !== null) {
    info(
      `dashboard plan ${lastHeartbeat.plan}: ${formatBytes(lastHeartbeat.usedBytes)} / ${formatBytes(lastHeartbeat.poolBytes)} used`
    );
  }
  info(`last result: ${lastOutcome}`);
}
async function commandTest() {
  try {
    const target = parseConnectionString(config.connectionString);
    info(`database: parsed ${describeTarget(target)} (ssl=${target.ssl})`);
    const binary = await detect();
    dumpBinary = binary;
    info(`dump binary: ${describeBinary(binary)}`);
    if (config.mode === "qbx" && api !== null) {
      info(`testing Qbox dashboard API at ${config.apiBase}...`);
      await pollOnce();
      info(`Qbox dashboard connected (plan=${lastHeartbeat?.plan ?? "unknown"})`);
    }
    if (isS3Configured(config.s3) && s3Client !== null) {
      info(
        `testing S3 storage at "${config.s3.endpoint ?? "AWS S3"}" (bucket: ${config.s3.bucket})...`
      );
      await s3Client.testConnection();
      info(`S3 storage connected successfully (bucket "${config.s3.bucket}" is accessible)`);
    }
  } catch (failure) {
    error(errorMessage(failure));
  }
}
async function commandRun() {
  try {
    await executeBackupPipeline("manual");
  } catch (failure) {
    lastOutcome = `failed: ${errorMessage(failure)}`;
    error(lastOutcome);
  }
}
function start() {
  const resourceDir = resourceDirectory();
  config = loadConfig((name, fallback) => GetConvar(name, fallback), {
    localDir: import_node_path7.default.join(resourceDir, "backups"),
    resourceDir
  });
  if (isS3Configured(config.s3)) {
    s3Client = new S3Client(config.s3);
  }
  info(
    `v${RESOURCE_VERSION} mode=${config.mode} target=${targetLabel()} local_dir=${config.localDir}`
  );
  if (config.connectionString.length === 0) {
    warn("no connection string: set mysql_connection_string or qbx_db_backup_connection_string");
  }
  if (config.intervalClamped) {
    warn("qbx_db_backup_interval_hours below 1 was raised to 1; use 0 to disable the schedule");
  }
  void refreshDumpBinary();
  if (config.mode === "qbx") {
    api = new AgentApi({
      baseUrl: config.apiBase,
      token: config.token,
      version: RESOURCE_VERSION
    });
    info(`polling ${config.apiBase} every ${config.pollSeconds}s`);
    const poll = () => {
      void pollOnce().catch((failure) => error(errorMessage(failure)));
    };
    setTimeout(poll, 5e3);
    pollTimer = setInterval(poll, config.pollSeconds * 1e3);
  }
  if (config.intervalHours > 0) {
    void startSchedule().catch((failure) => error(errorMessage(failure)));
  } else {
    info("scheduled backups: disabled");
  }
  RegisterCommand(
    "qbx_db_backup",
    (_source, args) => {
      switch ((args[0] ?? "").toLowerCase()) {
        case "run":
          void commandRun();
          break;
        case "status":
          commandStatus();
          break;
        case "test":
          void commandTest();
          break;
        case "version":
          info(`qbx_db_backup v${RESOURCE_VERSION}`);
          break;
        default:
          printUsage();
      }
    },
    true
  );
  on("onResourceStop", (name) => {
    if (name !== GetCurrentResourceName()) return;
    if (pollTimer !== null) clearInterval(pollTimer);
    pollTimer = null;
    if (scheduleTimer !== null) clearTimeout(scheduleTimer);
    scheduleTimer = null;
    scheduledAt = null;
    cancelRunningBackup("Resource stopped");
  });
}
try {
  start();
} catch (failure) {
  error(`startup failed: ${errorMessage(failure)}`);
}
