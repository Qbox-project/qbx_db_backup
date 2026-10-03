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

// src/cli.ts
var import_node_path5 = __toESM(require("node:path"), 1);

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
function toInt(value, fallback) {
  const parsed = Number.parseInt(value.trim(), 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}
function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

// src/s3/client.ts
var import_node_crypto2 = require("node:crypto");
var import_node_fs2 = require("node:fs");
var import_promises2 = require("node:fs/promises");
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
    const file = await (0, import_promises2.open)(filePath, "r");
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
var import_promises4 = require("node:fs/promises");
var import_node_path4 = __toESM(require("node:path"), 1);

// src/zip-sink.ts
var import_promises3 = require("node:fs/promises");
var import_node_path3 = __toESM(require("node:path"), 1);
var LocalFileSink = class {
  constructor(filePath) {
    this.filePath = filePath;
  }
  filePath;
  async open() {
    await (0, import_promises3.mkdir)(import_node_path3.default.dirname(this.filePath), { recursive: true });
    const partPath = `${this.filePath}.part`;
    const filePath = this.filePath;
    return {
      spoolPath: partPath,
      finish: async (spooled) => {
        await (0, import_promises3.rename)(partPath, filePath);
        return { ...spooled, location: filePath };
      },
      abort: () => (0, import_promises3.rm)(partPath, { force: true })
    };
  }
};
async function keepLocalCopy(spoolPath, destination) {
  if (destination === void 0 || destination.length === 0) return null;
  await (0, import_promises3.mkdir)(import_node_path3.default.dirname(destination), { recursive: true });
  const partPath = `${destination}.part`;
  await (0, import_promises3.copyFile)(spoolPath, partPath);
  await (0, import_promises3.rename)(partPath, destination);
  return destination;
}

// src/s3/sink.ts
var S3Sink = class {
  constructor(options) {
    this.options = options;
  }
  options;
  async open() {
    const root = this.options.tmpDir;
    await (0, import_promises4.mkdir)(root, { recursive: true });
    const dir = await (0, import_promises4.mkdtemp)(import_node_path4.default.join(root, "qbx-s3-sink-"));
    const spoolPath = import_node_path4.default.join(dir, "backup.zip");
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
          await (0, import_promises4.rm)(dir, { recursive: true, force: true });
        }
      },
      abort: () => (0, import_promises4.rm)(dir, { recursive: true, force: true })
    };
  }
};

// src/cli.ts
var FLAG_BY_CONVAR = {
  mysql_connection_string: "connection",
  qbx_db_backup_connection_string: "connection",
  qbx_db_backup_local_dir: "out",
  qbx_db_backup_dump_bin: "dump-bin",
  qbx_db_backup_zip_level: "zip-level",
  qbx_db_backup_max_mb_per_second: "max-mb-per-second",
  qbx_db_backup_timeout_minutes: "timeout-minutes",
  qbx_db_backup_s3_endpoint: "s3-endpoint",
  qbx_db_backup_s3_bucket: "s3-bucket",
  qbx_db_backup_s3_region: "s3-region",
  qbx_db_backup_s3_access_key_id: "s3-key",
  qbx_db_backup_s3_key: "s3-key",
  qbx_db_backup_s3_secret_access_key: "s3-secret",
  qbx_db_backup_s3_secret: "s3-secret",
  qbx_db_backup_s3_prefix: "s3-prefix",
  qbx_db_backup_s3_force_path_style: "s3-path-style",
  qbx_db_backup_s3_keep: "s3-keep",
  qbx_db_backup_s3_max_age_days: "s3-max-age"
};
var USAGE = `qbx_db_backup CLI v${RESOURCE_VERSION}

usage:
  node dist/cli.js run  --connection "<string>" [--out ./backups] [--dump-bin path] [--zip-level 6]
  node dist/cli.js test --connection "<string>" [--dump-bin path] [--s3-bucket b --s3-key k --s3-secret s]

Flags fall back to environment variables: MYSQL_CONNECTION_STRING,
QBX_DB_BACKUP_CONNECTION_STRING, QBX_DB_BACKUP_LOCAL_DIR, QBX_DB_BACKUP_DUMP_BIN,
QBX_DB_BACKUP_ZIP_LEVEL, QBX_DB_BACKUP_TIMEOUT_MINUTES, QBX_DB_BACKUP_S3_BUCKET,
QBX_DB_BACKUP_S3_KEY, QBX_DB_BACKUP_S3_SECRET, QBX_DB_BACKUP_S3_ENDPOINT,
QBX_DB_BACKUP_S3_REGION.`;
function parseFlags(argv) {
  const flags = {};
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (token === void 0 || !token.startsWith("--")) continue;
    const inline = token.indexOf("=");
    if (inline > 0) {
      flags[token.slice(2, inline)] = token.slice(inline + 1);
      continue;
    }
    const next = argv[index + 1];
    if (next === void 0 || next.startsWith("--")) {
      flags[token.slice(2)] = "1";
      continue;
    }
    flags[token.slice(2)] = next;
    index += 1;
  }
  return flags;
}
function buildConfig(flags) {
  const source = (name, fallback) => {
    if (name === "qbx_db_backup_token") return "";
    const flagName = FLAG_BY_CONVAR[name];
    const fromFlag = flagName === void 0 ? void 0 : flags[flagName];
    if (fromFlag !== void 0) return fromFlag;
    return process.env[name.toUpperCase()] ?? fallback;
  };
  return loadConfig(source, {
    localDir: import_node_path5.default.resolve("backups"),
    resourceDir: import_node_path5.default.resolve(__dirname, "..")
  });
}
async function commandRun(config) {
  const target = parseConnectionString(config.connectionString);
  const names = buildBackupNames(target.database, /* @__PURE__ */ new Date());
  let sink = new LocalFileSink(import_node_path5.default.resolve(config.localDir, names.zipName));
  if (isS3Configured(config.s3)) {
    const s3Client = new S3Client(config.s3);
    const key = config.s3.prefix ? `${config.s3.prefix.replace(/\/+$/, "")}/${names.zipName}` : names.zipName;
    sink = new S3Sink({
      client: s3Client,
      s3Key: key,
      tmpDir: import_node_path5.default.resolve(config.localDir, ".tmp"),
      keepLocalPath: config.keepLocal ? import_node_path5.default.resolve(config.localDir, names.zipName) : void 0
    });
  }
  const outcome = await runBackup({ config, sink, entryName: names.entryName });
  if (outcome.busy) {
    process.stderr.write("a backup is already running\n");
    return 1;
  }
  process.stdout.write(`${JSON.stringify(outcome, null, 2)}
`);
  return 0;
}
async function commandTest(config) {
  const target = parseConnectionString(config.connectionString);
  const binary = await detectDumpBinary({
    explicitPath: config.dumpBin.length > 0 ? config.dumpBin : void 0,
    resourceDir: config.resourceDir
  });
  let s3Status = null;
  if (isS3Configured(config.s3)) {
    try {
      const s3Client = new S3Client(config.s3);
      await s3Client.testConnection();
      s3Status = {
        ok: true,
        bucket: config.s3.bucket,
        endpoint: config.s3.endpoint ?? `s3.${config.s3.region}.amazonaws.com`
      };
    } catch (s3Err) {
      s3Status = { ok: false, error: errorMessage(s3Err) };
    }
  }
  process.stdout.write(
    `${JSON.stringify({ target: describeTarget(target), ssl: target.ssl, dumpBinary: binary, s3: s3Status }, null, 2)}
`
  );
  return 0;
}
async function main() {
  const argv = process.argv.slice(2);
  const command = argv[0] ?? "";
  const config = buildConfig(parseFlags(argv.slice(1)));
  if (command === "run") return await commandRun(config);
  if (command === "test") return await commandTest(config);
  process.stdout.write(`${USAGE}
`);
  return 1;
}
main().then((code) => {
  process.exitCode = code;
}).catch((failure) => {
  process.stderr.write(`${errorMessage(failure)}
`);
  process.exitCode = 1;
});
