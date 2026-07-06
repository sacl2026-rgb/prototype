/**
 * GreenyAgent — AI Durable Object (Phase 5: Qwen API)
 *
 * DO-resident hydroponics assistant. Calls Qwen DashScope API
 * (OpenAI-compatible) for native function-calling. Model decides
 * which tools to invoke, agent executes, results fed back.
 *
 * Exposed at POST /api/chat via Worker → DO stub.
 *
 * Tools:
 *   query_telemetry(device_id)   → DeviceHub DO /do-telemetry
 *   check_alerts(device_id, n)   → DeviceHub DO /do-alerts
 *   toggle_led(device_id, state) → DeviceHub DO /relay-cmd
 *   get_history(device_id, m, n) → D1 telemetry table (cold storage)
 */

import { DurableObject } from "cloudflare:workers";

// ── Types ──────────────────────────────────────────────────────────────────

interface AgentEnv {
  DEVICE_HUB: DurableObjectNamespace;
  GREENY_AGENT: DurableObjectNamespace;
  DB: D1Database;
  AI: Ai;
}

// ── System Prompt ──────────────────────────────────────────────────────────

const SYSTEM_PROMPT = `You are Greeny, a hydroponics AI assistant. You watch sensor data
from an ESP32 monitoring a hydroponic system. Your job is to translate
numbers into plant health. Normal is silence — only report deviations.

pH: 5.5-7.0 is optimal for most hydroponic crops. Below 5.0 or above
8.0 needs attention. If pH drifts slowly over days, probe needs
recalibration. If pH suddenly jumps to -10 or 34.95, probe is
disconnected — check BNC connector and amplifier board.

EC: 800-2000 µS/cm is typical. EC=0 means sensor disconnected. EC
rising without nutrient change means temperature effect (2%/°C is
normal physics). EC above 3000 needs dilution.

Temperature: 18-28°C optimal. Below 15°C roots slow. Above 30°C
stresses plants, increases pathogen risk.

Alerts: If you see ph_high or ph_low alerts that persist across
multiple readings, the condition is real — don't dismiss it.
Check if the probe was recently calibrated. If it was 30+ days ago,
suggest recalibration.

Tone: Be warm, precise, plant-focused. Don't list raw JSON. Say
'Your basil is thriving — pH 6.2 and stable' not 'pH: 6.2, EC: 1200.'
When something is wrong, explain what, why, and what to do.`;

// ── Tool Definitions (OpenAI function-calling format) ──────────────────────

const TOOLS = [
  {
    type: "function" as const,
    function: {
      name: "query_telemetry",
      description: "Get the latest sensor reading (tds, ec, ph, temp, led) for a device from live telemetry buffer",
      parameters: {
        type: "object",
        properties: {
          device_id: { type: "string", description: "Device ID, e.g. 'esp32-sensor'" },
        },
        required: ["device_id"],
      },
    },
  },
  {
    type: "function" as const,
    function: {
      name: "check_alerts",
      description: "Get recent alerts for a device from the alert buffer",
      parameters: {
        type: "object",
        properties: {
          device_id: { type: "string", description: "Device ID" },
          limit: { type: "number", description: "Max alerts to return (default 5)" },
        },
        required: ["device_id"],
      },
    },
  },
  {
    type: "function" as const,
    function: {
      name: "toggle_led",
      description: "Turn the grow LED on or off for a device",
      parameters: {
        type: "object",
        properties: {
          device_id: { type: "string", description: "Device ID" },
          state: { type: "string", enum: ["on", "off"], description: "Desired LED state" },
        },
        required: ["device_id", "state"],
      },
    },
  },
  {
    type: "function" as const,
    function: {
      name: "get_history",
      description: "Get historical trend data for a metric (ph, ec, tds, temp) from D1 cold storage. Returns array of {ts, value} in chronological order.",
      parameters: {
        type: "object",
        properties: {
          device_id: { type: "string", description: "Device ID" },
          metric: { type: "string", enum: ["ph", "ec", "tds", "temp"], description: "Which metric to retrieve" },
          limit: { type: "number", description: "Max data points (default 60)" },
        },
        required: ["device_id", "metric"],
      },
    },
  },
];

// ── GreenyAgent DO Class ───────────────────────────────────────────────────

export class GreenyAgent extends DurableObject {
  private declare env: AgentEnv;

  // ── Constructor: create SQLite tables for calibration state ─────────────

  constructor(ctx: DurableObjectState, env: AgentEnv) {
    super(ctx, env);
    this.env = env;

    ctx.storage.sql.exec(`CREATE TABLE IF NOT EXISTS calibration_sessions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      device_id TEXT NOT NULL,
      probe_type TEXT NOT NULL,
      status TEXT DEFAULT 'awaiting_point1',
      point1_value REAL,
      point1_mv REAL,
      point2_value REAL,
      point2_mv REAL,
      slope REAL,
      offset REAL,
      slope_pct REAL,
      created_at INTEGER,
      completed_at INTEGER
    )`);

    ctx.storage.sql.exec(`CREATE TABLE IF NOT EXISTS workflow_state (
      key TEXT PRIMARY KEY,
      value TEXT
    )`);
  }

  // ── HTTP Request Handler ───────────────────────────────────────────────

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const method = request.method;

    if (method === "OPTIONS") {
      return new Response(null, {
        status: 204,
        headers: {
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Methods": "POST, OPTIONS",
          "Access-Control-Allow-Headers": "Content-Type, Authorization",
        },
      });
    }

    if (url.pathname === "/api/chat" && method === "POST") {
      try {
        const body = (await request.json()) as { message: string };
        if (!body.message || typeof body.message !== "string") {
          return json({ error: "message required (string)" }, 400);
        }
        const reply = await this.chat(body.message);
        return json({ reply });
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        console.error("[GreenyAgent] chat error:", msg);
        return json({ error: "Internal error", detail: msg }, 500);
      }
    }

    return json({ error: "Not found" }, 404);
  }

  // ── Chat Router ─────────────────────────────────────────────────────────

  private async chat(userMessage: string): Promise<string> {
    const msg = userMessage.toLowerCase();
    const deviceId = "esp32-sensor";

    // LED toggle — instant regex shortcut, 0 API cost
    const ledMatch = msg.match(/turn\s+(on|off)\s+(the\s+)?(led|light)/i);
    if (ledMatch) {
      const state = ledMatch[1];
      const result = await this.tool_toggleLed(deviceId, state);
      if ((result as Record<string, unknown>).ok) {
        return `The LED is now ${state}. ${
          state === "on"
            ? "Your plants are getting extra light for photosynthesis."
            : "The light is off — your plants are in their dark cycle."
        }`;
      }
      return `I tried to turn the LED ${state}, but the device may be offline. The command is queued and will run when the ESP32 reconnects.`;
    }

    // Calibration state machine — multi-step, SQLite-tracked, 0 API cost
    const isCalCmd = msg.match(/^(calibrate|start calibration|run calibration|begin calibration)/i)
      || msg.match(/calibrate\s+(ph|ec|tds|the)/i);
    if (isCalCmd) return this.handleCalibration(msg, deviceId);

    const calKeywords = ["ready", "done", "ok", "go", "yes", "cancel", "stop", "abort", "status", "step", "where"];
    const activeSession = this.getActiveCalibration(deviceId);
    if (activeSession && calKeywords.some((k) => msg.includes(k))) {
      return this.handleCalibration(msg, deviceId);
    }

    // Everything else → pre-fetch real data, feed to Llama 3.2 for interpretation
    return this.chatSimple(userMessage);
  }

  // ── Simple Chat (pre-fetch data → prompt → Llama 3.2) ────────────────────

  private async chatSimple(userMessage: string): Promise<string> {
    const msg = userMessage.toLowerCase();
    const deviceId = "esp32-sensor";

    const dataFetches: Promise<[string, unknown]>[] = [];

    const wantsTelemetry = msg.includes("plant") || msg.includes("how are") ||
      msg.includes("status") || msg.includes("sensor") || msg.includes("reading") ||
      msg.includes("current") || msg.includes("now") || msg.includes("ph") ||
      msg.includes("ec") || msg.includes("tds") || msg.includes("temp") ||
      msg.includes("led state") || msg.includes("value");

    const wantsAlerts = msg.includes("alert") || msg.includes("warning") ||
      msg.includes("problem") || msg.includes("issue") || msg.includes("error") ||
      msg.includes("wrong") || msg.includes("anything wrong");

    const wantsHistory = msg.includes("history") || msg.includes("trend") ||
      msg.includes("chart") || msg.includes("past") || msg.includes("graph") ||
      msg.includes("over time") || msg.includes("last") || msg.includes("recent");

    if (wantsTelemetry) {
      dataFetches.push(this.tool_queryTelemetry(deviceId).then((r) => ["telemetry", r] as [string, unknown]));
    }
    if (wantsAlerts) {
      dataFetches.push(this.tool_checkAlerts(deviceId, 10).then((r) => ["alerts", r] as [string, unknown]));
    }
    if (wantsHistory) {
      const metric = msg.includes("ph") ? "ph" : msg.includes("ec") ? "ec" : msg.includes("tds") ? "tds" : msg.includes("temp") ? "temp" : "ph";
      dataFetches.push(this.tool_getHistory(deviceId, metric, 30).then((r) => ["history", r] as [string, unknown]));
    }

    if (dataFetches.length === 0) {
      dataFetches.push(
        this.tool_queryTelemetry(deviceId).then((r) => ["telemetry", r] as [string, unknown]),
        this.tool_checkAlerts(deviceId, 5).then((r) => ["alerts", r] as [string, unknown]),
      );
    }

    const dataContext = Object.fromEntries(await Promise.all(dataFetches));

    let dataBlock = "";
    if (dataContext.telemetry) {
      const t = dataContext.telemetry as Record<string, unknown>;
      if (t.status === "ok") {
        dataBlock += `\nCurrent sensor readings:\n  pH: ${t.ph}  |  EC: ${t.ec} µS/cm  |  TDS: ${t.tds} ppm  |  Temp: ${t.temp}°C  |  LED: ${t.led ? "ON" : "OFF"}\n`;
      } else {
        dataBlock += `\nSensor status: ${t.message || "No data available"}\n`;
      }
    }
    if (dataContext.alerts) {
      const a = dataContext.alerts as Record<string, unknown>;
      const alerts = a.alerts as Array<Record<string, unknown>> | undefined;
      if (alerts && alerts.length > 0) {
        dataBlock += `\nRecent alerts:\n${alerts.map((r) => `  - [${r.severity}] ${r.type}: ${r.message}`).join("\n")}\n`;
      } else {
        dataBlock += `\nAlerts: None — system is healthy.\n`;
      }
    }
    if (dataContext.history) {
      const h = dataContext.history as Record<string, unknown>;
      const points = h.data as Array<Record<string, unknown>> | undefined;
      if (points && points.length > 0) {
        const values = points.map((p) => p.value).join(", ");
        dataBlock += `\nHistorical ${h.metric} trend (${points.length} points, oldest→newest): ${values}\n`;
      }
    }

    const prompt = `[ROLE]
You are Greeny, a hydroponics AI assistant. You help users monitor their plants.

[KNOWLEDGE]
${SYSTEM_PROMPT}

[REAL DATA — use ONLY these values, never make up readings]
${dataBlock}

[USER QUESTION]
${userMessage}

[RULES]
- Use ONLY the sensor values provided above. Never make up numbers.
- Be warm, precise, and plant-focused.
- If data shows "No data available," tell the user the sensors may be offline.
- Keep your response to 3-5 sentences.
- If there are alerts, explain what they mean and what to do.
- Do NOT add notes, meta-commentary, or self-references about your response.

[RESPONSE]`;

    const aiResponse = (await this.env.AI.run("@cf/meta/llama-3.2-3b-instruct", {
      prompt,
      max_tokens: 512,
    })) as { response?: string };

    return (aiResponse.response || "").trim()
      || "I couldn't process that request. Try asking about your plants or sensor readings.";
  }

  // ── Tool Dispatcher ─────────────────────────────────────────────────────

  private async executeTool(name: string, args: Record<string, unknown>): Promise<unknown> {
    const deviceId = (args.device_id as string) || "esp32-sensor";

    switch (name) {
      case "query_telemetry":
        return this.tool_queryTelemetry(deviceId);
      case "check_alerts":
        return this.tool_checkAlerts(deviceId, (args.limit as number) || 5);
      case "toggle_led":
        return this.tool_toggleLed(deviceId, (args.state as string) || "off");
      case "get_history":
        return this.tool_getHistory(deviceId, (args.metric as string) || "ph", (args.limit as number) || 60);
      default:
        return { error: `Unknown tool: ${name}` };
    }
  }

  // ── Calibration State Machine ────────────────────────────────────────────

  private getActiveCalibration(deviceId: string): Record<string, unknown> | null {
    const cursor = this.ctx.storage.sql.exec(
      `SELECT * FROM calibration_sessions
       WHERE device_id = ? AND status NOT IN ('complete', 'cancelled')
       ORDER BY id DESC LIMIT 1`,
      deviceId,
    );
    const rows = [...cursor];
    if (rows.length === 0) return null;
    return rows[0] as unknown as Record<string, unknown>;
  }

  private async handleCalibration(msg: string, deviceId: string): Promise<string> {
    const session = this.getActiveCalibration(deviceId);

    if (!session) return this.startCalibration(msg, deviceId);

    const status = session.status as string;

    if (msg.includes("cancel") || msg.includes("stop") || msg.includes("abort")) {
      this.ctx.storage.sql.exec(
        `UPDATE calibration_sessions SET status = 'cancelled' WHERE device_id = ? AND status NOT IN ('complete', 'cancelled')`,
        deviceId,
      );
      return "Calibration cancelled. Your existing calibration values are unchanged. Say 'calibrate pH' whenever you're ready to try again.";
    }

    if (msg.includes("status") || msg.includes("where") || msg.includes("step")) {
      return this.calibrationStatus(session);
    }

    if (status === "awaiting_point1") return this.recordCalibrationPoint(deviceId, session, 1, msg);
    if (status === "awaiting_point2") return this.recordCalibrationPoint(deviceId, session, 2, msg);
    if (status === "computing") return this.finalizeCalibration(deviceId, session);

    return `Calibration is in progress (step: ${status}). Say "ready" when the probe is in the buffer solution, or "cancel" to stop.`;
  }

  private startCalibration(msg: string, deviceId: string): string {
    const probeType = msg.includes("ec") ? "ec" : msg.includes("tds") ? "tds" : "ph";
    if (probeType !== "ph") {
      return `I can calibrate pH probes. EC and TDS calibration uses a different process — typically a single standard solution. Let me know if you want to calibrate ${probeType} and I'll adapt the workflow. For now, I recommend calibrating your pH probe first since it's the most drift-sensitive.`;
    }

    this.ctx.storage.sql.exec(
      `INSERT INTO calibration_sessions (device_id, probe_type, status, created_at) VALUES (?, 'ph', 'awaiting_point1', ?)`,
      deviceId, Date.now(),
    );
    this.ctx.storage.sql.exec(
      `INSERT OR REPLACE INTO workflow_state (key, value) VALUES (?, ?)`,
      "active_workflow", JSON.stringify({ type: "calibration", probe: "ph", device_id: deviceId }),
    );

    return `Let's calibrate your pH probe. This is a 2-point calibration — it needs two buffer solutions to determine both the offset and slope of your probe.

**Step 1 of 2:** Rinse the probe with distilled water, then place it in **pH 7.0 buffer solution**. The probe needs about 30-60 seconds to stabilize. When it's stable, say **"ready"** and I'll record the reading.

(At any point, say "cancel" to abort — your existing calibration won't be changed.)`;
  }

  private async recordCalibrationPoint(
    deviceId: string, session: Record<string, unknown>, point: 1 | 2, msg: string,
  ): Promise<string> {
    if (!msg.includes("ready") && !msg.includes("go") && !msg.includes("ok") && !msg.includes("done") && !msg.includes("yes")) {
      const stepDesc = point === 1
        ? "Place the probe in pH 7.0 buffer solution, wait 30-60s for it to stabilize, then say **ready**."
        : "Rinse the probe with distilled water, place it in pH 4.0 buffer solution, wait 30-60s, then say **ready**.";
      return `I'm waiting for your confirmation. ${stepDesc}`;
    }

    const telemetry = await this.tool_queryTelemetry(deviceId);
    const t = telemetry as Record<string, unknown>;
    if (t.status !== "ok") {
      return "I can't read the sensor right now — the ESP32 may be offline. Let's wait and try again. Say **ready** when the device is back online.";
    }

    const knownValue = point === 1 ? 7.0 : 4.0;
    const measuredPh = t.ph as number;
    const col1 = point === 1 ? "point1_value" : "point2_value";
    const col2 = point === 1 ? "point1_mv" : "point2_mv";
    const nextStatus = point === 1 ? "awaiting_point2" : "computing";

    this.ctx.storage.sql.exec(
      `UPDATE calibration_sessions SET ${col1} = ?, ${col2} = ?, status = ? WHERE id = ?`,
      knownValue, measuredPh, nextStatus, session.id,
    );

    if (point === 1) {
      return `Recorded: your probe reads **pH ${measuredPh}** in pH 7.0 buffer. That's an offset of **${(measuredPh - 7.0).toFixed(2)}** pH units.

**Step 2 of 2:** Rinse the probe thoroughly with distilled water (cross-contamination will ruin the calibration). Now place it in **pH 4.0 buffer solution**. Wait 30-60 seconds for stabilization, then say **"ready"**.`;
    }

    return this.finalizeCalibration(deviceId, session);
  }

  private async finalizeCalibration(deviceId: string, session: Record<string, unknown>): Promise<string> {
    const ph1 = session.point1_value as number;
    const mv1 = session.point1_mv as number;
    const ph2 = session.point2_value as number;
    const mv2 = session.point2_mv as number;

    const idealSlope = 59.16;
    const deltaMv = mv1 - mv2;

    if (Math.abs(deltaMv) < 0.01) {
      this.ctx.storage.sql.exec(`UPDATE calibration_sessions SET status = 'cancelled' WHERE id = ?`, session.id);
      return "The two calibration points are nearly identical — the probe isn't responding to pH changes. Check that the probe is connected, the BNC connector is secure, and the buffer solutions are fresh. Calibration aborted.";
    }

    const slope = (ph1 - ph2) / deltaMv;
    const offset = ph1 - slope * mv1;
    const slopePct = (slope / idealSlope) * 100;

    this.ctx.storage.sql.exec(
      `UPDATE calibration_sessions SET slope = ?, offset = ?, slope_pct = ?, status = 'complete', completed_at = ? WHERE id = ?`,
      slope, offset, slopePct, Date.now(), session.id,
    );

    this.ctx.storage.sql.exec(
      `INSERT OR REPLACE INTO workflow_state (key, value) VALUES (?, ?)`,
      "active_workflow", JSON.stringify({ type: "idle" }),
    );

    try {
      const doId = this.env.DEVICE_HUB.idFromName(deviceId);
      const stub = this.env.DEVICE_HUB.get(doId);
      await stub.fetch(new Request("https://device-hub/relay-cmd", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          device_id: deviceId, command: "calibrate",
          params: { type: "ph", slope: Math.round(slope * 100) / 100, offset: Math.round(offset * 1000) / 1000, slope_pct: Math.round(slopePct * 10) / 10 },
        }),
      }));
    } catch (err) {
      console.log("[GreenyAgent] calibrate forward failed:", err);
    }

    let health = "";
    if (slopePct >= 90) health = "Excellent — your probe is in great condition.";
    else if (slopePct >= 80) health = "Good — your probe is aging normally.";
    else if (slopePct >= 70) health = "Fair — your probe is showing its age. Consider replacing in 30-60 days.";
    else health = "Poor — the slope is below 70% of ideal. Your probe needs replacement.";

    return `**Calibration complete!** Here's what we found:

- **Slope:** ${slope.toFixed(2)} mV/pH (${slopePct.toFixed(1)}% of ideal ${idealSlope} mV/pH)
- **Offset:** ${offset.toFixed(3)} pH units at pH 7.0

${health}

The new calibration has been sent to your ESP32. All future pH readings will use these values.`;
  }

  private calibrationStatus(session: Record<string, unknown>): string {
    const status = session.status as string;
    const probe = session.probe_type as string;
    if (status === "awaiting_point1") return `Calibration in progress for your ${probe.toUpperCase()} probe. **Step 1 of 2:** Place the probe in pH 7.0 buffer, wait for it to stabilize, then say **"ready"**. Say "cancel" to abort.`;
    if (status === "awaiting_point2") return `**Step 2 of 2:** Point 1 recorded (${session.point1_mv} in pH 7.0 buffer ✓). Rinse the probe, place it in **pH 4.0 buffer**, wait 30-60s, then say **"ready"**. Say "cancel" to abort.`;
    if (status === "computing") return "Both calibration points recorded. Computing slope and offset... say **done** to finalize.";
    return `Calibration status: ${status}. Say "cancel" to abort.`;
  }

  // ── Tool 1: query_telemetry (via DeviceHub DO — same colo, sub-ms) ────

  private async tool_queryTelemetry(deviceId: string) {
    try {
      const doId = this.env.DEVICE_HUB.idFromName(deviceId);
      const stub = this.env.DEVICE_HUB.get(doId);
      const resp = await stub.fetch(`https://device-hub/do-telemetry?device_id=${encodeURIComponent(deviceId)}`);
      return await resp.json();
    } catch (err) {
      return { device_id: deviceId, status: "error", message: String(err) };
    }
  }

  // ── Tool 2: check_alerts (via DeviceHub DO — same colo, sub-ms) ────────

  private async tool_checkAlerts(deviceId: string, limit: number) {
    try {
      const doId = this.env.DEVICE_HUB.idFromName(deviceId);
      const stub = this.env.DEVICE_HUB.get(doId);
      const resp = await stub.fetch(`https://device-hub/do-alerts?device_id=${encodeURIComponent(deviceId)}&limit=${limit}`);
      return await resp.json();
    } catch (err) {
      return { device_id: deviceId, status: "error", message: String(err) };
    }
  }

  // ── Tool 3: toggle_led (forward to DeviceHub DO) ───────────────────────

  private async tool_toggleLed(deviceId: string, state: string) {
    try {
      const ledState = state === "on";
      const doId = this.env.DEVICE_HUB.idFromName(deviceId);
      const stub = this.env.DEVICE_HUB.get(doId);
      const resp = await stub.fetch(new Request("https://device-hub/relay-cmd", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ device_id: deviceId, state: ledState }),
      }));
      const result = (await resp.json()) as Record<string, unknown>;
      return { ok: result.ok ?? true, device_id: deviceId, led: ledState };
    } catch (err) {
      return { ok: false, device_id: deviceId, error: String(err) };
    }
  }

  // ── Tool 4: get_history (D1 cold storage) ───────────────────────────────

  private async tool_getHistory(deviceId: string, metric: string, limit: number) {
    const validMetrics = new Set(["ph", "ec", "tds", "temp"]);
    if (!validMetrics.has(metric)) {
      return { device_id: deviceId, status: "error", message: `Invalid metric: ${metric}. Use one of: ph, ec, tds, temp.` };
    }

    try {
      const rows = await this.env.DB.prepare(
        `SELECT ${metric} as value, do_ms FROM telemetry WHERE device_id = ? AND ${metric} IS NOT NULL ORDER BY created_at DESC LIMIT ?`,
      ).bind(deviceId, limit).all();

      const data = rows.results.map((r: Record<string, unknown>) => ({ ts: r.do_ms as number, value: r.value as number })).reverse();
      return { device_id: deviceId, metric, data, count: data.length, status: "ok" };
    } catch (err) {
      return { device_id: deviceId, status: "error", message: String(err) };
    }
  }
}

// ── JSON Response Helper ───────────────────────────────────────────────────

function json(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" },
  });
}
