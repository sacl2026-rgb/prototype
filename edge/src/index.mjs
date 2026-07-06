import{DurableObject as O}from"cloudflare:workers";var E=class extends O{esp32ws=null;dashboards=new Map;ledState=!1;relay1State=!1;relay2State=!1;lastTelemetryMs=0;tds=0;ec=0;ph=7;temp=25;constructor(e,t){super(e,t),e.storage.sql.exec(`CREATE TABLE IF NOT EXISTS telemetry_buffer (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      device_id TEXT NOT NULL,
      tds REAL, ec REAL, ph REAL, temp REAL,
      led INTEGER DEFAULT 0,
      esp32_ms INTEGER, do_ms INTEGER,
      flushed INTEGER DEFAULT 0
    )`),e.storage.sql.exec(`CREATE TABLE IF NOT EXISTS relay_queue (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      device_id TEXT NOT NULL,
      command TEXT NOT NULL,
      params_json TEXT,
      created_at INTEGER
    )`),e.storage.sql.exec(`CREATE TABLE IF NOT EXISTS alert_buffer (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      device_id TEXT NOT NULL,
      alert_type TEXT NOT NULL,
      message TEXT NOT NULL,
      severity TEXT DEFAULT 'warning',
      created_at INTEGER,
      flushed INTEGER DEFAULT 0
    )`),e.storage.sql.exec(`CREATE TABLE IF NOT EXISTS relay_log_buffer (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      device_id TEXT NOT NULL,
      command TEXT NOT NULL,
      params_json TEXT,
      status TEXT DEFAULT 'sent',
      created_at INTEGER,
      flushed INTEGER DEFAULT 0
    )`),e.storage.sql.exec(`CREATE TABLE IF NOT EXISTS device_state (
      key TEXT PRIMARY KEY,
      value TEXT
    )`),e.getWebSockets().forEach(s=>{let a=s.deserializeAttachment();a?.role==="esp32"?this.esp32ws=s:a?.role==="dashboard"&&this.dashboards.set(s,a)}),console.log(`[DO] constructor \u2014 esp32:${this.esp32ws?"yes":"no"}, dashboards:${this.dashboards.size}`)}async fetch(e){let t=new URL(e.url);if(t.pathname==="/relay-cmd"&&e.method==="POST")try{let l=await e.json(),h=l.device_id||"esp32-sensor";if(l.command==="calibrate"&&l.params)return this.esp32ws?(this.esp32ws.send(JSON.stringify({command:"calibrate",params:{type:l.params.type||"ph",slope:l.params.slope,offset:l.params.offset,slope_pct:l.params.slope_pct}})),new Response(JSON.stringify({ok:!0,device_id:h,command:"calibrate"}),{headers:{"Content-Type":"application/json","Access-Control-Allow-Origin":"*"}})):new Response(JSON.stringify({ok:!1,device_id:h,error:"ESP32 not connected \u2014 calibrate is QoS 0, cannot queue"}),{status:503,headers:{"Content-Type":"application/json","Access-Control-Allow-Origin":"*"}});let o=typeof l.state=="boolean"?l.state:l.relay1!==void 0?l.relay1===1:null;if(o!==null){let d=JSON.stringify({state:o});return this.ctx.storage.sql.exec("INSERT INTO relay_queue (device_id, command, params_json, created_at) VALUES (?,?,?,?)",h,"set_led",d,Date.now()),this.ctx.storage.sql.exec("INSERT INTO relay_log_buffer (device_id, command, params_json, status, created_at) VALUES (?,?,?,'sent',?)",h,"set_led",d,Date.now()),this.esp32ws&&this.esp32ws.send(JSON.stringify({command:"set_led",params:{state:o}})),new Response(JSON.stringify({ok:!0,device_id:h,led:o}),{headers:{"Content-Type":"application/json","Access-Control-Allow-Origin":"*"}})}return new Response(JSON.stringify({error:"relay1, state, or command required"}),{status:400,headers:{"Content-Type":"application/json","Access-Control-Allow-Origin":"*"}})}catch{return new Response(JSON.stringify({error:"invalid json"}),{status:400,headers:{"Content-Type":"application/json","Access-Control-Allow-Origin":"*"}})}if(t.pathname==="/do-devices"&&e.method==="GET"){let l=this.esp32ws!==null;return new Response(JSON.stringify({devices:[{id:1,device_id:"esp32-sensor",name:"Test Sensor",type:"esp32",last_seen:Date.now(),status:l?"online":"offline"}]}),{headers:{"Content-Type":"application/json","Access-Control-Allow-Origin":"*"}})}if(t.pathname==="/do-telemetry"&&e.method==="GET")try{let l=t.searchParams.get("device_id")||"esp32-sensor",o=[...this.ctx.storage.sql.exec(`SELECT tds, ec, ph, temp, led, do_ms
           FROM telemetry_buffer WHERE device_id = ?
           ORDER BY id DESC LIMIT 1`,l)];if(o.length===0)return new Response(JSON.stringify({device_id:l,status:"no_data"}),{headers:{"Content-Type":"application/json","Access-Control-Allow-Origin":"*"}});let d=o[0];return new Response(JSON.stringify({device_id:l,tds:d.tds,ec:d.ec,ph:d.ph,temp:d.temp,led:d.led===1,do_ms:d.do_ms,status:"ok"}),{headers:{"Content-Type":"application/json","Access-Control-Allow-Origin":"*"}})}catch(l){return new Response(JSON.stringify({status:"error",message:String(l)}),{status:500,headers:{"Content-Type":"application/json","Access-Control-Allow-Origin":"*"}})}if(t.pathname==="/do-alerts"&&e.method==="GET")try{let l=t.searchParams.get("device_id")||"esp32-sensor",h=Math.min(parseInt(t.searchParams.get("limit")||"5"),50),d=[...this.ctx.storage.sql.exec(`SELECT alert_type, message, severity, created_at
           FROM alert_buffer WHERE device_id = ?
           ORDER BY id DESC LIMIT ?`,l,h)];return new Response(JSON.stringify({device_id:l,alerts:d.map(m=>({type:m.alert_type,message:m.message,severity:m.severity,created_at:m.created_at})),count:d.length,status:"ok"}),{headers:{"Content-Type":"application/json","Access-Control-Allow-Origin":"*"}})}catch(l){return new Response(JSON.stringify({status:"error",message:String(l)}),{status:500,headers:{"Content-Type":"application/json","Access-Control-Allow-Origin":"*"}})}let s=new WebSocketPair,[a,n]=Object.values(s);this.ctx.acceptWebSocket(n);let i=t.pathname.includes("dashboard")?"dashboard":"esp32",u=t.pathname.split("/").filter(Boolean),c=u.length>=2?u[1]:"unknown",p={role:i,deviceId:c,connectedAt:Date.now()};if(n.serializeAttachment(p),i==="esp32"){if(this.ctx.storage.sql.exec("DELETE FROM relay_queue WHERE device_id = ?",c),console.log(`[DO] purged relay_queue for ${c} on reconnect`),this.esp32ws)try{this.esp32ws.close(1e3,"Replaced by new connection")}catch{}this.esp32ws=n,this.lastTelemetryMs=0,console.log(`[DO] ESP32 connected: ${c}`),n.send(JSON.stringify({type:"sync",led:this.ledState,doTs:Date.now()})),this.broadcast({type:"device_status",device_id:c,status:"online"})}else this.dashboards.set(n,p),console.log(`[DO] Dashboard connected (${this.dashboards.size} total)`),n.send(JSON.stringify({type:"state",device_id:c,led:this.ledState,relay1:this.relay1State,relay2:this.relay2State,connected:this.esp32ws!==null,tds:this.tds,ec:this.ec,ph:this.ph,temp:this.temp,doTs:Date.now()}));return await this.ctx.storage.setAlarm(Date.now()+6e4),new Response(null,{status:101,webSocket:a})}async webSocketMessage(e,t){let s=e.deserializeAttachment();try{let a=JSON.parse(t);if(s?.role==="esp32")if(a.type==="telemetry")this.handleTelemetry(a);else if(a.type==="ack")this.handleAck(a);else if(a.type==="ping"){e.send(JSON.stringify({type:"pong",seq:a.seq,echo:`[DO] received ping seq=${a.seq}`}));let n=a.device_id||"esp32-sensor";this.drainRelayQueue(n)}else a.type==="wifi_list"?this.broadcast(a):a.type==="wifi_ack"&&this.broadcast(a);else if(s?.role==="dashboard")if(a.type==="relay"){let n=typeof a.relay1=="number"?a.relay1===1:!!a.state;this.handleDashboardCommand({command:"set_led",device_id:a.device_id,state:n},s)}else this.handleDashboardCommand(a,s)}catch(a){console.log("[DO] Invalid JSON from",s?.role,a)}}handleTelemetry(e){let t=e.device_id||"esp32-sensor",s=Date.now();this.lastTelemetryMs=Date.now(),this.tds=e.tds,this.ec=e.ec,this.ph=e.ph,this.temp=e.temp,typeof e.led=="boolean"&&(this.ledState=e.led),typeof e.relay_1=="boolean"&&(this.relay1State=e.relay_1),typeof e.relay_2=="boolean"&&(this.relay2State=e.relay_2),this.ctx.storage.sql.exec(`INSERT INTO telemetry_buffer
         (device_id, tds, ec, ph, temp, led, esp32_ms, do_ms)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,t,e.tds,e.ec,e.ph,e.temp,e.led?1:0,e.esp32_ms??0,s),this.ctx.storage.sql.exec("INSERT OR REPLACE INTO device_state (key, value) VALUES (?, ?)","ledState",this.ledState?"1":"0");let a=this.evaluateAlerts(this.ph,this.ec,this.temp);for(let n of a)[...this.ctx.storage.sql.exec("SELECT id FROM alert_buffer WHERE device_id = ? AND alert_type = ? AND flushed = 0 LIMIT 1",t,n.type)].length>0||(this.ctx.storage.sql.exec(`INSERT INTO alert_buffer (device_id, alert_type, message, severity, created_at)
         VALUES (?, ?, ?, ?, ?)`,t,n.type,n.message,n.severity,s),this.broadcast({type:"alert",device_id:t,alert_type:n.type,message:n.message,severity:n.severity,doTs:s}));this.drainRelayQueue(t),this.broadcast({type:"state",device_id:t,led:this.ledState,relay1:this.relay1State,relay2:this.relay2State,connected:!0,tds:this.tds,ec:this.ec,ph:this.ph,temp:this.temp,esp32_ms:e.esp32_ms??0,doTs:s}),this.broadcast({type:"telemetry_update",device_id:t,data:{ph:this.ph,tds:this.tds,ec:this.ec,water_temp:this.temp,relay1:this.ledState?1:0,relay2:0},ts_ms:s})}handleAck(e){typeof e.led=="boolean"&&(this.ledState=e.led),typeof e.state=="boolean"&&(e.command==="relay_1"&&(this.relay1State=e.state),e.command==="relay_2"&&(this.relay2State=e.state));let t=Date.now(),s=e.device_id||"esp32-sensor";this.ctx.storage.sql.exec("INSERT OR REPLACE INTO device_state (key, value) VALUES (?, ?)","ledState",this.ledState?"1":"0"),this.ctx.storage.sql.exec(`INSERT INTO relay_log_buffer (device_id, command, params_json, status, created_at)
       VALUES (?, ?, ?, 'acked', ?)`,s,e.command||"unknown",JSON.stringify({led:this.ledState,esp32_ms:e.esp32_ms}),t),console.log(`[DO] ESP32 ack: led=${this.ledState}, cmd=${e.command}, doTs=${t}`),this.broadcast({type:"state",device_id:s,led:this.ledState,relay1:this.relay1State,relay2:this.relay2State,connected:!0,tds:this.tds,ec:this.ec,ph:this.ph,temp:this.temp,doTs:t})}handleDashboardCommand(e,t){let s=e.device_id||t.deviceId||"esp32-sensor",a=Date.now();if(e.command==="set_led"){let n=JSON.stringify({state:e.state});this.ctx.storage.sql.exec(`INSERT INTO relay_queue (device_id, command, params_json, created_at)
         VALUES (?, ?, ?, ?)`,s,"set_led",n,a),this.ctx.storage.sql.exec(`INSERT INTO relay_log_buffer (device_id, command, params_json, status, created_at)
         VALUES (?, ?, ?, 'sent', ?)`,s,"set_led",n,a),this.esp32ws&&this.esp32ws.send(JSON.stringify({command:"set_led",params:{state:e.state}})),console.log(`[DO] queued set_led=${e.state} (esp32 connected: ${!!this.esp32ws})`)}else if(e.command==="calibrate"){let n=JSON.stringify(e.params||{});this.ctx.storage.sql.exec(`INSERT INTO relay_log_buffer (device_id, command, params_json, status, created_at)
         VALUES (?, ?, ?, ?, ?)`,s,"calibrate",n,this.esp32ws?"sent":"dropped",a),this.esp32ws&&this.esp32ws.send(JSON.stringify({command:"calibrate",params:e.params||{}})),console.log(`[DO] calibrate ${e.params?.type} \u2192 ${this.esp32ws?"forwarded":"dropped (ESP32 offline)"}`)}else e.command&&(this.esp32ws?(this.esp32ws.send(JSON.stringify(e)),console.log(`[DO] forwarded ${e.command} \u2192 ESP32`)):console.log(`[DO] dropped ${e.command} \u2014 ESP32 offline`))}evaluateAlerts(e,t,s){let a=[];return e<5.5?a.push({type:"ph_low",message:`pH \u8FC7\u4F4E: ${e}`,severity:"warning"}):e>8.5&&a.push({type:"ph_high",message:`pH \u8FC7\u9AD8: ${e}`,severity:"warning"}),t>2e3&&a.push({type:"ec_high",message:`EC \u8D85\u51FA\u9608\u503C: ${t} \u03BCS/cm`,severity:"warning"}),s<18?a.push({type:"temp_low",message:`\u6E29\u5EA6\u8FC7\u4F4E: ${s}\xB0C`,severity:"warning"}):s>30&&a.push({type:"temp_high",message:`\u6E29\u5EA6\u8FC7\u9AD8: ${s}\xB0C`,severity:"warning"}),a}drainRelayQueue(e){if(!this.esp32ws)return;let t=this.ctx.storage.sql.exec("SELECT id, command, params_json FROM relay_queue WHERE device_id = ? ORDER BY id ASC",e);for(let s of t){let a=s.params_json?JSON.parse(s.params_json):{};this.esp32ws.send(JSON.stringify({command:s.command,params:a})),this.ctx.storage.sql.exec("DELETE FROM relay_queue WHERE id = ?",s.id),console.log(`[DO] relay drain: ${s.command} \u2192 ESP32`)}}async alarm(){if(console.log("[DO] alarm() firing"),this.esp32ws&&this.lastTelemetryMs===0){console.log("[DO] health check FAILED \u2014 no telemetry since connect. Forcing reconnect.");try{this.esp32ws.close(1e3,"Health check failed \u2014 reconnect")}catch{}this.esp32ws=null,this.broadcast({type:"device_status",device_id:"esp32-sensor",status:"offline"})}try{let e=this.ctx.storage.sql.exec("SELECT id, device_id, tds, ec, ph, temp, led, esp32_ms, do_ms FROM telemetry_buffer WHERE flushed = 0");for(let n of e)try{await this.env.DB.prepare(`INSERT INTO telemetry (device_id, tds, ec, ph, temp, led, esp32_ms, do_ms)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8)`).bind(n.device_id,n.tds,n.ec,n.ph,n.temp,n.led,n.esp32_ms,n.do_ms).run()}catch(i){console.error("[alarm] telemetry insert failed for row",n.id,i)}this.ctx.storage.sql.exec("UPDATE telemetry_buffer SET flushed = 1 WHERE flushed = 0"),console.log("[alarm] flushed telemetry_buffer");let t=this.ctx.storage.sql.exec("SELECT id, device_id, alert_type, message, severity, created_at FROM alert_buffer WHERE flushed = 0");for(let n of t)try{await this.env.DB.prepare(`INSERT INTO alerts (device_id, alert_type, message, severity, created_at)
             VALUES (?1,?2,?3,?4,?5)`).bind(n.device_id,n.alert_type,n.message,n.severity,n.created_at).run()}catch(i){console.error("[alarm] alert insert failed for row",n.id,i)}this.ctx.storage.sql.exec("UPDATE alert_buffer SET flushed = 1 WHERE flushed = 0"),console.log("[alarm] flushed alert_buffer");let s=this.ctx.storage.sql.exec("SELECT id, device_id, command, params_json, status, created_at FROM relay_log_buffer WHERE flushed = 0");for(let n of s)try{await this.env.DB.prepare(`INSERT INTO relay_log (device_id, command, params_json, status, created_at)
             VALUES (?1,?2,?3,?4,?5)`).bind(n.device_id,n.command,n.params_json,n.status,n.created_at).run()}catch(i){console.error("[alarm] relay_log insert failed for row",n.id,i)}this.ctx.storage.sql.exec("UPDATE relay_log_buffer SET flushed = 1 WHERE flushed = 0"),console.log("[alarm] flushed relay_log_buffer");let a=this.ctx.storage.sql.exec(`SELECT DISTINCT device_id, MAX(do_ms) as last_seen
         FROM telemetry_buffer WHERE flushed = 1 GROUP BY device_id`);for(let n of a)try{await this.env.DB.prepare("UPDATE devices SET last_seen = ?, status = 'online' WHERE device_id = ?").bind(n.last_seen,n.device_id).run()}catch(i){console.error("[alarm] device update failed",n.device_id,i)}console.log("[alarm] updated device last_seen"),this.ctx.storage.sql.exec("DELETE FROM telemetry_buffer WHERE flushed = 1 AND id < (SELECT MAX(id) - 100 FROM telemetry_buffer)"),this.ctx.storage.sql.exec("DELETE FROM alert_buffer WHERE flushed = 1 AND id < (SELECT MAX(id) - 50 FROM alert_buffer)"),this.ctx.storage.sql.exec("DELETE FROM relay_log_buffer WHERE flushed = 1 AND id < (SELECT MAX(id) - 50 FROM relay_log_buffer)")}catch(e){console.error("[alarm] outer handler error:",e)}finally{await this.ctx.storage.setAlarm(Date.now()+6e4)}}async webSocketClose(e,t,s,a){let n=e.deserializeAttachment(),i=n?.deviceId||"esp32-sensor";if(n?.role==="esp32"){this.esp32ws=null,console.log("[DO] ESP32 disconnected");try{await this.env.DB.prepare("UPDATE devices SET status = 'offline' WHERE device_id = ?").bind(i).run()}catch(u){console.error("[DO] failed to update device offline status",u)}this.broadcast({type:"state",device_id:i,led:this.ledState,connected:!1,doTs:Date.now()}),this.broadcast({type:"device_status",device_id:i,status:"offline"})}else n?.role==="dashboard"&&(this.dashboards.delete(e),console.log(`[DO] Dashboard disconnected (${this.dashboards.size} remain)`));e.close(t,s)}async webSocketError(e,t){let s=e.deserializeAttachment();console.log(`[DO] WebSocket error on ${s?.role}:`,t),s?.role==="esp32"?(this.esp32ws=null,this.broadcast({type:"state",device_id:s.deviceId||"esp32-sensor",led:this.ledState,connected:!1,doTs:Date.now()})):s?.role==="dashboard"&&this.dashboards.delete(e)}broadcast(e){let t=JSON.stringify(e);this.dashboards.forEach((s,a)=>{try{a.send(t)}catch{this.dashboards.delete(a)}})}};import{DurableObject as N}from"cloudflare:workers";var I=`You are Greeny, a hydroponics AI assistant. You watch sensor data
from an ESP32 monitoring a hydroponic system. Your job is to translate
numbers into plant health. Normal is silence \u2014 only report deviations.

pH: 5.5-7.0 is optimal for most hydroponic crops. Below 5.0 or above
8.0 needs attention. If pH drifts slowly over days, probe needs
recalibration. If pH suddenly jumps to -10 or 34.95, probe is
disconnected \u2014 check BNC connector and amplifier board.

EC: 800-2000 \xB5S/cm is typical. EC=0 means sensor disconnected. EC
rising without nutrient change means temperature effect (2%/\xB0C is
normal physics). EC above 3000 needs dilution.

Temperature: 18-28\xB0C optimal. Below 15\xB0C roots slow. Above 30\xB0C
stresses plants, increases pathogen risk.

Alerts: If you see ph_high or ph_low alerts that persist across
multiple readings, the condition is real \u2014 don't dismiss it.
Check if the probe was recently calibrated. If it was 30+ days ago,
suggest recalibration.

Tone: Be warm, precise, plant-focused. Don't list raw JSON. Say
'Your basil is thriving \u2014 pH 6.2 and stable' not 'pH: 6.2, EC: 1200.'
When something is wrong, explain what, why, and what to do.`;var w=class extends N{constructor(e,t){super(e,t),this.env=t,e.storage.sql.exec(`CREATE TABLE IF NOT EXISTS calibration_sessions (
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
    )`),e.storage.sql.exec(`CREATE TABLE IF NOT EXISTS workflow_state (
      key TEXT PRIMARY KEY,
      value TEXT
    )`)}async fetch(e){let t=new URL(e.url),s=e.method;if(s==="OPTIONS")return new Response(null,{status:204,headers:{"Access-Control-Allow-Origin":"*","Access-Control-Allow-Methods":"POST, OPTIONS","Access-Control-Allow-Headers":"Content-Type, Authorization"}});if(t.pathname==="/api/chat"&&s==="POST")try{let a=await e.json();if(!a.message||typeof a.message!="string")return y({error:"message required (string)"},400);let n=await this.chat(a.message);return y({reply:n})}catch(a){let n=a instanceof Error?a.message:String(a);return console.error("[GreenyAgent] chat error:",n),y({error:"Internal error",detail:n},500)}return y({error:"Not found"},404)}async chat(e){let t=e.toLowerCase(),s="esp32-sensor",a=t.match(/turn\s+(on|off)\s+(the\s+)?(led|light)/i);if(a){let c=a[1];return(await this.tool_toggleLed(s,c)).ok?`The LED is now ${c}. ${c==="on"?"Your plants are getting extra light for photosynthesis.":"The light is off \u2014 your plants are in their dark cycle."}`:`I tried to turn the LED ${c}, but the device may be offline. The command is queued and will run when the ESP32 reconnects.`}if(t.match(/^(calibrate|start calibration|run calibration|begin calibration)/i)||t.match(/calibrate\s+(ph|ec|tds|the)/i))return this.handleCalibration(t,s);let i=["ready","done","ok","go","yes","cancel","stop","abort","status","step","where"];return this.getActiveCalibration(s)&&i.some(c=>t.includes(c))?this.handleCalibration(t,s):this.chatSimple(e)}async chatSimple(e){let t=e.toLowerCase(),s="esp32-sensor",a=[],n=t.includes("plant")||t.includes("how are")||t.includes("status")||t.includes("sensor")||t.includes("reading")||t.includes("current")||t.includes("now")||t.includes("ph")||t.includes("ec")||t.includes("tds")||t.includes("temp")||t.includes("led state")||t.includes("value"),i=t.includes("alert")||t.includes("warning")||t.includes("problem")||t.includes("issue")||t.includes("error")||t.includes("wrong")||t.includes("anything wrong"),u=t.includes("history")||t.includes("trend")||t.includes("chart")||t.includes("past")||t.includes("graph")||t.includes("over time")||t.includes("last")||t.includes("recent");if(n&&a.push(this.tool_queryTelemetry(s).then(o=>["telemetry",o])),i&&a.push(this.tool_checkAlerts(s,10).then(o=>["alerts",o])),u){let o=t.includes("ph")?"ph":t.includes("ec")?"ec":t.includes("tds")?"tds":t.includes("temp")?"temp":"ph";a.push(this.tool_getHistory(s,o,30).then(d=>["history",d]))}a.length===0&&a.push(this.tool_queryTelemetry(s).then(o=>["telemetry",o]),this.tool_checkAlerts(s,5).then(o=>["alerts",o]));let c=Object.fromEntries(await Promise.all(a)),p="";if(c.telemetry){let o=c.telemetry;o.status==="ok"?p+=`
Current sensor readings:
  pH: ${o.ph}  |  EC: ${o.ec} \xB5S/cm  |  TDS: ${o.tds} ppm  |  Temp: ${o.temp}\xB0C  |  LED: ${o.led?"ON":"OFF"}
`:p+=`
Sensor status: ${o.message||"No data available"}
`}if(c.alerts){let d=c.alerts.alerts;d&&d.length>0?p+=`
Recent alerts:
${d.map(m=>`  - [${m.severity}] ${m.type}: ${m.message}`).join(`
`)}
`:p+=`
Alerts: None \u2014 system is healthy.
`}if(c.history){let o=c.history,d=o.data;if(d&&d.length>0){let m=d.map(D=>D.value).join(", ");p+=`
Historical ${o.metric} trend (${d.length} points, oldest\u2192newest): ${m}
`}}let l=`[ROLE]
You are Greeny, a hydroponics AI assistant. You help users monitor their plants.

[KNOWLEDGE]
${I}

[REAL DATA \u2014 use ONLY these values, never make up readings]
${p}

[USER QUESTION]
${e}

[RULES]
- Use ONLY the sensor values provided above. Never make up numbers.
- Be warm, precise, and plant-focused.
- If data shows "No data available," tell the user the sensors may be offline.
- Keep your response to 3-5 sentences.
- If there are alerts, explain what they mean and what to do.
- Do NOT add notes, meta-commentary, or self-references about your response.

[RESPONSE]`;return((await this.env.AI.run("@cf/meta/llama-3.2-3b-instruct",{prompt:l,max_tokens:512})).response||"").trim()||"I couldn't process that request. Try asking about your plants or sensor readings."}async executeTool(e,t){let s=t.device_id||"esp32-sensor";switch(e){case"query_telemetry":return this.tool_queryTelemetry(s);case"check_alerts":return this.tool_checkAlerts(s,t.limit||5);case"toggle_led":return this.tool_toggleLed(s,t.state||"off");case"get_history":return this.tool_getHistory(s,t.metric||"ph",t.limit||60);default:return{error:`Unknown tool: ${e}`}}}getActiveCalibration(e){let s=[...this.ctx.storage.sql.exec(`SELECT * FROM calibration_sessions
       WHERE device_id = ? AND status NOT IN ('complete', 'cancelled')
       ORDER BY id DESC LIMIT 1`,e)];return s.length===0?null:s[0]}async handleCalibration(e,t){let s=this.getActiveCalibration(t);if(!s)return this.startCalibration(e,t);let a=s.status;return e.includes("cancel")||e.includes("stop")||e.includes("abort")?(this.ctx.storage.sql.exec("UPDATE calibration_sessions SET status = 'cancelled' WHERE device_id = ? AND status NOT IN ('complete', 'cancelled')",t),"Calibration cancelled. Your existing calibration values are unchanged. Say 'calibrate pH' whenever you're ready to try again."):e.includes("status")||e.includes("where")||e.includes("step")?this.calibrationStatus(s):a==="awaiting_point1"?this.recordCalibrationPoint(t,s,1,e):a==="awaiting_point2"?this.recordCalibrationPoint(t,s,2,e):a==="computing"?this.finalizeCalibration(t,s):`Calibration is in progress (step: ${a}). Say "ready" when the probe is in the buffer solution, or "cancel" to stop.`}startCalibration(e,t){let s=e.includes("ec")?"ec":e.includes("tds")?"tds":"ph";return s!=="ph"?`I can calibrate pH probes. EC and TDS calibration uses a different process \u2014 typically a single standard solution. Let me know if you want to calibrate ${s} and I'll adapt the workflow. For now, I recommend calibrating your pH probe first since it's the most drift-sensitive.`:(this.ctx.storage.sql.exec("INSERT INTO calibration_sessions (device_id, probe_type, status, created_at) VALUES (?, 'ph', 'awaiting_point1', ?)",t,Date.now()),this.ctx.storage.sql.exec("INSERT OR REPLACE INTO workflow_state (key, value) VALUES (?, ?)","active_workflow",JSON.stringify({type:"calibration",probe:"ph",device_id:t})),`Let's calibrate your pH probe. This is a 2-point calibration \u2014 it needs two buffer solutions to determine both the offset and slope of your probe.

**Step 1 of 2:** Rinse the probe with distilled water, then place it in **pH 7.0 buffer solution**. The probe needs about 30-60 seconds to stabilize. When it's stable, say **"ready"** and I'll record the reading.

(At any point, say "cancel" to abort \u2014 your existing calibration won't be changed.)`)}async recordCalibrationPoint(e,t,s,a){if(!a.includes("ready")&&!a.includes("go")&&!a.includes("ok")&&!a.includes("done")&&!a.includes("yes"))return`I'm waiting for your confirmation. ${s===1?"Place the probe in pH 7.0 buffer solution, wait 30-60s for it to stabilize, then say **ready**.":"Rinse the probe with distilled water, place it in pH 4.0 buffer solution, wait 30-60s, then say **ready**."}`;let i=await this.tool_queryTelemetry(e);if(i.status!=="ok")return"I can't read the sensor right now \u2014 the ESP32 may be offline. Let's wait and try again. Say **ready** when the device is back online.";let u=s===1?7:4,c=i.ph,p=s===1?"point1_value":"point2_value",l=s===1?"point1_mv":"point2_mv",h=s===1?"awaiting_point2":"computing";return this.ctx.storage.sql.exec(`UPDATE calibration_sessions SET ${p} = ?, ${l} = ?, status = ? WHERE id = ?`,u,c,h,t.id),s===1?`Recorded: your probe reads **pH ${c}** in pH 7.0 buffer. That's an offset of **${(c-7).toFixed(2)}** pH units.

**Step 2 of 2:** Rinse the probe thoroughly with distilled water (cross-contamination will ruin the calibration). Now place it in **pH 4.0 buffer solution**. Wait 30-60 seconds for stabilization, then say **"ready"**.`:this.finalizeCalibration(e,t)}async finalizeCalibration(e,t){let s=t.point1_value,a=t.point1_mv,n=t.point2_value,i=t.point2_mv,u=59.16,c=a-i;if(Math.abs(c)<.01)return this.ctx.storage.sql.exec("UPDATE calibration_sessions SET status = 'cancelled' WHERE id = ?",t.id),"The two calibration points are nearly identical \u2014 the probe isn't responding to pH changes. Check that the probe is connected, the BNC connector is secure, and the buffer solutions are fresh. Calibration aborted.";let p=(s-n)/c,l=s-p*a,h=p/u*100;this.ctx.storage.sql.exec("UPDATE calibration_sessions SET slope = ?, offset = ?, slope_pct = ?, status = 'complete', completed_at = ? WHERE id = ?",p,l,h,Date.now(),t.id),this.ctx.storage.sql.exec("INSERT OR REPLACE INTO workflow_state (key, value) VALUES (?, ?)","active_workflow",JSON.stringify({type:"idle"}));try{let d=this.env.DEVICE_HUB.idFromName(e);await this.env.DEVICE_HUB.get(d).fetch(new Request("https://device-hub/relay-cmd",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({device_id:e,command:"calibrate",params:{type:"ph",slope:Math.round(p*100)/100,offset:Math.round(l*1e3)/1e3,slope_pct:Math.round(h*10)/10}})}))}catch(d){console.log("[GreenyAgent] calibrate forward failed:",d)}let o="";return h>=90?o="Excellent \u2014 your probe is in great condition.":h>=80?o="Good \u2014 your probe is aging normally.":h>=70?o="Fair \u2014 your probe is showing its age. Consider replacing in 30-60 days.":o="Poor \u2014 the slope is below 70% of ideal. Your probe needs replacement.",`**Calibration complete!** Here's what we found:

- **Slope:** ${p.toFixed(2)} mV/pH (${h.toFixed(1)}% of ideal ${u} mV/pH)
- **Offset:** ${l.toFixed(3)} pH units at pH 7.0

${o}

The new calibration has been sent to your ESP32. All future pH readings will use these values.`}calibrationStatus(e){let t=e.status,s=e.probe_type;return t==="awaiting_point1"?`Calibration in progress for your ${s.toUpperCase()} probe. **Step 1 of 2:** Place the probe in pH 7.0 buffer, wait for it to stabilize, then say **"ready"**. Say "cancel" to abort.`:t==="awaiting_point2"?`**Step 2 of 2:** Point 1 recorded (${e.point1_mv} in pH 7.0 buffer \u2713). Rinse the probe, place it in **pH 4.0 buffer**, wait 30-60s, then say **"ready"**. Say "cancel" to abort.`:t==="computing"?"Both calibration points recorded. Computing slope and offset... say **done** to finalize.":`Calibration status: ${t}. Say "cancel" to abort.`}async tool_queryTelemetry(e){try{let t=this.env.DEVICE_HUB.idFromName(e);return await(await this.env.DEVICE_HUB.get(t).fetch(`https://device-hub/do-telemetry?device_id=${encodeURIComponent(e)}`)).json()}catch(t){return{device_id:e,status:"error",message:String(t)}}}async tool_checkAlerts(e,t){try{let s=this.env.DEVICE_HUB.idFromName(e);return await(await this.env.DEVICE_HUB.get(s).fetch(`https://device-hub/do-alerts?device_id=${encodeURIComponent(e)}&limit=${t}`)).json()}catch(s){return{device_id:e,status:"error",message:String(s)}}}async tool_toggleLed(e,t){try{let s=t==="on",a=this.env.DEVICE_HUB.idFromName(e);return{ok:(await(await this.env.DEVICE_HUB.get(a).fetch(new Request("https://device-hub/relay-cmd",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({device_id:e,state:s})}))).json()).ok??!0,device_id:e,led:s}}catch(s){return{ok:!1,device_id:e,error:String(s)}}}async tool_getHistory(e,t,s){if(!new Set(["ph","ec","tds","temp"]).has(t))return{device_id:e,status:"error",message:`Invalid metric: ${t}. Use one of: ph, ec, tds, temp.`};try{let i=(await this.env.DB.prepare(`SELECT ${t} as value, do_ms FROM telemetry WHERE device_id = ? AND ${t} IS NOT NULL ORDER BY created_at DESC LIMIT ?`).bind(e,s).all()).results.map(u=>({ts:u.do_ms,value:u.value})).reverse();return{device_id:e,metric:t,data:i,count:i.length,status:"ok"}}catch(n){return{device_id:e,status:"error",message:String(n)}}}};function y(r,e=200){return new Response(JSON.stringify(r),{status:e,headers:{"Content-Type":"application/json","Access-Control-Allow-Origin":"*"}})}function b(r){return btoa(String.fromCharCode(...new Uint8Array(r))).replace(/\+/g,"-").replace(/\//g,"_").replace(/=+$/,"")}function T(r){for(r=r.replace(/-/g,"+").replace(/_/g,"/");r.length%4;)r+="=";return atob(r)}var f=new TextEncoder,z=new TextDecoder;async function S(r,e,t=86400){let s={alg:"HS256",typ:"JWT"},a=Math.floor(Date.now()/1e3),n={...r,iat:a,exp:a+t},i=b(f.encode(JSON.stringify(s))),u=b(f.encode(JSON.stringify(n))),c=`${i}.${u}`,p=await crypto.subtle.importKey("raw",f.encode(e),{name:"HMAC",hash:"SHA-256"},!1,["sign"]),l=await crypto.subtle.sign("HMAC",p,f.encode(c)),h=b(l);return`${c}.${h}`}async function R(r,e){try{let t=r.split(".");if(t.length!==3)return null;let[s,a,n]=t,i=`${s}.${a}`,u=await crypto.subtle.importKey("raw",f.encode(e),{name:"HMAC",hash:"SHA-256"},!1,["verify"]),c=T(n),p=new Uint8Array(c.length);for(let d=0;d<c.length;d++)p[d]=c.charCodeAt(d);if(!await crypto.subtle.verify("HMAC",u,p.buffer,f.encode(i)))return null;let h=T(a),o=JSON.parse(h);return o.exp&&o.exp<Math.floor(Date.now()/1e3)?null:o}catch{return null}}async function L(r,e){let t=await crypto.subtle.importKey("raw",f.encode(r),{name:"PBKDF2"},!1,["deriveBits"]),s=await crypto.subtle.deriveBits({name:"PBKDF2",salt:f.encode(e),iterations:1e5,hash:"SHA-256"},t,256);return b(s)}async function x(r,e,t){return await L(r,e)===t}var v=null;async function C(r){if(v)return v;let e=await r.DB.prepare("SELECT value FROM settings WHERE key = 'jwt_secret'").first();return e&&(v=e.value),v??""}var k=new Set(["GET:/","GET:/dashboard","GET:/health","POST:/api/auth/login"]);function H(r,e){return e.startsWith("/device/")||e.startsWith("/dashboard/")?!1:!k.has(`${r}:${e}`)}async function P(r,e){let t=r.headers.get("Authorization");if(!t||!t.startsWith("Bearer "))return null;let s=t.slice(7),a=await C(e);return a?R(s,a):null}var U=`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Greeny \u2014 Smart Hydroponics</title>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    background: #0f172a; color: #e2e8f0;
    display: flex; justify-content: center; align-items: center;
    min-height: 100vh; padding: 16px;
  }
  .card {
    background: #1e293b; border-radius: 16px; padding: 28px;
    width: 100%; max-width: 440px;
    box-shadow: 0 25px 50px -12px rgba(0,0,0,0.5);
  }
  .title { font-size: 1.1rem; color: #94a3b8; margin-bottom: 2px; }
  .device { font-size: 1.4rem; font-weight: 700; margin-bottom: 12px; }

  /* Status dot + connection */
  .status-row { display: flex; align-items: center; gap: 8px; margin-bottom: 16px; font-size: 0.9rem; }
  .dot { width: 10px; height: 10px; border-radius: 50%; display: inline-block; transition: all 0.3s; }
  .dot-on    { background: #22c55e; box-shadow: 0 0 8px #22c55e; }
  .dot-stale { background: #f59e0b; box-shadow: 0 0 8px #f59e0b; animation: pulse 2s infinite; }
  .dot-off   { background: #ef4444; box-shadow: 0 0 8px #ef4444; }
  @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.4} }

  /* LED toggle switch (Fix 5) */
  .led-row { display: flex; align-items: center; gap: 14px; margin-bottom: 16px; }
  .led-circle {
    width: 56px; height: 56px; border-radius: 50%; border: 3px solid #334155;
    transition: all 0.3s ease; flex-shrink: 0;
  }
  .led-circle.on {
    background: #eab308; border-color: #facc15;
    box-shadow: 0 0 30px rgba(234,179,8,0.6), 0 0 60px rgba(234,179,8,0.3);
  }
  .led-circle.off { background: #1e293b; border-color: #334155; box-shadow: none; }
  .toggle { position: relative; display: inline-block; width: 48px; height: 26px; flex-shrink: 0; }
  .toggle input { opacity: 0; width: 0; height: 0; }
  .toggle-slider { position: absolute; cursor: pointer; top:0;left:0;right:0;bottom:0; background:#334155; border-radius:26px; transition:0.3s; }
  .toggle-slider:before { position:absolute; content:""; height:18px; width:18px; left:4px; bottom:4px; background:white; border-radius:50%; transition:0.3s; }
  input:checked + .toggle-slider { background: #eab308; }
  input:checked + .toggle-slider:before { transform: translateX(22px); }
  .led-label { font-size: 1rem; color: #94a3b8; }

  /* RTT + last updated */
  .meta-row { display: flex; justify-content: space-between; font-size: 0.8rem; color: #64748b; margin-bottom: 14px; }
  .meta-row span { color: #22d3ee; font-weight: 600; }

  /* Sensor gauge arcs (Fix 10) */
  .gauges { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; margin-bottom: 14px; }
  .gauge { position: relative; width: 100%; aspect-ratio: 1; max-width: 140px; margin: 0 auto; }
  .gauge-arc {
    width: 100%; height: 100%; border-radius: 50%;
    position: relative; overflow: hidden;
  }
  .gauge-bg {
    position: absolute; inset: 0; border-radius: 50%;
    background: conic-gradient(
      #22c55e 0deg var(--gauge-pct, 0deg),
      #334155 var(--gauge-pct, 0deg) 360deg
    );
  }
  .gauge-center {
    position: absolute; top: 50%; left: 50%; transform: translate(-50%,-50%);
    width: 70%; height: 70%; border-radius: 50%; background: #0f172a;
    display: flex; flex-direction: column; align-items: center; justify-content: center;
  }
  .gauge-value { font-size: 1.2rem; font-weight: 700; line-height: 1; }
  .gauge-value.sensor-normal  { color: #22c55e; }
  .gauge-value.sensor-warning { color: #f59e0b; }
  .gauge-value.sensor-danger  { color: #ef4444; }
  .gauge-unit  { font-size: 0.65rem; color: #64748b; margin-top: 2px; }
  .gauge-label { text-align: center; font-size: 0.68rem; color: #64748b; text-transform: uppercase; letter-spacing: 0.5px; margin-top: 4px; }

  /* Device info (Fix 9) */
  .device-info { font-size: 0.72rem; color: #475569; text-align: center; margin-bottom: 12px; min-height: 16px; }

  /* Toast container (Fix 7) */
  #toastContainer { position: fixed; top: 16px; right: 16px; z-index: 1000; display: flex; flex-direction: column; gap: 8px; max-width: 340px; }
  .toast { padding: 10px 14px; border-radius: 8px; color: #fff; font-size: 0.82rem; animation: slideIn 0.3s ease; display: flex; justify-content: space-between; align-items: flex-start; gap: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.4); }
  .toast-warning  { background: #b45309; }
  .toast-critical { background: #dc2626; }
  .toast-msg { flex: 1; }
  .toast-close { cursor: pointer; font-size: 1rem; opacity: 0.7; line-height: 1; }
  .toast-close:hover { opacity: 1; }
  @keyframes slideIn  { from { transform: translateX(120%); opacity: 0; } to { transform: translateX(0); opacity: 1; } }
  @keyframes slideOut { from { transform: translateX(0); opacity: 1; } to { transform: translateX(120%); opacity: 0; } }

  /* Login */
  .login-box { text-align: center; }
  .login-box input {
    width: 100%; padding: 12px; margin-bottom: 10px; border-radius: 8px;
    border: 1px solid #334155; background: #0f172a; color: #e2e8f0; font-size: 1rem;
  }
  .login-box input:focus { outline: none; border-color: #3b82f6; }
  .login-box button {
    width: 100%; padding: 12px; border-radius: 8px; border: none;
    font-size: 1rem; font-weight: 600; cursor: pointer; background: #3b82f6; color: #fff;
  }
  .login-box button:hover { background: #2563eb; }
  .login-error { color: #fca5a5; font-size: 0.85rem; margin-top: 8px; min-height: 20px; }
  .logout-link { font-size: 0.8rem; color: #64748b; cursor: pointer; text-align: right; margin-bottom: 8px; }
  .logout-link:hover { color: #ef4444; }

  /* Device selector */
  .device-select {
    width: 100%; padding: 8px 12px; margin-bottom: 12px; border-radius: 8px;
    border: 1px solid #334155; background: #0f172a; color: #e2e8f0; font-size: 0.95rem;
  }

  /* Log */
  .log {
    margin-top: 14px; padding: 10px; background: #0f172a; border-radius: 8px;
    font-family: 'Courier New', monospace; font-size: 0.72rem; color: #64748b;
    max-height: 140px; overflow-y: auto;
  }
  .log .line { padding: 2px 0; border-bottom: 1px solid #1e293b; }
  .log .line:last-child { border-bottom: none; }
</style>
</head>
<body>

<!-- Toast container -->
<div id="toastContainer"></div>

<!-- Login Panel -->
<div class="card login-box" id="loginBox">
  <div class="title">Greeny IoT Hub</div>
  <div class="device" style="margin-bottom:24px">Login</div>
  <input id="username" type="text" placeholder="Username" autocomplete="username" />
  <input id="password" type="password" placeholder="Password" autocomplete="current-password" />
  <button id="btnLogin">Sign In</button>
  <div class="login-error" id="loginError"></div>
</div>

<!-- Dashboard Panel -->
<div class="card" id="dashboardBox" style="display:none">
  <div class="logout-link" id="btnLogout">Logout</div>
  <div class="title">Device Dashboard</div>
  <select class="device-select" id="deviceSelect">
    <option value="esp32-sensor">esp32-sensor</option>
    <option value="esp32-led">esp32-led</option>
  </select>
  <div class="device" id="deviceId">esp32-sensor</div>

  <div class="device-info" id="deviceInfo"></div>

  <div class="status-row">
    <span class="dot dot-off" id="statusDot"></span>
    <span id="statusText">Connecting\u2026</span>
  </div>

  <div class="led-row">
    <div class="led-circle off" id="ledCircle"></div>
    <span class="led-label">LED</span>
    <label class="toggle">
      <input type="checkbox" id="ledToggle" disabled />
      <span class="toggle-slider"></span>
    </label>
  </div>

  <div class="meta-row">
    <span>RTT: <b id="rttVal">\u2014</b></span>
    <span id="lastUpdate" style="color:#64748b">\u2014</span>
  </div>

  <div class="gauges">
    <div>
      <div class="gauge"><div class="gauge-arc"><div class="gauge-bg" style="--gauge-pct:0deg" id="tdsArc"></div><div class="gauge-center"><div class="gauge-value" id="tdsVal">\u2014</div><div class="gauge-unit">ppm</div></div></div></div>
      <div class="gauge-label">TDS</div>
    </div>
    <div>
      <div class="gauge"><div class="gauge-arc"><div class="gauge-bg" style="--gauge-pct:0deg" id="ecArc"></div><div class="gauge-center"><div class="gauge-value" id="ecVal">\u2014</div><div class="gauge-unit">\u03BCS/cm</div></div></div></div>
      <div class="gauge-label">EC</div>
    </div>
    <div>
      <div class="gauge"><div class="gauge-arc"><div class="gauge-bg" style="--gauge-pct:0deg" id="phArc"></div><div class="gauge-center"><div class="gauge-value" id="phVal">\u2014</div><div class="gauge-unit">pH</div></div></div></div>
      <div class="gauge-label">pH</div>
    </div>
    <div>
      <div class="gauge"><div class="gauge-arc"><div class="gauge-bg" style="--gauge-pct:0deg" id="tempArc"></div><div class="gauge-center"><div class="gauge-value" id="tempVal">\u2014</div><div class="gauge-unit">\xB0C</div></div></div></div>
      <div class="gauge-label">Temp</div>
    </div>
  </div>

  <div class="log" id="logBox"></div>
</div>

<script>
// \u2500\u2500 Globals \u2500\u2500
const $ = (id) => document.getElementById(id);
let ws, currentDevice = "esp32-sensor", JWT = sessionStorage.getItem("jwt") || "";
let lastUpdateTs = 0, connected = false, updateTimer = 0;

// \u2500\u2500 Log \u2500\u2500
function log(msg) {
  const box = $("logBox"); if (!box) return;
  const now = new Date().toLocaleTimeString();
  box.innerHTML += '<div class="line">' + now + " " + msg + "</div>";
  box.scrollTop = box.scrollHeight;
}

// \u2500\u2500 Sensor color thresholds (Fix 4) \u2500\u2500
function sensorClass(value, ranges) {
  if (!value && value !== 0) return "";
  const [loN,hiN,loW,hiW] = ranges;
  if (value >= loN && value <= hiN) return "sensor-normal";
  if (value >= loW && value <= hiW) return "sensor-warning";
  return "sensor-danger";
}

function updateSensor(elId, arcId, value, ranges, unit) {
  const elV = $(elId); if (!elV) return;
  elV.textContent = value != null ? value : "\u2014";
  elV.className = "gauge-value " + sensorClass(value, ranges);
  // Update conic-gradient arc
  const arc = $(arcId); if (!arc || value == null) return;
  const maxVal = ranges[3]; // top of warning = full scale
  const pct = Math.min((value / maxVal) * 360, 360);
  arc.style.setProperty("--gauge-pct", pct + "deg");
}

function updateLastUpdate() {
  const el = $("lastUpdate"); if (!el) return;
  if (!connected) { el.textContent = "\u2014"; el.style.color = "#64748b"; return; }
  const s = Math.floor((Date.now() - lastUpdateTs) / 1000);
  el.textContent = "Updated " + s + "s ago";
  el.style.color = s > 30 ? "#ef4444" : s > 15 ? "#f59e0b" : "#22c55e";
}

// \u2500\u2500 Connection state (Fix 8: amber stale) \u2500\u2500
function setConnectionState(c, hasRecentData) {
  connected = c;
  const dot = $("statusDot"), txt = $("statusText"), tog = $("ledToggle");
  if (c) {
    if (!hasRecentData) {
      dot.className = "dot dot-on";
      txt.textContent = "Connected";
    } else {
      dot.className = "dot dot-stale";
      txt.textContent = "Connected (stale)";
    }
  } else {
    dot.className = "dot dot-off";
    txt.textContent = "Disconnected";
  }
  if (tog) tog.disabled = !c;
}

// \u2500\u2500 Toast alerts (Fix 7) \u2500\u2500
function showToast(type, message, severity) {
  const container = $("toastContainer"); if (!container) return;
  const toast = document.createElement("div");
  toast.className = "toast " + (severity === "critical" ? "toast-critical" : "toast-warning");
  toast.innerHTML = '<span class="toast-msg">' + message + '</span><span class="toast-close">\u2715</span>';
  toast.querySelector(".toast-close").addEventListener("click", () => {
    toast.style.animation = "slideOut 0.3s ease";
    setTimeout(() => toast.remove(), 300);
  });
  container.appendChild(toast);
  setTimeout(() => {
    if (toast.parentElement) {
      toast.style.animation = "slideOut 0.3s ease";
      setTimeout(() => { if (toast.parentElement) toast.remove(); }, 300);
    }
  }, 10000);
}

// \u2500\u2500 Commands \u2500\u2500
function sendCommand(state) {
  if (!ws || ws.readyState !== WebSocket.OPEN) return;
  ws.send(JSON.stringify({ command: "set_led", state, device_id: currentDevice, ts: Date.now() }));
  log(state ? "ON \u2192 sent" : "OFF \u2192 sent");
}

// \u2500\u2500 WebSocket \u2500\u2500
function connectWS() {
  if (!JWT) { showLogin(); return; }
  const proto = location.protocol === "https:" ? "wss://" : "ws://";
  const WS_URL = proto + location.host + "/dashboard/" + currentDevice + "?token=" + JWT;
  log("Connecting to " + currentDevice + " \u2026");
  ws = new WebSocket(WS_URL);

  ws.onopen = () => { log("WSS open"); };

  ws.onmessage = (e) => {
    let msg;
    try { msg = JSON.parse(e.data); } catch (err) { return; }

    if (msg.type === "state") {
      if (msg.connected !== undefined) setConnectionState(msg.connected, false);
      if (typeof msg.led === "boolean") {
        $("ledCircle").className = "led-circle " + (msg.led ? "on" : "off");
        const tog = $("ledToggle");
        if (tog) { tog.checked = msg.led; tog.disabled = !connected; }
      }

      // Update gauges with color thresholds + arcs (Fixes 4 + 10)
      updateSensor("tdsVal",  "tdsArc",  msg.tds,  [0,750,750,1500],  "ppm");
      updateSensor("ecVal",   "ecArc",   msg.ec,   [0,1500,1500,3000], "\u03BCS/cm");
      updateSensor("phVal",   "phArc",   msg.ph,   [6.0,7.0,5.5,8.5],  "pH");
      updateSensor("tempVal", "tempArc", msg.temp, [20,28,18,30],       "\xB0C");

      if (msg.doTs) { const el = $("rttVal"); if (el) el.textContent = (Date.now() - msg.doTs) + "ms"; }

      // Device info (Fix 9)
      if (msg.esp32_ms !== undefined) {
        const el = $("deviceInfo");
        if (el) el.textContent = "ESP32 uptime: " + Math.floor(msg.esp32_ms / 1000) + "s \xB7 " + (msg.device_id || currentDevice);
      }

      lastUpdateTs = Date.now();
      updateLastUpdate();
      setConnectionState(true, true);
    }

    if (msg.type === "alert") {
      showToast(msg.alert_type, msg.message, msg.severity);
      log("[ALERT] " + msg.alert_type + ": " + msg.message);
    }
  };

  ws.onclose = () => {
    setConnectionState(false, false);
    log("WSS closed \u2014 reconnecting in 3s\u2026");
    setTimeout(connectWS, 3000);
  };

  ws.onerror = () => { log("WSS error"); };
}

// \u2500\u2500 Auth \u2500\u2500
function showLogin() {
  $("loginBox").style.display = "block";
  $("dashboardBox").style.display = "none";
  if (ws) { try { ws.close(); } catch(e) {} ws = null; }
  JWT = ""; sessionStorage.removeItem("jwt");
  connected = false;
  if (updateTimer) { clearInterval(updateTimer); updateTimer = 0; }
}

function showDashboard() {
  $("loginBox").style.display = "none";
  $("dashboardBox").style.display = "block";
  $("deviceId").textContent = currentDevice;
  updateTimer = setInterval(updateLastUpdate, 1000);
  connectWS();
}

async function doLogin() {
  const username = $("username").value.trim();
  const password = $("password").value;
  const errEl = $("loginError");
  if (!username || !password) { errEl.textContent = "Enter username and password"; return; }
  errEl.textContent = "";
  try {
    const resp = await fetch("/api/auth/login", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ username, password }),
    });
    const data = await resp.json();
    if (!resp.ok || !data.token) { errEl.textContent = data.error || "Login failed"; return; }
    JWT = data.token;
    sessionStorage.setItem("jwt", JWT);
    log("Logged in as " + data.user.username);
    showDashboard();
  } catch (err) { errEl.textContent = "Network error \u2014 try again"; }
}

function doLogout() { showLogin(); log("Logged out"); }

// \u2500\u2500 Device Switching \u2500\u2500
function switchDevice(id) {
  currentDevice = id; $("deviceId").textContent = id;
  if (ws) { try { ws.close(); } catch(e) {} ws = null; }
  connectWS(); log("Switched to " + id);
}

// \u2500\u2500 Init \u2500\u2500
$("btnLogin").addEventListener("click", doLogin);
$("password").addEventListener("keydown", (e) => { if (e.key === "Enter") doLogin(); });
$("btnLogout").addEventListener("click", doLogout);
$("ledToggle").addEventListener("change", () => sendCommand($("ledToggle").checked));
$("deviceSelect").addEventListener("change", (e) => switchDevice(e.target.value));

if (JWT) { showDashboard(); } else { showLogin(); }
<\/script>
</body>
</html>`,_={"Access-Control-Allow-Origin":"*","Access-Control-Allow-Methods":"GET, POST, OPTIONS","Access-Control-Allow-Headers":"Content-Type, Authorization"};function A(r,e){let t=new Headers(e?.headers);for(let[s,a]of Object.entries(_))t.set(s,a);return new Response(r,{...e,headers:t})}function g(r,e){let t=new Headers(e?.headers);t.set("Content-Type","application/json");for(let[s,a]of Object.entries(_))t.set(s,a);return new Response(JSON.stringify(r),{...e,headers:t})}var ee={async fetch(r,e){let t=new URL(r.url),s=t.pathname,a=r.method;if(a==="OPTIONS")return new Response(null,{status:204,headers:_});if(a==="GET"&&(s==="/"||s==="/dashboard"))return A(U,{headers:{"Content-Type":"text/html; charset=utf-8"}});if(a==="GET"&&s==="/health")return g({status:"ok",uptime:Math.floor(performance.now()/1e3)});if(a==="POST"&&s==="/api/auth/login")return $(r,e);if(H(a,s)){let i=await P(r,e);if(!i)return g({error:"Unauthorized"},{status:401});if(a==="GET"&&s==="/api/auth/me")return g({user:i});if(a==="GET"&&s==="/api/telemetry")return B(t,e);if(a==="GET"&&s==="/api/devices")return M(e);if(a==="GET"&&s==="/api/alerts")return W(t,e);if(a==="POST"&&s==="/api/alerts/ack")return j(r,e);if(a==="POST"&&s==="/api/relay")return F(r,e);if(a==="POST"&&s==="/api/chat")return q(r,e)}if(r.headers.get("Upgrade")==="websocket"){let i=s.split("/").filter(Boolean),u=i.length>=2?i[1]:"unknown",c=e.DEVICE_HUB.idFromName(u);return e.DEVICE_HUB.get(c).fetch(r)}return A("Not found",{status:404})}};async function $(r,e){try{let t=await r.json();if(!t.username||!t.password)return g({error:"username and password required"},{status:400});let s=await e.DB.prepare("SELECT id, username, password_hash, salt, role FROM users WHERE username = ?").bind(t.username).first();if(!s)return g({error:"Invalid credentials"},{status:401});if(!await x(t.password,s.salt,s.password_hash))return g({error:"Invalid credentials"},{status:401});let n=await C(e),i=await S({sub:s.username,id:s.id,role:s.role,iat:Math.floor(Date.now()/1e3)},n);return g({token:i,user:{id:s.id,username:s.username,role:s.role}})}catch{return g({error:"Bad request"},{status:400})}}async function B(r,e){let t=r.searchParams.get("device_id"),s=Math.min(parseInt(r.searchParams.get("limit")||"100"),1e3),a;return t?a=await e.DB.prepare("SELECT * FROM telemetry WHERE device_id = ? ORDER BY created_at DESC LIMIT ?").bind(t,s).all():a=await e.DB.prepare("SELECT * FROM telemetry ORDER BY created_at DESC LIMIT ?").bind(s).all(),g({telemetry:a.results})}async function M(r){let e=r.DEVICE_HUB.idFromName("esp32-sensor"),s=await r.DEVICE_HUB.get(e).fetch(new Request("https://do/do-devices"));if(s.ok)return s;let a=await r.DB.prepare("SELECT * FROM devices ORDER BY id ASC").all();return g({devices:a.results})}async function W(r,e){let t=r.searchParams.get("device_id"),s=Math.min(parseInt(r.searchParams.get("limit")||"50"),500),a;return t?a=await e.DB.prepare("SELECT * FROM alerts WHERE device_id = ? ORDER BY created_at DESC LIMIT ?").bind(t,s).all():a=await e.DB.prepare("SELECT * FROM alerts ORDER BY created_at DESC LIMIT ?").bind(s).all(),g({alerts:a.results})}async function j(r,e){try{let t=await r.json();return t.alert_id?(await e.DB.prepare("UPDATE alerts SET acknowledged = 1 WHERE id = ?").bind(t.alert_id).run(),g({success:!0})):g({error:"alert_id required"},{status:400})}catch{return g({error:"Bad request"},{status:400})}}async function q(r,e){try{let t=e.GREENY_AGENT.idFromName("greeny");return e.GREENY_AGENT.get(t).fetch(r)}catch(t){return console.error("[Worker] handleChat error:",t),g({error:"Agent unavailable"},{status:503})}}async function F(r,e){try{let t=await r.json();if(!t.device_id)return g({error:"device_id required"},{status:400});let s=e.DEVICE_HUB.idFromName(t.device_id);return e.DEVICE_HUB.get(s).fetch(new Request("https://device-hub/relay-cmd",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(t)}))}catch{return g({error:"Bad request"},{status:400})}}export{E as DeviceHub,w as GreenyAgent,ee as default};
