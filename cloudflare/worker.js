const ROOM_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
const ROOM_CODE_LENGTH = 8;
const MAX_PLAYERS = 16;
const ROOM_TTL_MS = 24 * 60 * 60 * 1000;
const MAX_WS_MESSAGE = 24_000;
const MAX_PACK_BYTES = 45_000;
const ALLOWED_ORIGINS = new Set([
  "https://netomur.github.io",
  "http://localhost:8000",
  "http://127.0.0.1:5500"
]);
const JOIN_ATTEMPTS = new Map();

async function readJsonBody(request) {
  const length = Number(request.headers.get("Content-Length") || 0);
  if (length > MAX_PACK_BYTES + 8_000) return { error: "Запрос слишком большой.", status: 413 };
  const raw = await request.text();
  if (raw.length > MAX_PACK_BYTES + 8_000) return { error: "Запрос слишком большой.", status: 413 };
  try {
    const value = JSON.parse(raw);
    if (!isObject(value)) return { error: "Некорректный запрос.", status: 400 };
    return { value };
  } catch {
    return { error: "Не удалось прочитать запрос.", status: 400 };
  }
}

function allowJoinAttempt(ip) {
  const now = Date.now();
  const key = String(ip || "unknown").slice(0, 64);
  const entries = JOIN_ATTEMPTS.get(key) || [];
  const recent = entries.filter(time => now - time < 60_000);
  if (recent.length >= 15) return false;
  recent.push(now);
  if (JOIN_ATTEMPTS.size > 2_000) JOIN_ATTEMPTS.clear();
  JOIN_ATTEMPTS.set(key, recent);
  return true;
}

const CHARACTER_KEYS = [
  "gender", "race", "body", "trait", "job", "health",
  "hobby", "fear", "inv1", "inv2", "extra", "spec"
];
const PARAM_KEYS = ["gender", "body", "job", "hobby"];
const GAME_PATCH_KEYS = new Set([
  "players", "ban", "gm", "gmPlays", "surv", "status", "chars",
  "out", "shown", "votes", "vdone", "phase", "n", "ans"
]);
const FORBIDDEN_KEYS = new Set(["__proto__", "prototype", "constructor"]);

const json = (data, status = 200, extraHeaders = {}) => new Response(
  JSON.stringify(data),
  {
    status,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "Cache-Control": "no-store",
      ...extraHeaders
    }
  }
);

function corsHeaders(origin) {
  const headers = {
    "Vary": "Origin",
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Max-Age": "86400"
  };
  if (origin && ALLOWED_ORIGINS.has(origin)) {
    headers["Access-Control-Allow-Origin"] = origin;
  }
  return headers;
}

function randomBytes(size = 32) {
  const bytes = new Uint8Array(size);
  crypto.getRandomValues(bytes);
  return bytes;
}

function randomToken() {
  const bytes = randomBytes(32);
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

function randomCode() {
  const bytes = randomBytes(ROOM_CODE_LENGTH);
  let code = "";
  for (const byte of bytes) code += ROOM_ALPHABET[byte & 31];
  return code;
}

function randomUid() {
  const bytes = randomBytes(9);
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return "u" + btoa(binary).replace(/\+/g, "").replace(/\//g, "").replace(/=+$/g, "").toLowerCase();
}

async function hashToken(token) {
  const data = new TextEncoder().encode(token);
  const digest = await crypto.subtle.digest("SHA-256", data);
  return [...new Uint8Array(digest)].map(x => x.toString(16).padStart(2, "0")).join("");
}

function validUid(uid) {
  return typeof uid === "string" && /^u[a-z0-9_-]{6,40}$/i.test(uid);
}

function cleanText(value, max = 240) {
  if (typeof value !== "string") return "";
  return value.replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F]/g, "").trim().slice(0, max);
}

function cleanAvatar(value) {
  if (typeof value !== "string" || value.length > 600) return "";
  try {
    const u = new URL(value);
    if (u.protocol !== "https:" || u.hostname !== "cdn.discordapp.com") return "";
    return u.href;
  } catch {
    return "";
  }
}

function cleanList(value, maxItems = 120, maxLength = 180) {
  if (!Array.isArray(value)) return [];
  return value.slice(0, maxItems).map(x => cleanText(x, maxLength)).filter(Boolean);
}

function sanitizeSet(input) {
  if (!input || typeof input !== "object" || Array.isArray(input)) {
    throw new Error("Не передан набор карточек.");
  }
  const name = cleanText(input.name, 48) || "Без названия";
  const story = cleanList(input.story, 20, 600);
  if (!story.length) throw new Error("В наборе должен быть хотя бы один сюжет.");

  const lists = {};
  for (const key of CHARACTER_KEYS) {
    lists[key] = cleanList(input.lists?.[key], 120, 180);
  }

  const sx = {};
  for (const key of PARAM_KEYS) {
    const x = input.sx?.[key];
    if (!x || typeof x !== "object") continue;
    const item = {
      list: cleanList(x.list, 50, 100),
      e: Math.max(0, Math.min(100, Number(x.e) || 0))
    };
    if (key === "gender" || key === "body") {
      const lo = x.lo === null || x.lo === "" ? null : Number(x.lo);
      const hi = x.hi === null || x.hi === "" ? null : Number(x.hi);
      if (Number.isFinite(lo) && Number.isFinite(hi) && hi >= lo) {
        item.lo = lo;
        item.hi = hi;
        item.p = Math.max(0, Math.min(100, Number(x.p ?? 50) || 0));
      } else {
        item.lo = null;
        item.hi = null;
      }
    }
    sx[key] = item;
  }

  const questions = Array.isArray(input.q) ? input.q.slice(0, 60) : [];
  const q = questions.map(item => ({
    t: cleanText(item?.t, 300),
    need: item?.need === "n" ? "n" : "y",
    priority: Math.max(1, Math.min(10, Math.round(Number(item?.priority) || 1)))
  })).filter(item => item.t);

  return {
    id: cleanText(input.id, 48),
    bid: cleanText(input.bid, 48),
    name,
    story,
    lists,
    sx,
    bunkerSize: cleanList(input.bunkerSize, 16, 180),
    rooms: cleanList(input.rooms, 16, 300),
    q
  };
}

function parseExtra(value) {
  const m = String(value ?? "").match(/^([\s\S]*?)\s*\[([\s\S]*)\]\s*$/);
  return m ? { v: m[1].trim(), h: m[2].trim() } : { v: String(value ?? "").trim(), h: "" };
}

function shuffle(values) {
  const result = [...values];
  for (let i = result.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [result[i], result[j]] = [result[j], result[i]];
  }
  return result;
}

function pick(values) {
  return Array.isArray(values) && values.length
    ? values[Math.floor(Math.random() * values.length)]
    : "—";
}

function randomInt(min, max) {
  return min + Math.floor(Math.random() * (max - min + 1));
}

function years(n) {
  const a = n % 100;
  const b = n % 10;
  return n + (a > 10 && a < 15 ? " лет" : b === 1 ? " год" : b > 1 && b < 5 ? " года" : " лет");
}

function extraValue(set, key) {
  const x = set.sx?.[key];
  if (!x) return "";
  if (x.e && Math.random() * 100 < x.e) return "";
  const list = Array.isArray(x.list) ? x.list : [];
  const range = x.lo != null && x.hi != null && Number(x.hi) >= Number(x.lo);
  let result;
  if (list.length && (!range || Math.random() * 100 < (x.p ?? 50))) {
    result = pick(list);
  } else if (range) {
    const n = randomInt(Number(x.lo), Number(x.hi));
    result = key === "gender" ? years(n) : n + " см";
  } else {
    return "";
  }
  if (/^\d+$/.test(String(result))) {
    result = key === "gender" ? years(Number(result)) : result + " см";
  }
  return /^(пусто|-)$/i.test(String(result)) ? "" : " (" + result + ")";
}

function generateRoll(set, key) {
  const values = set.lists?.[key];
  const parsed = parseExtra(pick(Array.isArray(values) && values.length ? values : ["—"]));
  return { v: parsed.v + extraValue(set, key), h: parsed.h };
}

function createCharacters(room) {
  const ids = Object.keys(room.players).filter(id => id !== room.gm || room.gmPlays);
  const pools = {};
  for (const key of CHARACTER_KEYS) {
    const values = room.set.lists?.[key];
    pools[key] = shuffle(Array.isArray(values) && values.length ? values : ["—"]);
  }
  const chars = {};
  ids.forEach((id, index) => {
    chars[id] = {};
    for (const key of CHARACTER_KEYS) {
      const p = parseExtra(pools[key][index % pools[key].length]);
      chars[id][key] = { v: p.v + extraValue(room.set, key), h: p.h, o: 0 };
    }
  });
  room.chars = chars;
  room.story = pick(room.set.story);
  room.env = {
    size: pick(room.set.bunkerSize),
    rooms: pick(room.set.rooms)
  };
  room.status = "play";
  room.phase = "choose";
  room.n = 1;
  room.surv = Math.min(room.surv, Math.max(1, ids.length - 1));
}

function safeMerge(target, patch) {
  if (!patch || typeof patch !== "object" || Array.isArray(patch)) {
    throw new Error("Некорректное игровое действие.");
  }
  for (const key of Object.keys(patch)) {
    if (FORBIDDEN_KEYS.has(key)) throw new Error("Запрещённое поле.");
    const value = patch[key];
    if (value === null) {
      delete target[key];
    } else if (value && typeof value === "object" && !Array.isArray(value)) {
      if (!target[key] || typeof target[key] !== "object" || Array.isArray(target[key])) {
        target[key] = {};
      }
      safeMerge(target[key], value);
    } else {
      target[key] = value;
    }
  }
  return target;
}

function countPlayers(room) {
  return Object.keys(room.players || {}).length;
}

function isObject(value) {
  return !!value && typeof value === "object" && !Array.isArray(value);
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const origin = request.headers.get("Origin");
    const cors = corsHeaders(origin);

    if (request.method === "OPTIONS") {
      if (origin && !ALLOWED_ORIGINS.has(origin)) return new Response(null, { status: 403 });
      return new Response(null, { status: 204, headers: cors });
    }

    if (url.pathname === "/health" && request.method === "GET") {
      return json({ ok: true, service: "bunker-room-server" });
    }

    if (origin && !ALLOWED_ORIGINS.has(origin)) {
      return json({ error: "Источник запроса не разрешён." }, 403, cors);
    }

    if (url.pathname.startsWith("/api/") && request.method !== "POST") {
      return json({ error: "Метод не поддерживается." }, 405, cors);
    }

    if (url.pathname === "/api/create") {
      const body = await readJsonBody(request);
      if (body.error) return json({ error: body.error }, body.status, cors);
      const uid = validUid(body.value.uid) ? body.value.uid : randomUid();
      const name = cleanText(body.value.name, 24);
      if (!name) return json({ error: "Введите имя игрока." }, 400, cors);

      let set;
      try {
        set = sanitizeSet(body.value.set);
        if (JSON.stringify(set).length > MAX_PACK_BYTES) {
          return json({ error: "Набор слишком большой." }, 413, cors);
        }
      } catch (error) {
        return json({ error: error.message || "Некорректный набор." }, 400, cors);
      }

      const profile = { name, av: cleanAvatar(body.value.av) };
      for (let attempt = 0; attempt < 5; attempt++) {
        const code = randomCode();
        const id = env.ROOMS.idFromName(code);
        const stub = env.ROOMS.get(id);
        const response = await stub.fetch("https://room/_create", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            code, uid, profile, set,
            gmPlays: body.value.gmPlays !== false,
            surv: Math.max(1, Math.min(16, Math.floor(Number(body.value.surv) || 2)))
          })
        });
        const result = await response.json().catch(() => ({}));
        if (response.status === 409) continue;
        return json(result, response.status, cors);
      }
      return json({ error: "Не удалось подобрать свободный код. Попробуйте ещё раз." }, 503, cors);
    }

    if (url.pathname === "/api/join" || url.pathname === "/api/resume") {
      const body = await readJsonBody(request);
      if (body.error) return json({ error: body.error }, body.status, cors);
      const data = body.value;
      const code = String(data.code || "").toUpperCase();
      if (!/^[A-Z2-9]{8}$/.test(code)) {
        return json({ error: "Код комнаты должен состоять из 8 символов." }, 400, cors);
      }
      const ip = request.headers.get("CF-Connecting-IP") || "unknown";
      if (url.pathname === "/api/join" && !allowJoinAttempt(ip)) {
        return json({ error: "Слишком много попыток подключения. Попробуйте через минуту." }, 429, cors);
      }
      const stub = env.ROOMS.get(env.ROOMS.idFromName(code));
      const path = url.pathname === "/api/join" ? "/_join" : "/_resume";
      const response = await stub.fetch("https://room" + path, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Client-IP": ip
        },
        body: JSON.stringify({
          code,
          uid: data.uid,
          token: data.token,
          preferredUid: data.preferredUid,
          profile: {
            name: cleanText(data.name, 24),
            av: cleanAvatar(data.av)
          }
        })
      });
      const result = await response.json().catch(() => ({}));
      return json(result, response.status, cors);
    }

    const match = url.pathname.match(/^\/ws\/([A-Z2-9]{8})$/);
    if (match && request.headers.get("Upgrade")?.toLowerCase() === "websocket") {
      if (!origin || !ALLOWED_ORIGINS.has(origin)) {
        return json({ error: "Источник WebSocket не разрешён." }, 403, cors);
      }
      const stub = env.ROOMS.get(env.ROOMS.idFromName(match[1]));
      const wsUrl = new URL(request.url);
      wsUrl.pathname = "/_ws";
      return stub.fetch(new Request(wsUrl.toString(), request));
    }

    return json({ error: "Маршрут не найден." }, 404, cors);
  },


};

export class Room {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.rate = new Map();
  }

  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/_ws") return this.upgrade(request);

    if (request.method !== "POST") return json({ error: "Метод не поддерживается." }, 405);
    const body = await request.json().catch(() => null);
    if (!isObject(body)) return json({ error: "Некорректный запрос." }, 400);

    if (url.pathname === "/_create") return this.createRoom(body);
    if (url.pathname === "/_join") return this.joinRoom(body, request);
    if (url.pathname === "/_resume") return this.resumeRoom(body);
    return json({ error: "Маршрут не найден." }, 404);
  }

  async createRoom(body) {
    const existing = await this.state.storage.get("room");
    if (existing) return json({ error: "Код уже занят." }, 409);
    if (!validUid(body.uid) || !body.profile?.name) {
      return json({ error: "Некорректные данные создателя." }, 400);
    }

    const token = randomToken();
    const room = {
      host: body.uid,
      hid: body.uid,
      ep: 0,
      gm: body.uid,
      gmPlays: body.gmPlays !== false,
      status: "lobby",
      surv: Math.max(1, Math.min(16, Number(body.surv) || 2)),
      set: body.set,
      players: { [body.uid]: body.profile },
      chars: {},
      out: {},
      shown: {},
      votes: {},
      ans: {},
      n: 0,
      phase: "choose",
      ban: {}
    };

    await this.state.storage.put("room", room);
    await this.state.storage.put("token:" + body.uid, await hashToken(token));
    await this.touch();
    return json({
      ok: true, code: body.code, uid: body.uid, token,
      R: this.viewFor(room, body.uid)
    });
  }

  async joinRoom(body, request) {
    const room = await this.state.storage.get("room");
    if (!room) return json({ error: "Комната не найдена или срок её хранения истёк." }, 404);

    const sourceIp = cleanText(request.headers.get("X-Client-IP") || "unknown", 64);
    const rateKey = "join:" + sourceIp;
    const now = Date.now();
    const prior = (await this.state.storage.get(rateKey)) || [];
    const recent = prior.filter(time => now - time < 60_000);
    if (recent.length >= 10) return json({ error: "Слишком много подключений к комнате. Попробуйте позже." }, 429);
    recent.push(now);
    await this.state.storage.put(rateKey, recent);

    if (room.status !== "lobby") return json({ error: "Игра уже началась. Новые участники не принимаются." }, 403);
    if (countPlayers(room) >= MAX_PLAYERS) return json({ error: "Комната заполнена." }, 403);

    const profile = {
      name: cleanText(body.profile?.name, 24),
      av: cleanAvatar(body.profile?.av)
    };
    if (!profile.name) return json({ error: "Введите имя игрока." }, 400);

    const preferred = validUid(body.preferredUid) ? body.preferredUid : "";
    if (preferred && room.ban?.[preferred]) {
      return json({ error: "Ведущий исключил этот игровой профиль из комнаты." }, 403);
    }

    let uid = preferred && !room.players[preferred] ? preferred : randomUid();
    while (room.players[uid]) uid = randomUid();
    const token = randomToken();

    room.players[uid] = profile;
    await this.state.storage.put("room", room);
    await this.state.storage.put("token:" + uid, await hashToken(token));
    await this.touch();
    await this.broadcast(room);

    return json({ ok: true, code: body.code, uid, token, R: this.viewFor(room, uid) });
  }

  async resumeRoom(body) {
    const room = await this.state.storage.get("room");
    if (!room || !validUid(body.uid) || typeof body.token !== "string") {
      return json({ error: "Сохранённая сессия не найдена. Подключитесь по коду комнаты." }, 404);
    }
    const stored = await this.state.storage.get("token:" + body.uid);
    const actual = await hashToken(body.token);
    if (!stored || stored !== actual || !room.players[body.uid]) {
      return json({ error: "Сессия недействительна. Подключитесь по коду комнаты." }, 403);
    }
    await this.touch();
    return json({ ok: true, code: body.code, uid: body.uid, token: body.token, R: this.viewFor(room, body.uid) });
  }

  async upgrade(request) {
    if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
      return new Response("Expected WebSocket", { status: 426 });
    }
    const room = await this.state.storage.get("room");
    if (!room) return new Response("Room not found", { status: 404 });
    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];
    this.state.acceptWebSocket(server);
    server.serializeAttachment({ uid: null });
    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws, message) {
    if (typeof message !== "string" || message.length > MAX_WS_MESSAGE) {
      ws.close(1009, "Message too large");
      return;
    }
    let data;
    try {
      data = JSON.parse(message);
    } catch {
      ws.close(1003, "Invalid JSON");
      return;
    }
    if (!isObject(data)) {
      ws.close(1003, "Invalid message");
      return;
    }

    const attachment = ws.deserializeAttachment() || {};
    if (!attachment.uid) {
      if (data.type !== "auth" || !validUid(data.uid) || typeof data.token !== "string") {
        ws.close(1008, "Authentication required");
        return;
      }
      const room = await this.state.storage.get("room");
      const stored = await this.state.storage.get("token:" + data.uid);
      const actual = await hashToken(data.token);
      if (!room || !room.players[data.uid] || !stored || stored !== actual) {
        ws.send(JSON.stringify({ t: "err", e: "Не удалось подтвердить сессию. Войдите в комнату заново." }));
        ws.close(1008, "Invalid session");
        return;
      }
      ws.serializeAttachment({ uid: data.uid });
      ws.send(JSON.stringify({ t: "auth", ok: true }));
      ws.send(JSON.stringify({ t: "s", R: this.viewFor(room, data.uid) }));
      await this.touch();
      return;
    }

    const uid = attachment.uid;
    const room = await this.state.storage.get("room");
    if (!room || !room.players[uid]) {
      ws.send(JSON.stringify({ t: "err", e: "Вы больше не состоите в этой комнате." }));
      ws.close(1008, "Not a room member");
      return;
    }

    if (!this.allowAction(uid)) {
      ws.send(JSON.stringify({ t: "err", e: "Слишком много действий подряд. Немного подождите." }));
      return;
    }

    try {
      if (data.t === "u") {
        this.applyPatch(room, uid, data.d);
      } else if (data.t === "start") {
        if (uid !== room.gm || room.status !== "lobby") throw new Error("Начать игру может только ведущий.");
        this.startGame(room);
      } else if (data.t === "rr") {
        if (uid !== room.gm) throw new Error("Изменять карточки может только ведущий.");
        if (!CHARACTER_KEYS.includes(data.k) || !room.chars?.[data.i]?.[data.k]) {
          throw new Error("Некорректная карточка.");
        }
        const rolled = generateRoll(room.set, data.k);
        room.chars[data.i][data.k] = { ...rolled, o: 0 };
      } else {
        throw new Error("Неизвестное действие.");
      }

      this.validateRoom(room);
      await this.state.storage.put("room", room);
      await this.touch();
      await this.broadcast(room);
    } catch (error) {
      ws.send(JSON.stringify({ t: "err", e: cleanText(error.message || "Действие отклонено.", 180) }));
      ws.send(JSON.stringify({ t: "s", R: this.viewFor(room, uid) }));
    }
  }

  applyPatch(room, uid, patch) {
    if (!isObject(patch) || JSON.stringify(patch).length > 18_000) {
      throw new Error("Некорректное или слишком большое действие.");
    }
    if (uid === room.gm) {
      for (const key of Object.keys(patch)) {
        if (!GAME_PATCH_KEYS.has(key)) throw new Error("Ведущий изменяет недопустимое поле.");
      }
      safeMerge(room, patch);
      return;
    }

    const allowed = Object.keys(patch);
    if (!allowed.length || allowed.some(key => !["chars", "votes", "shown"].includes(key))) {
      throw new Error("У участника нет прав на это действие.");
    }
    if (room.out?.[uid] && room.status !== "end") throw new Error("Выбывший игрок не может выполнять это действие.");

    if (patch.chars) {
      if (!isObject(patch.chars) || Object.keys(patch.chars).length !== 1 || !patch.chars[uid]) {
        throw new Error("Можно изменять только собственные карточки.");
      }
      const fields = patch.chars[uid];
      if (!isObject(fields)) throw new Error("Некорректная карточка.");
      for (const key of Object.keys(fields)) {
        if (!CHARACTER_KEYS.includes(key) || !isObject(fields[key]) || Object.keys(fields[key]).some(x => x !== "o") || fields[key].o !== 1) {
          throw new Error("Можно только раскрывать собственные карточки.");
        }
        const card = room.chars?.[uid]?.[key];
        if (!card) throw new Error("Карточка не найдена.");
        if (key === "spec") {
          if (card.o && room.status !== "end") throw new Error("Специальная возможность уже активирована.");
        } else if (room.status !== "end") {
          if (room.status !== "play" || room.phase !== "reveal" || room.shown?.[room.n]?.[uid]) {
            throw new Error("Сейчас нельзя раскрывать ещё одну характеристику.");
          }
        }
        card.o = 1;
      }
      if (room.status === "play" && room.phase === "reveal") {
        room.shown ||= {};
        room.shown[room.n] ||= {};
        room.shown[room.n][uid] = true;
      }
    }

    if (patch.shown) {
      if (room.status !== "play" || room.phase !== "reveal" ||
          !isObject(patch.shown) || Object.keys(patch.shown).length !== 1 ||
          !patch.shown[room.n] || Object.keys(patch.shown[room.n]).length !== 1 ||
          patch.shown[room.n][uid] !== true) {
        throw new Error("Нельзя менять отметки раскрытия других игроков.");
      }
      room.shown ||= {};
      room.shown[room.n] ||= {};
      room.shown[room.n][uid] = true;
    }

    if (patch.votes) {
      if (room.status !== "play" || room.phase !== "vote" || room.vdone?.[room.n]) {
        throw new Error("Сейчас голосование недоступно.");
      }
      if (!isObject(patch.votes) || Object.keys(patch.votes).length !== 1 ||
          !Object.prototype.hasOwnProperty.call(patch.votes, room.n) ||
          !isObject(patch.votes[room.n]) || Object.keys(patch.votes[room.n]).length !== 1 ||
          !Object.prototype.hasOwnProperty.call(patch.votes[room.n], uid)) {
        throw new Error("Можно отправить только собственный голос.");
      }
      const target = patch.votes[room.n][uid];
      if (typeof target !== "string" || target === uid ||
          !room.players[target] || !room.chars[target] || room.out?.[target]) {
        throw new Error("Нельзя голосовать за этого участника.");
      }
      room.votes ||= {};
      room.votes[room.n] ||= {};
      room.votes[room.n][uid] = target;
    }
  }

  startGame(room) {
    if (room.status !== "lobby") throw new Error("Игра уже запущена.");
    if (!room.players[room.gm]) throw new Error("Ведущий больше не находится в комнате.");
    const ids = Object.keys(room.players).filter(id => id !== room.gm || room.gmPlays);
    if (ids.length < 2) throw new Error("Для игры нужно хотя бы два участника.");
    createCharacters(room);
  }

  validateRoom(room) {
    if (!["lobby", "play", "end"].includes(room.status)) throw new Error("Некорректный статус партии.");
    if (!["choose", "reveal", "vote"].includes(room.phase)) throw new Error("Некорректная фаза игры.");
    if (room.gm && !room.players[room.gm]) throw new Error("Ведущий должен быть участником комнаты.");
    room.surv = Math.max(1, Math.min(16, Math.floor(Number(room.surv) || 1)));
    if (!isObject(room.players) || countPlayers(room) > MAX_PLAYERS) {
      throw new Error("Некорректный список игроков.");
    }
    for (const [uid, player] of Object.entries(room.players)) {
      if (!validUid(uid) || !isObject(player) || typeof player.name !== "string" || player.name.length > 24) {
        throw new Error("Некорректный профиль игрока.");
      }
    }
    if (!isObject(room.chars) || !isObject(room.out) || !isObject(room.votes) || !isObject(room.ans)) {
      throw new Error("Некорректное состояние партии.");
    }
  }

  viewFor(room, uid) {
    const view = structuredClone(room);
    view.set = {
      name: room.set?.name || "Набор",
      bid: room.set?.bid || "",
      q: Array.isArray(room.set?.q) ? room.set.q : []
    };

    for (const [playerId, cards] of Object.entries(view.chars || {})) {
      for (const key of Object.keys(cards || {})) {
        const card = cards[key];
        if (!card?.o && playerId !== uid && uid !== room.gm) {
          cards[key] = { o: 0, ...(card?.u ? { u: 1 } : {}) };
        }
      }
    }

    if (uid !== room.gm) {
      const votes = {};
      for (const [round, roundVotes] of Object.entries(room.votes || {})) {
        if (room.vdone?.[round]) {
          votes[round] = structuredClone(roundVotes);
        } else {
          votes[round] = {};
          for (const [voter, target] of Object.entries(roundVotes || {})) {
            votes[round][voter] = voter === uid ? target : "?";
          }
        }
      }
      view.votes = votes;
    }
    return view;
  }

  async broadcast(room) {
    for (const ws of this.state.getWebSockets()) {
      const attachment = ws.deserializeAttachment() || {};
      const uid = attachment.uid;
      if (!uid) continue;
      if (!room.players[uid]) {
        try {
          ws.send(JSON.stringify({ t: "err", e: "Вас исключили из комнаты." }));
          ws.close(1008, "Removed from room");
        } catch {}
        continue;
      }
      try {
        ws.send(JSON.stringify({ t: "s", R: this.viewFor(room, uid) }));
      } catch {}
    }
  }

  allowAction(uid) {
    const now = Date.now();
    const times = (this.rate.get(uid) || []).filter(time => now - time < 10_000);
    if (times.length >= 35) return false;
    times.push(now);
    this.rate.set(uid, times);
    return true;
  }

  async touch() {
    await this.state.storage.put("lastActivity", Date.now());
    await this.state.storage.setAlarm(Date.now() + ROOM_TTL_MS);
  }

  async alarm() {
    const last = await this.state.storage.get("lastActivity") || 0;
    if (Date.now() - last >= ROOM_TTL_MS) {
      for (const ws of this.state.getWebSockets()) {
        try { ws.close(1001, "Room expired"); } catch {}
      }
      await this.state.storage.deleteAll();
    } else {
      await this.state.storage.setAlarm(last + ROOM_TTL_MS);
    }
  }
}
