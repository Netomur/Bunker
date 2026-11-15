import { chromium } from 'playwright';
import { createServer } from 'node:http';
import { WebSocketServer } from 'ws';
import { readFile, stat, writeFile } from 'node:fs/promises';
import { extname, resolve, sep } from 'node:path';
import { randomBytes } from 'node:crypto';

const ROOT = resolve(process.cwd());
const HOST = '127.0.0.1';
const APP_PORT = 4173;
const BROKER_PORT = 4174;
const BASE_URL = 'http://' + HOST + ':' + APP_PORT + '/';
const RUN_ID = randomBytes(6).toString('hex');
const PUBLIC_BROKERS = [
  ['emqx', 'wss://broker.emqx.io:8084/mqtt'],
  ['hivemq', 'wss://broker.hivemq.com:8884/mqtt'],
  ['eclipse', 'wss://mqtt.eclipseprojects.io:443'],
  ['mosquitto', 'wss://test.mosquitto.org:8081/'],
];

const report = {
  runId: RUN_ID,
  startedAt: new Date().toISOString(),
  mode: 'test branch only; no GitHub Pages deployment',
  scenarios: [],
  browserErrors: [],
  publicMqtt: null,
  summary: {},
};

function record(name, status, details = {}) {
  report.scenarios.push({ name, status, details, at: new Date().toISOString() });
  console.log('[' + status + '] ' + name + ': ' + JSON.stringify(details));
}

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

const mime = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.json': 'application/json; charset=utf-8',
};

const httpServer = createServer(async (req, res) => {
  try {
    let rel = decodeURIComponent(new URL(req.url || '/', BASE_URL).pathname).replace(/^\/+/, '');
    if (!rel) rel = 'index.html';
    const file = resolve(ROOT, rel);
    assert(file === ROOT || file.startsWith(ROOT + sep), 'Path traversal blocked');
    const info = await stat(file);
    assert(info.isFile(), 'Not a file');
    const body = await readFile(file);
    res.writeHead(200, {
      'content-type': mime[extname(file).toLowerCase()] || 'application/octet-stream',
      'cache-control': 'no-store',
      'content-length': body.length,
    });
    res.end(body);
  } catch {
    res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' });
    res.end('Not found');
  }
});
await new Promise((ok) => httpServer.listen(APP_PORT, HOST, ok));

/* Deterministic MQTT-like broker for browser stress. Each broker URL has an
   isolated topic namespace. A separate scenario probes the real WSS brokers. */
const wss = new WebSocketServer({ host: HOST, port: BROKER_PORT });
const sockets = new Set();
const brokerEvents = [];
wss.on('connection', (ws) => {
  const client = { ws, broker: '', player: 'unknown', subscriptions: new Set() };
  sockets.add(client);
  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(String(raw)); } catch { return; }
    if (msg.type === 'hello' && typeof msg.broker === 'string') {
      client.broker = msg.broker;
      client.player = msg.player || 'unknown';
      ws.send(JSON.stringify({ type: 'ready' }));
      return;
    }
    if (!client.broker) return;
    if (msg.type === 'subscribe' && typeof msg.topic === 'string') {
      client.subscriptions.add(msg.topic);
      return;
    }
    if (msg.type === 'unsubscribe' && typeof msg.topic === 'string') {
      client.subscriptions.delete(msg.topic);
      return;
    }
    if (msg.type === 'publish' && typeof msg.topic === 'string') {
      try { const parsed = JSON.parse(String(msg.payload ?? '')); if (parsed.t === 'join' || parsed.t === 'err') brokerEvents.push({ broker: client.broker, sender: client.player, topic: msg.topic, messageType: parsed.t, uid: parsed.uid, messageId: parsed.id, error: parsed.e, receivers: [...sockets].filter(s => s !== client && s.broker === client.broker && s.subscriptions.has(msg.topic)).map(s=>s.player) }); } catch {}
      const packet = JSON.stringify({ type: 'message', topic: msg.topic, payload: String(msg.payload ?? '') });
      for (const target of sockets) {
        if (target !== client && target.broker === client.broker &&
            target.subscriptions.has(msg.topic) && target.ws.readyState === 1) {
          target.ws.send(packet);
        }
      }
    }
  });
  ws.on('close', () => sockets.delete(client));
  ws.on('error', () => sockets.delete(client));
});
await new Promise((ok) => wss.once('listening', ok));

const browser = await chromium.launch({ headless: true, args: ['--no-sandbox', '--disable-dev-shm-usage'] });

const fakeMqttInit = [
  '(() => {',
  '  class TestClient {',
  '    constructor(url) {',
  '      this.url=url; this.handlers=Object.create(null); this.subscriptions=new Set(); this.connected=false;',
  "      this.ws=new WebSocket('ws://127.0.0.1:4174');",
  "      this.ws.addEventListener('open',()=>this.ws.send(JSON.stringify({type:'hello',broker:url,player:window.__testPlayerName||'unknown'})));",
  '      this.ws.addEventListener("message",(event)=>{',
  '        let msg; try { msg=JSON.parse(event.data); } catch { return; }',
  "        if(msg.type==='ready'){this.connected=true;this.emit('connect');}",
  "        else if(msg.type==='message'&&this.subscriptions.has(msg.topic)){const payload=String(msg.payload??'');this.emit('message',msg.topic,{toString:()=>payload});}",
  '      });',
  "      this.ws.addEventListener('close',()=>{this.connected=false;this.emit('close');});",
  "      this.ws.addEventListener('error',()=>this.emit('error',new Error('fake broker websocket error')));",
  '    }',
  '    on(name,callback){(this.handlers[name]||=[]).push(callback);return this;}',
  '    emit(name,...args){for(const cb of this.handlers[name]||[]){try{cb(...args);}catch(error){setTimeout(()=>{throw error;},0);}}}',
  '    subscribe(topic,options,callback){if(typeof options==="function")callback=options;this.subscriptions.add(topic);',
  '      const send=()=>{if(this.ws.readyState===WebSocket.OPEN)this.ws.send(JSON.stringify({type:"subscribe",topic}));};',
  '      if(this.ws.readyState===WebSocket.OPEN)send();else this.ws.addEventListener("open",send,{once:true});',
  '      if(callback)callback(null,[{topic,qos:0}]);return this;}',
  '    unsubscribe(topic,callback){this.subscriptions.delete(topic);if(this.ws.readyState===WebSocket.OPEN)this.ws.send(JSON.stringify({type:"unsubscribe",topic}));if(callback)callback(null);return this;}',
  '    publish(topic,payload,options,callback){if(typeof options==="function")callback=options;',
  '      if(this.ws.readyState===WebSocket.OPEN){this.ws.send(JSON.stringify({type:"publish",topic,payload:String(payload)}));if(callback)callback(null);}',
  '      else if(callback)callback(new Error("socket not connected"));return this;}',
  '    end(force,callback){this.connected=false;try{this.ws.close();}catch{}if(callback)callback();return this;}',
  '  }',
  '  window.__bunkerTestMqtt={connect:(url)=>new TestClient(url)};',
  '})();',
].join('\n');

async function newPlayer(name, uid, fake = true) {
  const context = await browser.newContext();
  const page = await context.newPage();
  page.on('pageerror', (error) => report.browserErrors.push({ player: name, message: error.message }));
  page.on('console', (msg) => {
    if (msg.type() === 'error') report.browserErrors.push({ player: name, console: msg.text() });
  });
  if (fake) {
    await context.addInitScript(({ playerName, playerId, init }) => {
      window.__testPlayerName = playerName;
      localStorage.setItem('bn', playerName);
      localStorage.setItem('bu', JSON.stringify(playerId));
      (0, eval)(init);
    }, { playerName: name, playerId: uid, init: fakeMqttInit });
    await page.route('**/mqtt.min.js*', (route) => route.fulfill({
      status: 200,
      contentType: 'application/javascript',
      body: 'window.mqtt = window.__bunkerTestMqtt;',
    }));
  }
  await page.goto(BASE_URL, { waitUntil: 'domcontentloaded' });
  await page.waitForSelector('#name', { timeout: 15000 });
  return { name, uid, context, page };
}

async function click(page, selector, timeout = 10000) {
  const element = page.locator(selector).first();
  await element.waitFor({ state: 'visible', timeout });
  await element.click();
}

async function bodyText(page) { return page.locator('body').innerText(); }

async function waitBody(page, needle, timeout = 15000) {
  await page.waitForFunction((text) =>
    document.body && document.body.innerText.includes(text), needle, { timeout });
}

async function makeRoom(host) {
  await host.page.locator('#name').fill(host.name);
  await click(host.page, 'button[data-a=create]');
  await waitBody(host.page, 'Код комнаты');
  const match = (await bodyText(host.page)).match(/Код комнаты\s+([A-Z2-9]{6})/);
  assert(match, 'Room code was not rendered');
  return match[1];
}

async function joinRoom(player, code) {
  await player.page.locator('#name').fill(player.name);
  await player.page.locator('#code').fill(code);
  await click(player.page, 'button[data-a=join]');
}

async function waitForNames(host, names, timeout = 20000) {
  await host.page.waitForFunction((expected) =>
    expected.every((name) => document.body.innerText.includes(name)), names, { timeout });
}

async function createPlayers(count, prefix) {
  const clean = prefix.toLowerCase().replace(/[^a-z0-9]/g, '').slice(0, 8);
  const players = [await newPlayer(prefix + 'Host', 'uhost' + clean)];
  for (let i = 1; i < count; i++) {
    const suffix = String(i).padStart(2, '0');
    players.push(await newPlayer(prefix + suffix, 'u' + clean + suffix));
  }
  return players;
}

async function closePlayers(players) {
  await Promise.all(players.map(async (p) => { try { await p.context.close(); } catch {} }));
}

async function answerAll(page, failCritical = false) {
  const count = await page.locator('button[data-a=ans][data-k=y]').count();
  assert(count >= 8, 'Expected built-in final questions, found ' + count);
  if (failCritical) {
    await click(page, 'button[data-a=ans][data-k=n][data-i="0"]');
    for (let i = 1; i < count; i++) {
      await click(page, 'button[data-a=ans][data-k=y][data-i="' + i + '"]');
    }
  } else {
    for (let i = 0; i < count; i++) {
      await click(page, 'button[data-a=ans][data-k=y][data-i="' + i + '"]');
    }
  }
}

async function createAndJoin(count, prefix) {
  const players = await createPlayers(count, prefix);
  const code = await makeRoom(players[0]);
  await Promise.all(players.slice(1).map((p) => joinRoom(p, code)));
  await waitForNames(players[0], players.map((p) => p.name), 30000);
  await Promise.all(players.slice(1).map((p) => waitBody(p.page, 'Код комнаты', 30000)));
  return { players, code };
}

async function scenarioNormalGameAndRecovery() {
  const players = await createPlayers(4, 'Alpha');
  try {
    const code = await makeRoom(players[0]);

    await click(players[0].page, 'button[data-a=start]');
    await waitBody(players[0].page, 'нужны минимум два участника');
    assert((await bodyText(players[0].page)).includes('Игроки (1)'), 'Solo host left the lobby');

    await Promise.all(players.slice(1).map((p) => joinRoom(p, code)));
    await waitForNames(players[0], players.map((p) => p.name), 30000);

    await click(players[0].page, 'button[data-a=start]');
    await waitBody(players[0].page, 'Выберите тему раунда');
    const columns = await players[0].page.locator('.w table tr:first-child th').count();
    assert(columns === 14, 'Expected 12 character categories plus player/actions, got ' + columns);

    await click(players[0].page, 'button[data-a=ph][data-k=reveal]');
    await Promise.all(players.map((p) => p.page.locator('button[data-a=rev]').first().click()));
    await waitBody(players[0].page, 'Показали: 4/4', 20000);
    await click(players[0].page, 'button[data-a=endr]');
    await click(players[0].page, 'button[data-a=ph][data-k=vote]');

    const eliminated = players[3].name;
    await Promise.all(players.map(async (p, index) => {
      const target = index === 3 ? players[2].name : eliminated;
      await p.page.locator('tr').filter({ hasText: target }).locator('button[data-a=vote]').click();
    }));
    await waitBody(players[0].page, 'Проголосовали: 4/4', 20000);
    await click(players[0].page, 'button[data-a=vend]');
    await click(players[0].page, 'button[data-a=out][data-i="' + players[3].uid + '"]');

    await click(players[0].page, 'button[data-a=fin]');
    await answerAll(players[0].page, false);
    await waitBody(players[0].page, 'Выжившие справились!', 15000);
    for (const p of players.slice(1)) await waitBody(p.page, 'Выжившие справились!', 15000);

    await players[0].page.reload({ waitUntil: 'domcontentloaded' });
    await players[0].page.waitForSelector('button[data-a=rest]', { timeout: 15000 });
    await click(players[0].page, 'button[data-a=rest]');
    await waitBody(players[0].page, 'Комната ' + code, 15000);

    await closePlayers(players);
    record('normal four-player game, simultaneous reveals, vote, elimination, successful finale and restore', 'PASS', {
      players: 4, characterCategories: 12, simultaneousReveals: 4, votes: 4,
      eliminated, finale: 'successful', hostRestore: 'recovered from local snapshot after reload',
    });
  } catch (error) { await closePlayers(players); throw error; }
}

async function scenarioCriticalFinalRule() {
  const { players } = await createAndJoin(3, 'Bravo');
  try {
    await click(players[0].page, 'button[data-a=start]');
    await waitBody(players[0].page, 'Выберите тему раунда');
    await click(players[0].page, 'button[data-a=fin]');
    await answerAll(players[0].page, true);
    const body = await bodyText(players[0].page);
    const score = Number(body.match(/Итоговая оценка:\s*(\d+)%/)?.[1] || 0);
    assert(score >= 70, 'Expected weighted score >=70%, got ' + score);
    assert(body.includes('Выжившие не справились.'), 'Priority 10 failure did not override passing score');
    await closePlayers(players);
    record('critical priority-10 question overrides a passing weighted score', 'PASS', { scorePercent: score, wrongCriticalAnswers: 1 });
  } catch (error) { await closePlayers(players); throw error; }
}

async function scenarioEightPlayerStress() {
  const { players } = await createAndJoin(8, 'Charlie');
  try {
    await click(players[0].page, 'button[data-a=start]');
    await waitBody(players[0].page, 'Выберите тему раунда');
    await click(players[0].page, 'button[data-a=ph][data-k=reveal]');
    await Promise.all(players.map((p) => p.page.locator('button[data-a=rev]').first().click()));
    await waitBody(players[0].page, 'Показали: 8/8', 30000);

    await click(players[0].page, 'button[data-a=endr]');
    await click(players[0].page, 'button[data-a=ph][data-k=vote]');
    const target = players[7].name;
    await Promise.all(players.map(async (p, i) => {
      const desired = i === 7 ? players[6].name : target;
      await p.page.locator('tr').filter({ hasText: desired }).locator('button[data-a=vote]').click();
    }));
    await waitBody(players[0].page, 'Проголосовали: 8/8', 30000);

    await players[1].page.locator('tr').filter({ hasText: players[5].name }).locator('button[data-a=vote]').click();
    await waitBody(players[0].page, 'Проголосовали: 8/8', 10000);
    await click(players[0].page, 'button[data-a=vend]');

    const rows = await players[0].page.locator('.w table tr').count();
    assert(rows === 9, 'Expected one header and eight player rows, got ' + rows);
    await closePlayers(players);
    record('eight-player concurrency stress', 'PASS', {
      players: 8, concurrentReveals: 8, concurrentVotes: 8,
      voteChange: 'one vote changed without increasing voter count', rows,
    });
  } catch (error) { await closePlayers(players); throw error; }
}

async function scenarioTwelvePlayerStress() {
  const { players } = await createAndJoin(12, 'Foxtrot');
  try {
    await click(players[0].page, 'button[data-a=start]');
    await waitBody(players[0].page, 'Выберите тему раунда');
    await click(players[0].page, 'button[data-a=ph][data-k=reveal]');
    await Promise.all(players.map((p) => p.page.locator('button[data-a=rev]').first().click()));
    await waitBody(players[0].page, 'Показали: 12/12', 30000);

    await click(players[0].page, 'button[data-a=endr]');
    await click(players[0].page, 'button[data-a=ph][data-k=vote]');
    const target = players[11].name;
    await Promise.all(players.map(async (p, index) => {
      const desired = index === 11 ? players[10].name : target;
      await p.page.locator('tr').filter({ hasText: desired }).locator('button[data-a=vote]').click();
    }));
    await waitBody(players[0].page, 'Проголосовали: 12/12', 30000);
    await click(players[0].page, 'button[data-a=vend]');
    const rows = await players[0].page.locator('.w table tr').count();
    assert(rows === 13, 'Expected one header and twelve player rows, got ' + rows);

    await closePlayers(players);
    record('twelve-player concurrency stress', 'PASS', {
      players: 12,
      simultaneousReveals: 12,
      simultaneousVotes: 12,
      tableRowsIncludingHeader: rows,
    });
  } catch (error) { await closePlayers(players); throw error; }
}

async function scenarioConcurrentRoomIsolation() {
  const a = await createAndJoin(3, 'Hotel');
  const b = await createAndJoin(3, 'India');
  const all = [...a.players, ...b.players];
  try {
    const bodyA = await bodyText(a.players[0].page);
    const bodyB = await bodyText(b.players[0].page);
    assert(bodyA.includes('Hotel01') && !bodyA.includes('IndiaHost'), 'Room A contains a player from room B');
    assert(bodyB.includes('India01') && !bodyB.includes('HotelHost'), 'Room B contains a player from room A');

    await Promise.all([
      click(a.players[0].page, 'button[data-a=start]'),
      click(b.players[0].page, 'button[data-a=start]'),
    ]);
    await Promise.all([
      waitBody(a.players[0].page, 'Выберите тему раунда'),
      waitBody(b.players[0].page, 'Выберите тему раунда'),
    ]);
    const afterA = await bodyText(a.players[0].page);
    const afterB = await bodyText(b.players[0].page);
    assert(afterA.includes('Hotel01') && !afterA.includes('IndiaHost'), 'Room A received state from room B');
    assert(afterB.includes('India01') && !afterB.includes('HotelHost'), 'Room B received state from room A');

    await closePlayers(all);
    record('two concurrent independent rooms', 'PASS', {
      rooms: 2,
      players: all.length,
      roomA: a.code,
      roomB: b.code,
      crossRoomMessagesObserved: false,
    });
  } catch (error) { await closePlayers(all); throw error; }
}

async function scenarioKickAndBan() {
  const { players, code } = await createAndJoin(3, 'Delta');
  try {
    const kicked = players[2];
    await players[0].page.locator('p.pl').filter({ hasText: kicked.name }).locator('button[data-a=kick]').click();
    await waitBody(kicked.page, 'Вас нет в списке игроков', 15000);
    await click(kicked.page, 'button[data-a=leave]');
    await kicked.page.locator('#code').fill(code);
    await kicked.page.waitForTimeout(800);
    const codeAfterOldDisconnect = await kicked.page.locator('#code').inputValue();
    assert(codeAfterOldDisconnect === code, 'A delayed close event from the previous room erased the new room code');
    const enteredCode = await kicked.page.locator('#code').inputValue();
    const codeAcceptedByUi = /^[ABCDEFGHJKLMNPQRSTUVWXYZ23456789]{6}$/.test(enteredCode);
    if (!codeAcceptedByUi) throw new Error('Test supplied invalid room code: ' + JSON.stringify({ code, enteredCode }));
    await click(kicked.page, 'button[data-a=join]');
    await waitBody(kicked.page, 'Ведущий исключил вас из комнаты', 15000).catch(async (error) => {
      const guestStored = await kicked.page.evaluate(() => {
        let id = null; try { id = JSON.parse(localStorage.getItem('bu')); } catch {}
        return { id, body: document.body.innerText, codeValue: document.querySelector('#code')?.value, app: window.__bunkerTestDebug?.() };
      });
      const hostStored = await players[0].page.evaluate(() => {
        let saved = null; try { saved = JSON.parse(localStorage.getItem('bh')); } catch {}
        return { ban: saved?.R?.ban || null, players: saved?.R?.players || null, app: window.__bunkerTestDebug?.(), body: document.body.innerText };
      });
      console.log('[KICK DEBUG]', JSON.stringify({ code, enteredCode, guestStored, hostStored, brokerEvents: brokerEvents.slice(-40) }));
      throw error;
    });
    await closePlayers(players);
    record('kick and rejoin ban', 'PASS', { kickedPlayer: kicked.name, rejoin: 'rejected' });
  } catch (error) { await closePlayers(players); throw error; }
}

async function scenarioImportValidation() {
  const player = await newPlayer('ImportTester', 'uimport001');
  try {
    await click(player.page, 'button[data-a=new]');
    await player.page.locator('#e_name').fill('Тестовый набор');
    await player.page.locator('#e_story').fill('Тестовый сценарий');
    await player.page.locator('#e_gender').fill('Мужчина\\nЖенщина');
    await player.page.locator('#e_bunkerSize').fill('Средний бункер');
    await player.page.locator('#e_rooms').fill('Жилая зона, склад');
    await player.page.locator('#e_q').fill('Есть ли вода? [10] | да');
    await click(player.page, 'button[data-a=save]');
    await waitBody(player.page, 'Тестовый набор');

    const downloadPromise = player.page.waitForEvent('download');
    await click(player.page, 'button[data-a=exp]');
    const download = await downloadPromise;
    assert(download.suggestedFilename() === 'bunker-sets.json', 'Unexpected export filename');

    await player.page.locator('input[data-a=imp]').setInputFiles({
      name: 'not-an-array.json',
      mimeType: 'application/json',
      buffer: Buffer.from(JSON.stringify({ not: 'an array' })),
    });
    await waitBody(player.page, 'Ошибка:', 10000);
    assert((await bodyText(player.page)).includes('Тестовый набор'), 'Invalid import destroyed saved set');
    await closePlayers([player]);
    record('custom pack creation, export and malformed import handling', 'PASS', {
      createdPack: 'Тестовый набор', exportFilename: download.suggestedFilename(),
      malformedImport: 'graceful error and existing data preserved',
    });
  } catch (error) { await closePlayers([player]); throw error; }
}

async function scenarioHostFailover() {
  const { players } = await createAndJoin(3, 'Echo');
  try {
    await click(players[0].page, 'button[data-a=start]');
    await waitBody(players[0].page, 'Выберите тему раунда');
    await players[0].context.close();

    const successor = players.slice(1).sort((a, b) => a.uid.localeCompare(b.uid))[0];
    await waitBody(successor.page, 'Прежний хост пропал', 70000);
    const canControl = await successor.page.locator('button[data-a=fin]').count();
    if (canControl === 0) {
      const state = await successor.page.evaluate(() => {
        let saved = null; try { saved = JSON.parse(localStorage.getItem('bh')); } catch {}
        return { savedGM: saved?.R?.gm, savedHid: saved?.R?.hid, status: saved?.R?.status, app: window.__bunkerTestDebug?.(), body: document.body.innerText };
      });
      console.log('[FAILOVER DEBUG]', JSON.stringify({ oldHost: players[0].uid, oldGM: players[0].uid, successor: successor.uid, candidates: players.slice(1).map(p=>p.uid), state }));
    }
    assert(canControl > 0, 'Successor inherited network hosting but not game-leading permissions');

    await closePlayers(players.slice(1));
    record('host failover after heartbeat loss', 'PASS', {
      previousHost: players[0].name, successor: successor.name, successorCanControlGame: true,
    });
  } catch (error) { await closePlayers(players.slice(1)); throw error; }
}

async function scenarioPublicMqttSmoke() {
  let stage = 'create public-broker host';
  let players = [];
  try {
    const host = await newPlayer('PublicMQTTHost', 'upublichost', false);
    players.push(host);
    const code = await makeRoom(host);
    await waitBody(host.page, 'Серверы связи:', 30000);
    await host.page.waitForFunction(
      () => /Серверы связи:[^\\n]*✓/.test(document.body.innerText),
      null,
      { timeout: 35000 }
    );

    stage = 'join a second browser through the public MQTT brokers';
    const guest = await newPlayer('PublicMQTTGuest', 'upublicguest', false);
    players.push(guest);
    await joinRoom(guest, code);
    await waitForNames(host, [host.name, guest.name], 60000);
    await waitBody(guest.page, host.name, 60000);

    stage = 'start the actual two-browser game';
    await click(host.page, 'button[data-a=start]');
    await waitBody(host.page, 'Выберите тему раунда', 30000);
    await waitBody(guest.page, 'Выберите тему раунда', 30000);
    await click(host.page, 'button[data-a=ph][data-k=reveal]');
    await waitBody(guest.page, 'Показ характеристик.', 30000);
    await Promise.all([
      click(host.page, 'button[data-a=rev]'),
      click(guest.page, 'button[data-a=rev]'),
    ]);
    await waitBody(host.page, 'Показали: 2/2', 30000);
    await waitBody(guest.page, 'Показали: 2/2', 30000);

    const hostBody = await bodyText(host.page);
    const guestBody = await bodyText(guest.page);
    const getConnected = (body) => {
      const line = body.match(/Серверы связи:[^\\n]*/)?.[0] || '';
      return PUBLIC_BROKERS.filter(([name]) => line.includes(name + ' ✓')).map(([name]) => name);
    };
    const hostConnected = getConnected(hostBody);
    const guestConnected = getConnected(guestBody);
    const common = hostConnected.filter(name => guestConnected.includes(name));
    report.publicMqtt = {
      configured: PUBLIC_BROKERS.map(([name, url]) => ({ name, url })),
      hostConnected,
      guestConnected,
      common,
      roomCode: code,
      simultaneousReveals: 2,
      synchronized: hostBody.includes('Показали: 2/2') && guestBody.includes('Показали: 2/2'),
    };
    assert(common.length > 0, 'The two browsers did not share a connected public MQTT broker');
    await closePlayers(players);
    record('real two-browser game over public MQTT WSS', 'PASS', report.publicMqtt);
  } catch (error) {
    await closePlayers(players);
    report.publicMqtt = { stage, error: error.message, configured: PUBLIC_BROKERS.map(([name, url]) => ({ name, url })) };
    record('real two-browser game over public MQTT WSS', 'WARN', report.publicMqtt);
  }
}

const scenarios = [
  ['normal four-player game and recovery', scenarioNormalGameAndRecovery],
  ['critical final scoring rule', scenarioCriticalFinalRule],
  ['eight-player concurrency stress', scenarioEightPlayerStress],
  ['twelve-player concurrency stress', scenarioTwelvePlayerStress],
  ['two concurrent independent rooms', scenarioConcurrentRoomIsolation],
  ['kick and ban', scenarioKickAndBan],
  ['custom pack import/export', scenarioImportValidation],
  ['host failover', scenarioHostFailover],
  ['real public MQTT connectivity', scenarioPublicMqttSmoke],
];
for (const [name, run] of scenarios) {
  try {
    await run();
  } catch (error) {
    record(name, 'FAIL', { message: error.message, stack: String(error.stack || '').split('\n').slice(0, 4) });
    console.error('[SCENARIO FAIL]', name, error.stack || error);
  }
}
if (report.browserErrors.length) {
  record('browser runtime errors', 'FAIL', { errors: report.browserErrors.slice(0, 12), count: report.browserErrors.length });
} else {
  record('browser runtime errors', 'PASS', { count: 0 });
}
{
  report.finishedAt = new Date().toISOString();
  report.summary = {
    passed: report.scenarios.filter((x) => x.status === 'PASS').length,
    warnings: report.scenarios.filter((x) => x.status === 'WARN').length,
    failed: report.scenarios.filter((x) => x.status === 'FAIL').length,
    browserErrors: report.browserErrors.length,
  };
  try { await writeFile('bunker-stress-report.json', JSON.stringify(report, null, 2)); } catch {}
  try { await browser.close(); } catch {}
  try { await new Promise((ok) => wss.close(ok)); } catch {}
  try { await new Promise((ok) => httpServer.close(ok)); } catch {}
}
console.log('[SUMMARY]', JSON.stringify(report.summary));
if (report.summary.failed > 0) process.exitCode = 1;
