#!/usr/bin/env node
'use strict';

// ASCII Realms - servidor de salas (relay WebSocket).
//
// Quem cria a sala vira o "host" (id 1) e roda o mundo; quem entra com o codigo recebe um id
// aleatorio. O servidor so repassa os pacotes binarios entre os jogadores da mesma sala, entao
// ninguem precisa abrir porta no roteador. Sem dependencias: so Node.js 18+.
//
//   node relay-server.js            (porta 8080, ou a variavel de ambiente PORT)
//
// Protocolo:
//   texto  (JSON)  cliente -> servidor  {op:"create", v, max, code?} | {op:"join", v, code} | {op:"kick", id}
//                  servidor -> cliente  {op:"created", code, id} | {op:"joined", code, id, peers:[...]}
//                                       {op:"peer_join", id} | {op:"peer_leave", id} | {op:"room_closed"}
//                                       {op:"error", msg}
//   binario        cliente -> servidor  [destino int32 LE][canal u8][modo u8][dados]
//                  servidor -> cliente  [origem  int32 LE][canal u8][modo u8][dados]
//                  destino 0 = todos da sala, negativo = todos menos -destino

const http = require('http');
const crypto = require('crypto');

const PORT = parseInt(process.env.PORT || '8080', 10);
const PROTOCOL = 1;
const MAX_ROOMS = parseInt(process.env.MAX_ROOMS || '500', 10);
const MAX_PLAYERS_HARD = 16;
const MAX_MESSAGE = 1 << 20;             // 1 MB por mensagem
const SLOW_CLIENT_BYTES = 8 * 1024 * 1024;
const DROP_UNRELIABLE_BYTES = 512 * 1024;
const HEARTBEAT_MS = 20000;
const CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
const WS_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const MODE_RELIABLE = 2;

const rooms = new Map();   // code -> {code, max, host, peers: Map<id, Client>}
const clients = new Set();

function log(...args) {
  console.log(new Date().toISOString(), ...args);
}

function encodeFrame(opcode, payload) {
  const len = payload.length;
  let header;
  if (len < 126) {
    header = Buffer.alloc(2);
    header[1] = len;
  } else if (len < 65536) {
    header = Buffer.alloc(4);
    header[1] = 126;
    header.writeUInt16BE(len, 2);
  } else {
    header = Buffer.alloc(10);
    header[1] = 127;
    header.writeBigUInt64BE(BigInt(len), 2);
  }
  header[0] = 0x80 | opcode;
  return Buffer.concat([header, payload]);
}

class Client {
  constructor(socket) {
    this.socket = socket;
    this.buf = Buffer.alloc(0);
    this.fragments = null;
    this.fragOpcode = 0;
    this.room = null;
    this.id = 0;
    this.alive = true;
    this.closed = false;
  }

  sendFrame(opcode, payload) {
    if (this.closed || this.socket.destroyed) return;
    this.socket.write(encodeFrame(opcode, payload));
  }

  sendJson(obj) {
    this.sendFrame(0x1, Buffer.from(JSON.stringify(obj), 'utf8'));
  }

  // pacotes nao confiaveis (posicoes) sao descartados se o cliente estiver atrasado
  sendData(buf, mode) {
    if (this.closed) return;
    const pending = this.socket.writableLength;
    if (pending > SLOW_CLIENT_BYTES) {
      this.close(1008, 'conexao lenta demais');
      return;
    }
    if (mode !== MODE_RELIABLE && pending > DROP_UNRELIABLE_BYTES) return;
    this.sendFrame(0x2, buf);
  }

  close(code = 1000, reason = '') {
    if (this.closed) return;
    const r = Buffer.from(reason, 'utf8').subarray(0, 120);
    const payload = Buffer.alloc(2 + r.length);
    payload.writeUInt16BE(code, 0);
    r.copy(payload, 2);
    this.sendFrame(0x8, payload);
    this.closed = true;
    this.socket.end();
    setTimeout(() => this.socket.destroy(), 2000).unref();
  }

  onData(chunk) {
    this.buf = this.buf.length ? Buffer.concat([this.buf, chunk]) : chunk;
    for (;;) {
      const buf = this.buf;
      if (buf.length < 2) return;
      const fin = (buf[0] & 0x80) !== 0;
      const opcode = buf[0] & 0x0f;
      const masked = (buf[1] & 0x80) !== 0;
      let len = buf[1] & 0x7f;
      let off = 2;
      if (len === 126) {
        if (buf.length < 4) return;
        len = buf.readUInt16BE(2);
        off = 4;
      } else if (len === 127) {
        if (buf.length < 10) return;
        const big = buf.readBigUInt64BE(2);
        if (big > BigInt(MAX_MESSAGE)) return this.close(1009, 'mensagem grande demais');
        len = Number(big);
        off = 10;
      }
      if (len > MAX_MESSAGE) return this.close(1009, 'mensagem grande demais');
      if (!masked) return this.close(1002, 'frame sem mascara');
      if (buf.length < off + 4 + len) return;
      const mask = buf.subarray(off, off + 4);
      off += 4;
      const payload = Buffer.from(buf.subarray(off, off + len));
      for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i & 3];
      this.buf = buf.subarray(off + len);
      this.onFrame(fin, opcode, payload);
      if (this.closed) return;
    }
  }

  onFrame(fin, opcode, payload) {
    switch (opcode) {
      case 0x8: // close
        this.close(1000, '');
        return;
      case 0x9: // ping
        this.sendFrame(0xa, payload);
        return;
      case 0xa: // pong
        this.alive = true;
        return;
      case 0x0: // continuacao
        if (!this.fragments) return this.close(1002, 'continuacao inesperada');
        this.fragments.push(payload);
        if (Buffer.concat(this.fragments).length > MAX_MESSAGE) return this.close(1009, 'mensagem grande demais');
        if (fin) {
          const whole = Buffer.concat(this.fragments);
          this.fragments = null;
          this.onMessage(this.fragOpcode, whole);
        }
        return;
      case 0x1:
      case 0x2:
        if (!fin) {
          this.fragments = [payload];
          this.fragOpcode = opcode;
          return;
        }
        this.onMessage(opcode, payload);
        return;
      default:
        this.close(1002, 'opcode desconhecido');
    }
  }

  onMessage(opcode, payload) {
    this.alive = true;
    if (opcode === 0x1) {
      let msg;
      try {
        msg = JSON.parse(payload.toString('utf8'));
      } catch (e) {
        return this.fail('mensagem invalida');
      }
      this.onControl(msg || {});
      return;
    }
    // binario: repassa dentro da sala
    const room = this.room;
    if (!room || payload.length < 6) return;
    const target = payload.readInt32LE(0);
    const mode = payload[5];
    payload.writeInt32LE(this.id, 0);
    if (target > 0) {
      const dst = room.peers.get(target);
      if (dst && dst !== this) dst.sendData(payload, mode);
      return;
    }
    const except = target < 0 ? -target : 0;
    for (const [id, dst] of room.peers) {
      if (dst !== this && id !== except) dst.sendData(payload, mode);
    }
  }

  fail(msg) {
    this.sendJson({ op: 'error', msg });
    this.close(1008, msg);
  }

  onControl(msg) {
    switch (msg.op) {
      case 'create':
        return this.createRoom(msg);
      case 'join':
        return this.joinRoom(msg);
      case 'kick':
        if (this.room && this.id === 1) {
          const dst = this.room.peers.get(parseInt(msg.id, 10));
          if (dst && dst !== this) dst.fail('Voce foi removido da sala');
        }
        return;
      default:
        return;
    }
  }

  createRoom(msg) {
    if (this.room) return;
    if (parseInt(msg.v, 10) !== PROTOCOL) return this.fail('Versao do jogo diferente da do servidor. Atualize o jogo.');
    if (rooms.size >= MAX_ROOMS) return this.fail('Servidor cheio, tente mais tarde');
    let code = String(msg.code || '').toUpperCase();
    if (code && (!/^[A-Z0-9]{4,8}$/.test(code) || rooms.has(code))) code = '';
    while (!code) {
      let c = '';
      for (let i = 0; i < 5; i++) c += CODE_CHARS[crypto.randomInt(CODE_CHARS.length)];
      if (!rooms.has(c)) code = c;
    }
    const max = Math.max(2, Math.min(MAX_PLAYERS_HARD, parseInt(msg.max, 10) || 8));
    const room = { code, max, host: this, peers: new Map() };
    this.id = 1;
    this.room = room;
    room.peers.set(1, this);
    rooms.set(code, room);
    this.sendJson({ op: 'created', code, id: 1 });
    log(`sala ${code} criada (${rooms.size} salas)`);
  }

  joinRoom(msg) {
    if (this.room) return;
    if (parseInt(msg.v, 10) !== PROTOCOL) return this.fail('Versao do jogo diferente da do servidor. Atualize o jogo.');
    const code = String(msg.code || '').toUpperCase().trim();
    const room = rooms.get(code);
    if (!room) return this.fail(`Sala ${code} nao encontrada`);
    if (room.peers.size >= room.max) return this.fail('Sala cheia');
    let id = 0;
    while (id < 2 || room.peers.has(id)) id = crypto.randomInt(2, 0x7fffffff);
    const others = [...room.peers.keys()];
    this.id = id;
    this.room = room;
    room.peers.set(id, this);
    this.sendJson({ op: 'joined', code, id, peers: others });
    for (const [, dst] of room.peers) if (dst !== this) dst.sendJson({ op: 'peer_join', id });
    log(`sala ${code}: entrou ${id} (${room.peers.size}/${room.max})`);
  }

  onClose() {
    if (this.closedHandled) return;
    this.closedHandled = true;
    this.closed = true;
    clients.delete(this);
    const room = this.room;
    if (!room) return;
    room.peers.delete(this.id);
    if (room.host === this) {
      for (const [, dst] of room.peers) {
        dst.sendJson({ op: 'room_closed' });
        dst.close(1000, 'host saiu');
      }
      rooms.delete(room.code);
      log(`sala ${room.code} fechada (${rooms.size} salas)`);
    } else {
      for (const [, dst] of room.peers) dst.sendJson({ op: 'peer_leave', id: this.id });
      log(`sala ${room.code}: saiu ${this.id} (${room.peers.size}/${room.max})`);
    }
  }
}

const server = http.createServer((req, res) => {
  let players = 0;
  for (const r of rooms.values()) players += r.peers.size;
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('ok');
    return;
  }
  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end(`ASCII Realms - servidor de salas\nsalas: ${rooms.size}\njogadores: ${players}\n`);
});

server.on('upgrade', (req, socket, head) => {
  const key = req.headers['sec-websocket-key'];
  if (!key || String(req.headers.upgrade || '').toLowerCase() !== 'websocket') {
    socket.destroy();
    return;
  }
  const accept = crypto.createHash('sha1').update(key + WS_GUID).digest('base64');
  socket.write(
    'HTTP/1.1 101 Switching Protocols\r\n' +
      'Upgrade: websocket\r\n' +
      'Connection: Upgrade\r\n' +
      `Sec-WebSocket-Accept: ${accept}\r\n\r\n`
  );
  socket.setNoDelay(true);
  const c = new Client(socket);
  clients.add(c);
  socket.on('data', (d) => c.onData(d));
  socket.on('close', () => c.onClose());
  socket.on('end', () => c.onClose());
  socket.on('error', () => c.onClose());
  if (head && head.length) c.onData(head);
});

setInterval(() => {
  for (const c of clients) {
    if (!c.alive) {
      c.socket.destroy();
      c.onClose();
      continue;
    }
    c.alive = false;
    c.sendFrame(0x9, Buffer.alloc(0));
  }
}, HEARTBEAT_MS).unref();

server.listen(PORT, '0.0.0.0', () => {
  log(`ASCII Realms relay ouvindo na porta ${PORT}`);
});

function shutdown() {
  log('desligando...');
  for (const c of clients) c.close(1001, 'servidor desligando');
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 1500).unref();
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
