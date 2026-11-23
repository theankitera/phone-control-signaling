#!/usr/bin/env node
/**
 * signaling_server.js — WebSocket signaling server for phone-control.
 *
 * What it does
 * ------------
 * WebRTC peers cannot find each other on the open internet by themselves.
 * This server is the meeting point: a Host phone and a Viewer phone both
 * open a WebSocket here and join the same "room" using a short code
 * (e.g. "482913"). The server then relays the WebRTC handshake messages
 * (SDP offer/answer and ICE candidates) between the two peers in the room.
 * Once the handshake completes, audio/video/data flow directly peer-to-peer
 * and this server is no longer in the media path.
 *
 * Protocol (all messages are JSON text frames)
 * --------------------------------------------
 * Client -> Server:
 *   { "type": "join",  "code": "482913", "role": "host" | "viewer" }
 *   { "type": "offer",  "sdp": "..." }                      (relayed)
 *   { "type": "answer", "sdp": "..." }                      (relayed)
 *   { "type": "ice",    "candidate": {...} }                (relayed)
 *   { "type": "bye" }                                       (relayed)
 *   { "type": "ping" }                                      (keep-alive)
 *
 * Server -> Client:
 *   { "type": "joined", "code": "...", "role": "...", "peerPresent": bool }
 *   { "type": "peer-joined", "role": "host" | "viewer" }
 *   { "type": "peer-left",   "role": "host" | "viewer" }
 *   { "type": "error", "message": "..." }
 *   { "type": "pong" }
 *   ...plus relayed offer/answer/ice/bye messages, tagged with "from".
 *
 * Rules:
 *   - A room holds at most 2 peers: one "host" and one "viewer".
 *   - Joining a room with a role that is already taken is rejected.
 *   - A client may join exactly one room.
 *   - There is no database and no authentication in v1 — the room code is
 *     the shared secret. Use a fresh, unguessable code per session
 *     (the app generates a random 6-digit code).
 *
 * Run:  PORT=8080 node signaling_server.js
 */

'use strict';

const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = process.env.PORT || 8080;
const MAX_PEERS_PER_ROOM = 2;

/** @type {Map<string, Map<string, WebSocket>>} code -> (role -> socket) */
const rooms = new Map();

function send(ws, obj) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(obj));
  }
}

function getRoom(code) {
  let room = rooms.get(code);
  if (!room) {
    room = new Map();
    rooms.set(code, room);
  }
  return room;
}

function otherRole(role) {
  return role === 'host' ? 'viewer' : 'host';
}

function relayToPeer(ws, msg) {
  const room = rooms.get(ws.roomCode);
  if (!room) return;
  const peer = room.get(otherRole(ws.role));
  if (peer && peer.readyState === WebSocket.OPEN) {
    send(peer, { ...msg, from: ws.role });
  }
}

function leaveRoom(ws) {
  if (!ws.roomCode) return;
  const room = rooms.get(ws.roomCode);
  if (room) {
    if (room.get(ws.role) === ws) room.delete(ws.role);
    const peer = room.get(otherRole(ws.role));
    if (peer) send(peer, { type: 'peer-left', role: ws.role });
    if (room.size === 0) rooms.delete(ws.roomCode);
  }
  ws.roomCode = null;
  ws.role = null;
}

function handleJoin(ws, msg) {
  const code = String(msg.code || '').trim();
  const role = msg.role;

  if (!code || code.length < 4 || code.length > 12) {
    return send(ws, { type: 'error', message: 'Invalid room code.' });
  }
  if (role !== 'host' && role !== 'viewer') {
    return send(ws, { type: 'error', message: 'Role must be "host" or "viewer".' });
  }
  if (ws.roomCode) {
    return send(ws, { type: 'error', message: 'Already joined a room.' });
  }

  const room = getRoom(code);
  if (room.size >= MAX_PEERS_PER_ROOM) {
    return send(ws, { type: 'error', message: 'Room is full.' });
  }
  if (room.has(role)) {
    return send(ws, { type: 'error', message: `Room already has a ${role}.` });
  }

  ws.roomCode = code;
  ws.role = role;
  room.set(role, ws);

  const peer = room.get(otherRole(role));
  send(ws, { type: 'joined', code, role, peerPresent: Boolean(peer) });
  if (peer) send(peer, { type: 'peer-joined', role });
  console.log(`[join] ${role} joined room ${code} (peers: ${room.size})`);
}

function handleMessage(ws, raw) {
  let msg;
  try {
    msg = JSON.parse(raw);
  } catch {
    return send(ws, { type: 'error', message: 'Message is not valid JSON.' });
  }
  if (!msg || typeof msg.type !== 'string') {
    return send(ws, { type: 'error', message: 'Message has no "type".' });
  }

  switch (msg.type) {
    case 'join':
      handleJoin(ws, msg);
      break;
    case 'ping':
      send(ws, { type: 'pong' });
      break;
    case 'offer':
    case 'answer':
    case 'ice':
    case 'bye':
      if (!ws.roomCode) {
        return send(ws, { type: 'error', message: 'Join a room first.' });
      }
      relayToPeer(ws, msg);
      break;
    default:
      send(ws, { type: 'error', message: `Unknown message type: ${msg.type}` });
  }
}

// A plain HTTP endpoint for health checks (Render/Railway ping this).
const httpServer = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, rooms: rooms.size }));
  } else {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('phone-control signaling server\n');
  }
});

const wss = new WebSocketServer({ server: httpServer });

wss.on('connection', (ws, req) => {
  ws.isAlive = true;
  ws.roomCode = null;
  ws.role = null;
  console.log(`[connect] ${req.socket.remoteAddress}`);

  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', (data) => handleMessage(ws, data.toString()));
  ws.on('close', () => {
    console.log(`[disconnect] role=${ws.role} room=${ws.roomCode}`);
    leaveRoom(ws);
  });
  ws.on('error', (err) => {
    console.error('[ws error]', err.message);
    leaveRoom(ws);
  });
});

// Drop dead sockets (e.g. phones that lost network without a clean close).
const heartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) {
      leaveRoom(ws);
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    ws.ping();
  }
}, 30000);
wss.on('close', () => clearInterval(heartbeat));

httpServer.listen(PORT, () => {
  console.log(`phone-control signaling server listening on port ${PORT}`);
});
