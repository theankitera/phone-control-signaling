// worker.js — phone-control signaling server on Cloudflare Workers.
//
// Same wire protocol as server/signaling_server.js (the Node version), so
// the app needs no protocol changes — only the server URL changes.
//
// One global Durable Object holds every room. Room membership is derived
// from the accepted WebSockets' attachments, so the object can hibernate
// freely without losing track of who is in which room.

export class SignalingHub {
  constructor(ctx, env) {
    this.ctx = ctx;
  }

  async fetch(request) {
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    this.ctx.acceptWebSocket(server);
    server.serializeAttachment({ code: null, role: null });
    return new Response(null, { status: 101, webSocket: client });
  }

  peersInRoom(code) {
    return this.ctx.getWebSockets().filter((ws) => {
      const a = ws.deserializeAttachment();
      return a && a.code === code;
    });
  }

  peerOf(ws) {
    const me = ws.deserializeAttachment();
    if (!me || !me.code) return null;
    return (
      this.peersInRoom(me.code).find((other) => {
        const a = other.deserializeAttachment();
        return other !== ws && a && a.role && a.role !== me.role;
      }) || null
    );
  }

  send(ws, obj) {
    try {
      if (ws.readyState === 1) ws.send(JSON.stringify(obj));
    } catch (_) {
      // Socket already gone; close handler does the cleanup.
    }
  }

  leaveRoom(ws) {
    const me = ws.deserializeAttachment();
    if (!me || !me.code) return;
    const peer = this.peerOf(ws);
    if (peer) this.send(peer, { type: "peer-left", role: me.role });
    try {
      ws.serializeAttachment({ code: null, role: null });
    } catch (_) {}
  }

  handleJoin(ws, msg) {
    const code = String(msg.code || "").trim();
    const role = msg.role;
    if (!code || code.length < 4 || code.length > 12) {
      return this.send(ws, { type: "error", message: "Invalid room code." });
    }
    if (role !== "host" && role !== "viewer") {
      return this.send(ws, {
        type: "error",
        message: 'Role must be "host" or "viewer".',
      });
    }
    const me = ws.deserializeAttachment();
    if (me && me.code) {
      return this.send(ws, { type: "error", message: "Already joined a room." });
    }
    const peers = this.peersInRoom(code);
    if (peers.length >= 2) {
      return this.send(ws, { type: "error", message: "Room is full." });
    }
    if (peers.some((p) => (p.deserializeAttachment() || {}).role === role)) {
      return this.send(ws, {
        type: "error",
        message: `Room already has a ${role}.`,
      });
    }
    ws.serializeAttachment({ code, role });
    const peer = this.peerOf(ws);
    this.send(ws, { type: "joined", code, role, peerPresent: Boolean(peer) });
    if (peer) this.send(peer, { type: "peer-joined", role });
  }

  async webSocketMessage(ws, message) {
    let msg;
    try {
      const text =
        typeof message === "string"
          ? message
          : new TextDecoder().decode(message);
      msg = JSON.parse(text);
    } catch {
      return this.send(ws, {
        type: "error",
        message: "Message is not valid JSON.",
      });
    }
    if (!msg || typeof msg.type !== "string") {
      return this.send(ws, { type: "error", message: 'Message has no "type".' });
    }

    switch (msg.type) {
      case "join":
        this.handleJoin(ws, msg);
        break;
      case "ping":
        this.send(ws, { type: "pong" });
        break;
      case "offer":
      case "answer":
      case "ice":
      case "bye": {
        const me = ws.deserializeAttachment();
        if (!me || !me.code) {
          return this.send(ws, {
            type: "error",
            message: "Join a room first.",
          });
        }
        const peer = this.peerOf(ws);
        if (peer) this.send(peer, { ...msg, from: me.role });
        break;
      }
      default:
        this.send(ws, {
          type: "error",
          message: `Unknown message type: ${msg.type}`,
        });
    }
  }

  async webSocketClose(ws) {
    this.leaveRoom(ws);
  }

  async webSocketError(ws) {
    this.leaveRoom(ws);
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/health") {
      return Response.json({ ok: true });
    }
    if ((request.headers.get("Upgrade") || "").toLowerCase() === "websocket") {
      const stub = env.SIGNALING.get(env.SIGNALING.idFromName("global"));
      return stub.fetch(request);
    }
    return new Response("phone-control signaling server\n", {
      headers: { "Content-Type": "text/plain" },
    });
  },
};
