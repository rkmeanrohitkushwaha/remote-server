const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer, WebSocket } = require('ws');

// Use port provided by Cloud provider (Render, Railway, etc.) or default to 8080
const PORT = process.env.PORT || 8080;

// 1. HTTP Server to serve client.html
const server = http.createServer((req, res) => {
  if (req.url === '/' || req.url === '/client.html') {
    const filePath = path.join(__dirname, 'client.html');
    fs.readFile(filePath, (err, data) => {
      if (err) {
        res.writeHead(500, { 'Content-Type': 'text/plain' });
        return res.end('Error loading client.html');
      }
      res.writeHead(200, { 'Content-Type': 'text/html' });
      res.end(data);
    });
  } else {
    res.writeHead(404, { 'Content-Type': 'text/plain' });
    res.end('Not Found');
  }
});

// 2. WebSocket Server on top of HTTP
const wss = new WebSocketServer({ server });

// Map of sessionId -> { hostWs, clientWs }
const sessions = new Map();

function generateSessionId() {
  let id;
  do {
    id = Math.floor(100000 + Math.random() * 900000).toString();
  } while (sessions.has(id));
  return id;
}

wss.on('connection', (ws) => {
  ws.sessionId = null;
  ws.role = null;

  ws.on('message', (message, isBinary) => {
    // Binary messages are JPEG frames sent from Host -> Client
    if (isBinary) {
      if (ws.role === 'host' && ws.sessionId) {
        const session = sessions.get(ws.sessionId);
        if (session && session.clientWs && session.clientWs.readyState === WebSocket.OPEN) {
          session.clientWs.send(message, { binary: true });
        }
      }
      return;
    }

    // JSON control & input messages
    try {
      const data = JSON.parse(message.toString());

      // Host registers
      if (data.type === 'register-host') {
        const sessionId = generateSessionId();
        ws.sessionId = sessionId;
        ws.role = 'host';
        sessions.set(sessionId, { hostWs: ws, clientWs: null });

        ws.send(JSON.stringify({ type: 'host-registered', sessionId }));
        console.log(`[+] Host registered with Session ID: ${sessionId}`);
      }

      // Client joins using 6-digit code
      else if (data.type === 'join') {
        const { sessionId } = data;
        const session = sessions.get(sessionId);

        if (!session || !session.hostWs || session.hostWs.readyState !== WebSocket.OPEN) {
          return ws.send(JSON.stringify({
            type: 'error',
            message: 'Session ID not found or Host is offline.'
          }));
        }

        ws.sessionId = sessionId;
        ws.role = 'client';
        session.clientWs = ws;

        ws.send(JSON.stringify({ type: 'joined', sessionId }));
        session.hostWs.send(JSON.stringify({ type: 'client-connected' }));
        console.log(`[+] Client connected to Session ID: ${sessionId}`);
      }

      // Input events from Client -> Host
      else if (ws.role === 'client' && ws.sessionId) {
        const session = sessions.get(ws.sessionId);
        if (session && session.hostWs && session.hostWs.readyState === WebSocket.OPEN) {
          session.hostWs.send(message.toString());
        }
      }
    } catch (err) {
      console.error('Error handling message:', err.message);
    }
  });

  ws.on('close', () => {
    if (!ws.sessionId) return;
    const session = sessions.get(ws.sessionId);

    if (ws.role === 'host') {
      console.log(`[-] Host disconnected (Session ${ws.sessionId})`);
      if (session && session.clientWs && session.clientWs.readyState === WebSocket.OPEN) {
        session.clientWs.send(JSON.stringify({ type: 'host-disconnected' }));
      }
      sessions.delete(ws.sessionId);
    } else if (ws.role === 'client') {
      console.log(`[-] Client disconnected from Session ${ws.sessionId}`);
      if (session && session.hostWs && session.hostWs.readyState === WebSocket.OPEN) {
        session.hostWs.send(JSON.stringify({ type: 'client-disconnected' }));
        session.clientWs = null;
      }
    }
  });
});

server.listen(PORT, () => {
  console.log(`===============================================`);
  console.log(` Signaling Server running on port: ${PORT}`);
  console.log(` Ready for Cloud & Worldwide connections!`);
  console.log(`===============================================`);
});
