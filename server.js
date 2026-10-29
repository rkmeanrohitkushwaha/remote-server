const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = process.env.PORT || 8080;

// HTTP Server serving AnyDesk client.html
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

// WebSocket Server
const wss = new WebSocketServer({ server });

// Map of hostId -> { hostWs, clientWs, password }
const hosts = new Map();

// 25s Ping/Pong KeepAlive to keep Cloud Proxies (Render) alive 24/7
setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 25000);

wss.on('connection', (ws) => {
  ws.hostId = null;
  ws.role = null;
  ws.isAlive = true;

  ws.on('pong', () => {
    ws.isAlive = true;
  });

  ws.on('message', (message, isBinary) => {
    // Binary screen frame (Host -> Client)
    if (isBinary) {
      if (ws.role === 'host' && ws.hostId) {
        const session = hosts.get(ws.hostId);
        if (session && session.clientWs && session.clientWs.readyState === WebSocket.OPEN) {
          session.clientWs.send(message, { binary: true });
        }
      }
      return;
    }

    // JSON control, chat & input messages
    try {
      const data = JSON.parse(message.toString());

      // 1. Host Registration
      if (data.type === 'register-host') {
        const { hostId, password } = data;
        if (!hostId || !password) return;

        ws.hostId = hostId.replace(/\s+/g, '');
        ws.role = 'host';
        hosts.set(ws.hostId, { hostWs: ws, clientWs: null, password: password.toString() });

        ws.send(JSON.stringify({ type: 'host-registered', hostId: ws.hostId }));
        console.log(`[+] AnyDesk Host registered: ID=${ws.hostId}`);
      }

      // 2. Client Connection Request
      else if (data.type === 'join') {
        const targetId = (data.hostId || '').replace(/\s+/g, '');
        const providedPassword = (data.password || '').toString();

        const session = hosts.get(targetId);

        if (!session || !session.hostWs || session.hostWs.readyState !== WebSocket.OPEN) {
          return ws.send(JSON.stringify({
            type: 'auth-error',
            message: 'AnyDesk Address offline hai ya exist nahi karta.'
          }));
        }

        if (session.password !== providedPassword) {
          return ws.send(JSON.stringify({
            type: 'auth-error',
            message: 'Galat Password! (Incorrect Password)'
          }));
        }

        ws.hostId = targetId;
        ws.role = 'client';
        session.clientWs = ws;

        ws.send(JSON.stringify({ type: 'joined', hostId: targetId }));
        session.hostWs.send(JSON.stringify({ type: 'client-connected' }));
        console.log(`[+] AnyDesk Client joined Host ID: ${targetId}`);
      }

      // 3. Reject Session
      else if (data.type === 'reject-session' && ws.role === 'host') {
        const session = hosts.get(ws.hostId);
        if (session && session.clientWs) {
          session.clientWs.send(JSON.stringify({ type: 'auth-error', message: 'Host ne connection request reject kar di.' }));
          session.clientWs = null;
        }
      }

      // 4. Two-way In-Session Chat & Remote Actions & Mouse/Keyboard
      else if (ws.role === 'client' && ws.hostId) {
        const session = hosts.get(ws.hostId);
        if (session && session.hostWs && session.hostWs.readyState === WebSocket.OPEN) {
          session.hostWs.send(message.toString());
        }
      } else if (ws.role === 'host' && ws.hostId) {
        const session = hosts.get(ws.hostId);
        if (session && session.clientWs && session.clientWs.readyState === WebSocket.OPEN) {
          session.clientWs.send(message.toString());
        }
      }
    } catch (err) {
      console.error('Message error:', err.message);
    }
  });

  ws.on('close', () => {
    if (!ws.hostId) return;
    const session = hosts.get(ws.hostId);

    if (ws.role === 'host') {
      console.log(`[-] Host offline: ${ws.hostId}`);
      if (session && session.clientWs && session.clientWs.readyState === WebSocket.OPEN) {
        session.clientWs.send(JSON.stringify({ type: 'host-disconnected' }));
      }
      hosts.delete(ws.hostId);
    } else if (ws.role === 'client') {
      console.log(`[-] Client disconnected from Host: ${ws.hostId}`);
      if (session && session.hostWs && session.hostWs.readyState === WebSocket.OPEN) {
        session.hostWs.send(JSON.stringify({ type: 'client-disconnected' }));
        session.clientWs = null;
      }
    }
  });
});

server.listen(PORT, () => {
  console.log(`AnyDesk Signaling Server running on port ${PORT}`);
});
