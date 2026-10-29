const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = process.env.PORT || 8080;

// HTTP Server serving client.html
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

wss.on('connection', (ws) => {
  ws.hostId = null;
  ws.role = null;

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

    // JSON control & input messages
    try {
      const data = JSON.parse(message.toString());

      // 1. Host Registration with User ID and Password
      if (data.type === 'register-host') {
        const { hostId, password } = data;
        if (!hostId || !password) {
          return ws.send(JSON.stringify({ type: 'error', message: 'Missing Host ID or Password' }));
        }

        ws.hostId = hostId.replace(/\s+/g, '');
        ws.role = 'host';
        hosts.set(ws.hostId, { hostWs: ws, clientWs: null, password: password.toString() });

        ws.send(JSON.stringify({ type: 'host-registered', hostId: ws.hostId }));
        console.log(`[+] Host registered: ID=${ws.hostId}`);
      }

      // 2. Client Connection Request with User ID and Password
      else if (data.type === 'join') {
        const targetId = (data.hostId || '').replace(/\s+/g, '');
        const providedPassword = (data.password || '').toString();

        const session = hosts.get(targetId);

        if (!session || !session.hostWs || session.hostWs.readyState !== WebSocket.OPEN) {
          return ws.send(JSON.stringify({
            type: 'auth-error',
            message: 'User ID offline hai ya exist nahi karta.'
          }));
        }

        // Verify Password
        if (session.password !== providedPassword) {
          return ws.send(JSON.stringify({
            type: 'auth-error',
            message: 'Galat Password! (Incorrect Password)'
          }));
        }

        // Password matched!
        ws.hostId = targetId;
        ws.role = 'client';
        session.clientWs = ws;

        ws.send(JSON.stringify({ type: 'joined', hostId: targetId }));
        session.hostWs.send(JSON.stringify({ type: 'client-connected' }));
        console.log(`[+] Client connected to Host ID: ${targetId}`);
      }

      // 3. Remote input events (Client -> Host)
      else if (ws.role === 'client' && ws.hostId) {
        const session = hosts.get(ws.hostId);
        if (session && session.hostWs && session.hostWs.readyState === WebSocket.OPEN) {
          session.hostWs.send(message.toString());
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
  console.log(`Signaling Server running on port ${PORT}`);
});
