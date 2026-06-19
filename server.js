// =============================================
// server.js - Ultimate Multiplayer Friend System
// =============================================
const WebSocket = require('ws');
const http = require('http');

// ========== Server Configuration ==========
const PORT = process.env.PORT || 10000;
const server = new WebSocket.Server({ port: PORT });

// ========== Data Storage ==========
const players = new Map();
const pendingRequests = new Map();
const rooms = new Map();

// ========== Stats ==========
const stats = {
    connections: 0,
    totalConnections: 0,
    messagesSent: 0,
    messagesReceived: 0,
    roomsCreated: 0,
    startTime: Date.now()
};

// ========== Logging ==========
function log(message, type = 'info') {
    const timestamp = new Date().toLocaleTimeString();
    console.log(`[${timestamp}] ${message}`);
}

// ========== WebSocket Server ==========
server.on('connection', (ws, req) => {
    let player_id = null;
    let character = null;
    let room_id = null;
    
    stats.connections++;
    stats.totalConnections++;
    log(`🔗 New connection from ${req.socket.remoteAddress}`);
    
    ws.on('message', (data) => {
        try {
            const msg = JSON.parse(data.toString());
            stats.messagesReceived++;
            
            switch (msg.type) {
                // ===== AUTH =====
                case 'auth':
                    player_id = msg.player_id;
                    character = msg.character;
                    
                    if (players.has(player_id)) {
                        const old = players.get(player_id);
                        if (old.ws.readyState === WebSocket.OPEN) {
                            old.ws.close(1000, 'reconnect');
                        }
                    }
                    
                    players.set(player_id, { 
                        ws: ws, 
                        online: true, 
                        character: character,
                        room: null
                    });
                    
                    ws.send(JSON.stringify({ 
                        type: 'auth_ok', 
                        player_id: player_id,
                        character: character 
                    }));
                    
                    log(`✅ ${player_id} authenticated as ${character}`);
                    
                    for (const [id, player] of players) {
                        if (id !== player_id && player.online) {
                            player.ws.send(JSON.stringify({
                                type: 'friend_online',
                                player_id: player_id,
                                character: character
                            }));
                        }
                    }
                    break;
                
                // ===== FRIEND REQUEST =====
                case 'friend_request':
                    const to_id = msg.to_id;
                    const from_id = msg.from_id;
                    const target = players.get(to_id);
                    
                    if (!target || !target.online) {
                        ws.send(JSON.stringify({
                            type: 'error',
                            message: 'Friend is offline or does not exist'
                        }));
                        log(`❌ Request to ${to_id} failed (offline)`);
                        break;
                    }
                    
                    pendingRequests.set(from_id, to_id);
                    
                    target.ws.send(JSON.stringify({
                        type: 'friend_request',
                        from_id: from_id,
                        character: msg.character
                    }));
                    
                    ws.send(JSON.stringify({
                        type: 'friend_request_sent',
                        to_id: to_id
                    }));
                    
                    log(`📩 Request from ${from_id} to ${to_id}`);
                    break;
                
                // ===== ACCEPT REQUEST =====
                case 'friend_request_accepted':
                    const accept_from = msg.from_id;
                    const accept_to = msg.to_id;
                    const accept_character = msg.character;
                    
                    room_id = 'room_' + Math.random().toString(36).substring(2, 8);
                    
                    const from_player = players.get(accept_from);
                    const to_player = players.get(accept_to);
                    
                    if (!rooms.has(room_id)) {
                        rooms.set(room_id, new Set());
                    }
                    rooms.get(room_id).add(accept_from);
                    rooms.get(room_id).add(accept_to);
                    stats.roomsCreated++;
                    
                    if (from_player && from_player.online) {
                        from_player.room = room_id;
                        from_player.ws.send(JSON.stringify({
                            type: 'friend_request_accepted',
                            from_id: accept_to,
                            to_id: accept_from,
                            character: accept_character,
                            room_id: room_id
                        }));
                    }
                    
                    if (to_player && to_player.online) {
                        to_player.room = room_id;
                        to_player.ws.send(JSON.stringify({
                            type: 'friend_request_accepted',
                            from_id: accept_from,
                            to_id: accept_to,
                            character: to_player.character || 'King',
                            room_id: room_id
                        }));
                    }
                    
                    pendingRequests.delete(accept_from);
                    log(`🎮 Room ${room_id} created for ${accept_from} ↔ ${accept_to}`);
                    break;
                
                // ===== REJECT REQUEST =====
                case 'friend_request_rejected':
                    const reject_from = msg.from_id;
                    const reject_to = msg.to_id;
                    
                    const reject_target = players.get(reject_from);
                    if (reject_target && reject_target.online) {
                        reject_target.ws.send(JSON.stringify({
                            type: 'friend_request_rejected',
                            from_id: reject_to
                        }));
                    }
                    
                    pendingRequests.delete(reject_from);
                    log(`❌ Request from ${reject_from} rejected`);
                    break;
                
                // ===== PING =====
                case 'ping':
                    ws.send(JSON.stringify({ type: 'pong' }));
                    stats.messagesSent++;
                    break;
                
                // ===== DISCONNECT =====
                case 'disconnect':
                    log(`🔌 ${player_id || 'unknown'} requested disconnect`);
                    ws.close(1000, 'client_disconnect');
                    break;
                
                default:
                    log(`⚠️ Unknown message type: ${msg.type}`);
                    ws.send(JSON.stringify({
                        type: 'error',
                        message: 'Invalid message type'
                    }));
            }
            
        } catch (error) {
            log(`❌ Error processing message: ${error.message}`);
            ws.send(JSON.stringify({
                type: 'error',
                message: 'Error processing message'
            }));
        }
    });
    
    // ========== Connection Close ==========
    ws.on('close', (code, reason) => {
        stats.connections--;
        
        if (player_id) {
            const player = players.get(player_id);
            if (player) {
                player.online = false;
                
                if (player.room && rooms.has(player.room)) {
                    const room = rooms.get(player.room);
                    room.delete(player_id);
                    if (room.size === 0) {
                        rooms.delete(player.room);
                        log(`🗑️ Room ${player.room} removed (empty)`);
                    }
                }
                
                for (const [id, p] of players) {
                    if (id !== player_id && p.online) {
                        p.ws.send(JSON.stringify({
                            type: 'friend_offline',
                            player_id: player_id
                        }));
                    }
                }
            }
            log(`❌ ${player_id} disconnected | Code: ${code}`);
        }
    });
    
    ws.on('error', (error) => {
        log(`❌ WebSocket error: ${error.message}`);
    });
});

// ========== Keep Connections Alive ==========
setInterval(() => {
    for (const [id, player] of players) {
        if (player.online && player.ws.readyState === WebSocket.OPEN) {
            player.ws.ping();
        }
    }
}, 30000);

// ========== Stats Display ==========
setInterval(() => {
    const uptime = Math.floor((Date.now() - stats.startTime) / 1000);
    log(`📊 Stats | Connections: ${stats.connections} | Rooms: ${rooms.size} | Messages: ${stats.messagesSent}/${stats.messagesReceived} | Uptime: ${uptime}s`);
}, 60000);

// ========== HTTP Health Check ==========
const httpServer = http.createServer((req, res) => {
    if (req.url === '/health' || req.url === '/') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
            status: 'online',
            connections: stats.connections,
            rooms: rooms.size,
            messagesSent: stats.messagesSent,
            messagesReceived: stats.messagesReceived,
            uptime: Math.floor((Date.now() - stats.startTime) / 1000),
            timestamp: Date.now()
        }));
    } else {
        res.writeHead(404);
        res.end('Not Found');
    }
});

const HTTP_PORT = parseInt(PORT) + 1;
httpServer.listen(HTTP_PORT, () => {
    log(`📊 HTTP health check on port ${HTTP_PORT}`);
});

// ========== Server Ready ==========
log(`🚀 WebSocket server running on port ${PORT}`);
log(`📡 Connect with: wss://your-app-name.onrender.com`);
log(`👥 Max 2 players per room`);

// ========== Graceful Shutdown ==========
process.on('SIGINT', () => {
    log('🛑 Shutting down...');
    
    for (const [id, player] of players) {
        if (player.ws.readyState === WebSocket.OPEN) {
            player.ws.close(1000, 'server_shutdown');
        }
    }
    
    httpServer.close();
    server.close(() => {
        log('✅ Server closed');
        process.exit(0);
    });
});