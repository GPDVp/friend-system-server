// =============================================
// server.js - Multiplayer Friend System Server
// =============================================
const WebSocket = require('ws');

// ========== Server Configuration ==========
const PORT = process.env.PORT || 10000;
const server = new WebSocket.Server({ port: PORT });

// ========== Data Storage ==========
const players = new Map();          // player_id -> { ws, online, character }
const pendingRequests = new Map();  // from_id -> to_id
const rooms = new Map();            // room_id -> Set of player_ids

// ========== Stats ==========
let stats = {
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
    const colors = {
        info: '\x1b[36m',
        success: '\x1b[32m',
        error: '\x1b[31m',
        warn: '\x1b[33m',
        reset: '\x1b[0m'
    };
    console.log(`${colors[type]}[${timestamp}]${colors.reset} ${message}`);
}

// ========== Generate Room ID ==========
function generateRoomId() {
    return 'room_' + Math.random().toString(36).substring(2, 8);
}

// ========== WebSocket Server ==========
server.on('connection', (ws, req) => {
    let player_id = null;
    let character = null;
    let room_id = null;
    
    stats.connections++;
    stats.totalConnections++;
    log(`🔗 New connection from ${req.socket.remoteAddress}`, 'info');
    
    // ========== Message Handler ==========
    ws.on('message', (data) => {
        try {
            const msg = JSON.parse(data.toString());
            stats.messagesReceived++;
            log(`📨 Received: ${msg.type} from ${player_id || 'unknown'}`, 'info');
            
            switch (msg.type) {
                // ===== AUTH =====
                case 'auth':
                    player_id = msg.player_id;
                    character = msg.character;
                    
                    // Remove old connection if exists
                    if (players.has(player_id)) {
                        const old = players.get(player_id);
                        if (old.ws.readyState === WebSocket.OPEN) {
                            old.ws.close(1000, 'reconnect');
                        }
                    }
                    
                    players.set(player_id, { 
                        ws: ws, 
                        online: true, 
                        character: character 
                    });
                    
                    ws.send(JSON.stringify({ 
                        type: 'auth_ok', 
                        player_id: player_id,
                        character: character 
                    }));
                    
                    log(`✅ ${player_id} authenticated as ${character}`, 'success');
                    
                    // Notify all other players that this player is online
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
                        log(`❌ Request to ${to_id} failed (offline)`, 'error');
                        break;
                    }
                    
                    pendingRequests.set(from_id, to_id);
                    
                    target.ws.send(JSON.stringify({
                        type: 'friend_request',
                        from_id: from_id,
                        character: msg.character
                    }));
                    
                    ws.send(JSON.stringify({
                        type: 'friend_online',
                        player_id: to_id,
                        character: target.character
                    }));
                    
                    log(`📩 Request from ${from_id} to ${to_id}`, 'info');
                    break;
                
                // ===== ACCEPT REQUEST =====
                case 'friend_request_accepted':
                    const accept_from = msg.from_id;
                    const accept_to = msg.to_id;
                    const accept_character = msg.character;
                    
                    // Create room
                    room_id = generateRoomId();
                    
                    const from_player = players.get(accept_from);
                    const to_player = players.get(accept_to);
                    
                    if (!rooms.has(room_id)) {
                        rooms.set(room_id, new Set());
                    }
                    rooms.get(room_id).add(accept_from);
                    rooms.get(room_id).add(accept_to);
                    stats.roomsCreated++;
                    
                    if (from_player && from_player.online) {
                        from_player.ws.send(JSON.stringify({
                            type: 'friend_request_accepted',
                            from_id: accept_to,
                            to_id: accept_from,
                            character: accept_character,
                            room_id: room_id
                        }));
                    }
                    
                    if (to_player && to_player.online) {
                        to_player.ws.send(JSON.stringify({
                            type: 'friend_request_accepted',
                            from_id: accept_from,
                            to_id: accept_to,
                            character: to_player.character || 'King',
                            room_id: room_id
                        }));
                    }
                    
                    pendingRequests.delete(accept_from);
                    log(`🎮 Room ${room_id} created for ${accept_from} ↔ ${accept_to}`, 'success');
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
                    log(`❌ Request from ${reject_from} to ${reject_to} rejected`, 'warn');
                    break;
                
                // ===== PING =====
                case 'ping':
                    ws.send(JSON.stringify({ type: 'pong' }));
                    stats.messagesSent++;
                    break;
                
                // ===== DISCONNECT =====
                case 'disconnect':
                    log(`🔌 ${player_id || 'unknown'} requested disconnect`, 'warn');
                    ws.close(1000, 'client_disconnect');
                    break;
                
                default:
                    log(`⚠️ Unknown message type: ${msg.type}`, 'warn');
                    ws.send(JSON.stringify({
                        type: 'error',
                        message: 'Invalid message type'
                    }));
            }
            
        } catch (error) {
            log(`❌ Error processing message: ${error.message}`, 'error');
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
                
                // Remove from room
                if (room_id && rooms.has(room_id)) {
                    const room = rooms.get(room_id);
                    room.delete(player_id);
                    if (room.size === 0) {
                        rooms.delete(room_id);
                        log(`🗑️ Room ${room_id} removed (empty)`, 'info');
                    }
                }
                
                // Notify friends
                for (const [id, p] of players) {
                    if (id !== player_id && p.online) {
                        p.ws.send(JSON.stringify({
                            type: 'friend_offline',
                            player_id: player_id
                        }));
                    }
                }
            }
            log(`❌ ${player_id} disconnected | Code: ${code}`, 'error');
        }
    });
    
    ws.on('error', (error) => {
        log(`❌ WebSocket error: ${error.message}`, 'error');
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
    const hours = Math.floor(uptime / 3600);
    const minutes = Math.floor((uptime % 3600) / 60);
    const seconds = uptime % 60;
    
    log(`📊 Stats | Connections: ${stats.connections} | Rooms: ${rooms.size} | Messages: ${stats.messagesSent}/${stats.messagesReceived} | Uptime: ${hours}h ${minutes}m ${seconds}s`, 'info');
}, 60000);

// ========== Server Ready ==========
log(`🚀 WebSocket server running on port ${PORT}`, 'success');
log(`📡 Connect with: wss://friend-system-server-2.onrender.com`, 'info');
log(`👥 Max 2 players per room`, 'info');

// ========== Graceful Shutdown ==========
process.on('SIGINT', () => {
    log('🛑 Shutting down...', 'warn');
    
    for (const [id, player] of players) {
        if (player.ws.readyState === WebSocket.OPEN) {
            player.ws.close(1000, 'server_shutdown');
        }
    }
    
    server.close(() => {
        log('✅ Server closed', 'success');
        process.exit(0);
    });
});