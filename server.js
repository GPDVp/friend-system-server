// =============================================
// server.js - Ultimate Multiplayer Friend System
// =============================================
const WebSocket = require('ws');

// ========== Server Configuration ==========
const PORT = process.env.PORT || 10000;
const server = new WebSocket.Server({ 
    port: PORT,
    perMessageDeflate: true,
    maxPayload: 65536
});

// ========== Data Storage ==========
const players = new Map();
const pendingRequests = new Map();
const rooms = new Map();
const bannedPlayers = new Set();
const playerSessions = new Map();

// ========== Configuration ==========
const CONFIG = {
    MAX_ROOM_SIZE: 2,
    PING_INTERVAL: 15000,
    PONG_TIMEOUT: 30000,
    RECONNECT_GRACE: 5000,
    MAX_REQUESTS_PER_MINUTE: 10,
    REQUEST_COOLDOWN: 2000,
    SESSION_TIMEOUT: 300000,
    MAX_PLAYERS: 1000,
    RATE_LIMIT_WINDOW: 60000
};

// ========== Stats ==========
const stats = {
    connections: 0,
    totalConnections: 0,
    messagesSent: 0,
    messagesReceived: 0,
    roomsCreated: 0,
    roomsDestroyed: 0,
    errors: 0,
    startTime: Date.now(),
    peakConnections: 0,
    totalRequests: 0,
    acceptedRequests: 0,
    rejectedRequests: 0
};

// ========== Rate Limiting ==========
const requestCounts = new Map();

function isRateLimited(playerId) {
    const now = Date.now();
    const data = requestCounts.get(playerId);
    if (!data) {
        requestCounts.set(playerId, { count: 1, timestamp: now });
        return false;
    }
    if (now - data.timestamp > CONFIG.RATE_LIMIT_WINDOW) {
        requestCounts.set(playerId, { count: 1, timestamp: now });
        return false;
    }
    if (data.count >= CONFIG.MAX_REQUESTS_PER_MINUTE) {
        return true;
    }
    data.count++;
    return false;
}

// ========== Logging ==========
function log(message, type = 'info') {
    const timestamp = new Date().toLocaleString('fa-IR');
    const colors = {
        info: '\x1b[36m',
        success: '\x1b[32m',
        error: '\x1b[31m',
        warn: '\x1b[33m',
        debug: '\x1b[90m'
    };
    console.log(`${colors[type]}[${timestamp}]${colors.reset} ${message}`);
}

function logError(message, error) {
    stats.errors++;
    log(`${message}: ${error.message}`, 'error');
    if (error.stack) {
        log(error.stack, 'debug');
    }
}

// ========== Utility Functions ==========
function generateId() {
    return Math.random().toString(36).substring(2, 10) + 
           Date.now().toString(36).substring(5, 9);
}

function generateRoomId() {
    return 'room_' + generateId();
}

function getPlayerInfo(playerId) {
    const player = players.get(playerId);
    if (!player) return null;
    return {
        id: playerId,
        name: player.name || playerId,
        character: player.character,
        online: player.online,
        room: player.room,
        connectedAt: player.connectedAt,
        lastActive: player.lastActive
    };
}

function getOnlinePlayers() {
    const result = [];
    for (const [id, player] of players) {
        if (player.online) {
            result.push(getPlayerInfo(id));
        }
    }
    return result;
}

function broadcastToAll(message, excludeId = null) {
    const data = JSON.stringify(message);
    for (const [id, player] of players) {
        if (player.online && id !== excludeId) {
            try {
                if (player.ws.readyState === WebSocket.OPEN) {
                    player.ws.send(data);
                    stats.messagesSent++;
                }
            } catch (e) {
                logError(`Broadcast failed to ${id}`, e);
            }
        }
    }
}

function sendToPlayer(playerId, message) {
    const player = players.get(playerId);
    if (!player || !player.online) return false;
    try {
        if (player.ws.readyState === WebSocket.OPEN) {
            player.ws.send(JSON.stringify(message));
            stats.messagesSent++;
            return true;
        }
    } catch (e) {
        logError(`Send to ${playerId} failed`, e);
    }
    return false;
}

function removePlayer(playerId, reason = 'normal') {
    const player = players.get(playerId);
    if (!player) return;
    
    player.online = false;
    player.disconnectedAt = Date.now();
    
    // Remove from room
    if (player.room && rooms.has(player.room)) {
        const room = rooms.get(player.room);
        room.delete(playerId);
        if (room.size === 0) {
            rooms.delete(player.room);
            stats.roomsDestroyed++;
            log(`🗑️ Room ${player.room} destroyed`, 'info');
        }
    }
    
    // Notify others
    for (const [id, p] of players) {
        if (id !== playerId && p.online) {
            sendToPlayer(id, {
                type: 'friend_offline',
                player_id: playerId,
                player_name: player.name || playerId,
                timestamp: Date.now()
            });
        }
    }
    
    // Cleanup
    pendingRequests.delete(playerId);
    playerSessions.delete(playerId);
    requestCounts.delete(playerId);
    
    log(`👋 ${playerId} removed (${reason})`, 'info');
}

// ========== Connection Handler ==========
server.on('connection', (ws, req) => {
    const clientId = generateId();
    let player_id = null;
    let authenticated = false;
    let pingTimeout = null;
    let pongReceived = true;
    let connectionTime = Date.now();
    
    stats.connections++;
    stats.totalConnections++;
    if (stats.connections > stats.peakConnections) {
        stats.peakConnections = stats.connections;
    }
    
    const ip = req.socket.remoteAddress;
    log(`🔗 New connection ${clientId} from ${ip}`, 'info');
    
    // ========== Ping/Pong Management ==========
    function resetPingTimeout() {
        if (pingTimeout) clearTimeout(pingTimeout);
        pingTimeout = setTimeout(() => {
            if (!pongReceived) {
                log(`⏰ Ping timeout for ${player_id || clientId}`, 'warn');
                ws.terminate();
                return;
            }
            pongReceived = false;
            try {
                ws.ping();
                resetPingTimeout();
            } catch (e) {
                logError('Ping failed', e);
                ws.terminate();
            }
        }, CONFIG.PING_INTERVAL);
    }
    
    // ========== WebSocket Events ==========
    ws.on('pong', () => {
        pongReceived = true;
        if (player_id) {
            const player = players.get(player_id);
            if (player) {
                player.lastPong = Date.now();
            }
        }
    });
    
    ws.on('message', (data) => {
        try {
            const msg = JSON.parse(data.toString());
            stats.messagesReceived++;
            
            // Rate limiting for authenticated players
            if (player_id && isRateLimited(player_id)) {
                sendToPlayer(player_id, {
                    type: 'error',
                    message: 'Rate limit exceeded. Please wait.',
                    code: 'RATE_LIMIT'
                });
                return;
            }
            
            // ========== Message Router ==========
            switch (msg.type) {
                // ===== AUTH =====
                case 'auth':
                    if (!msg.player_id || !msg.character) {
                        ws.send(JSON.stringify({
                            type: 'auth_fail',
                            reason: 'Missing player_id or character'
                        }));
                        return;
                    }
                    
                    // Check banned
                    if (bannedPlayers.has(msg.player_id)) {
                        ws.send(JSON.stringify({
                            type: 'auth_fail',
                            reason: 'Account banned'
                        }));
                        ws.close(1000, 'banned');
                        return;
                    }
                    
                    // Check max players
                    if (stats.connections >= CONFIG.MAX_PLAYERS) {
                        ws.send(JSON.stringify({
                            type: 'auth_fail',
                            reason: 'Server is full'
                        }));
                        ws.close(1000, 'server_full');
                        return;
                    }
                    
                    player_id = msg.player_id;
                    const character = msg.character;
                    
                    // Handle reconnection
                    if (players.has(player_id)) {
                        const oldPlayer = players.get(player_id);
                        if (oldPlayer.ws.readyState === WebSocket.OPEN) {
                            oldPlayer.ws.close(1000, 'reconnect');
                        }
                    }
                    
                    players.set(player_id, {
                        ws: ws,
                        online: true,
                        character: character,
                        name: msg.name || player_id,
                        room: null,
                        connectedAt: Date.now(),
                        lastActive: Date.now(),
                        lastPong: Date.now(),
                        ip: ip,
                        clientId: clientId
                    });
                    
                    playerSessions.set(player_id, {
                        sessionId: generateId(),
                        connectedAt: Date.now(),
                        ip: ip
                    });
                    
                    authenticated = true;
                    
                    ws.send(JSON.stringify({
                        type: 'auth_ok',
                        player_id: player_id,
                        character: character,
                        server_time: Date.now(),
                        session_id: playerSessions.get(player_id).sessionId,
                        online_count: stats.connections
                    }));
                    
                    log(`✅ ${player_id} authenticated (${character})`, 'success');
                    
                    // Broadcast to all other players
                    for (const [id, player] of players) {
                        if (id !== player_id && player.online) {
                            sendToPlayer(id, {
                                type: 'friend_online',
                                player_id: player_id,
                                player_name: player_id,
                                character: character,
                                timestamp: Date.now()
                            });
                            // Send existing players to new player
                            sendToPlayer(player_id, {
                                type: 'friend_online',
                                player_id: id,
                                player_name: id,
                                character: player.character,
                                timestamp: Date.now()
                            });
                        }
                    }
                    
                    // Reset ping
                    pongReceived = true;
                    resetPingTimeout();
                    break;
                
                // ===== FRIEND REQUEST =====
                case 'friend_request':
                    if (!authenticated || !player_id) {
                        ws.send(JSON.stringify({
                            type: 'error',
                            message: 'Not authenticated'
                        }));
                        return;
                    }
                    
                    const to_id = msg.to_id;
                    const from_id = msg.from_id;
                    
                    if (to_id === from_id) {
                        sendToPlayer(player_id, {
                            type: 'error',
                            message: 'Cannot send request to yourself'
                        });
                        return;
                    }
                    
                    // Check if already connected
                    if (pendingRequests.has(from_id) && pendingRequests.get(from_id) === to_id) {
                        sendToPlayer(player_id, {
                            type: 'error',
                            message: 'Request already pending'
                        });
                        return;
                    }
                    
                    const target = players.get(to_id);
                    if (!target || !target.online) {
                        sendToPlayer(player_id, {
                            type: 'error',
                            message: 'Player is not online'
                        });
                        log(`❌ Request to ${to_id} failed (offline)`, 'error');
                        return;
                    }
                    
                    // Check if already in room
                    if (target.room || (players.get(from_id) && players.get(from_id).room)) {
                        sendToPlayer(player_id, {
                            type: 'error',
                            message: 'Player is already in a game'
                        });
                        return;
                    }
                    
                    stats.totalRequests++;
                    pendingRequests.set(from_id, to_id);
                    
                    // Send request to target
                    const sent = sendToPlayer(to_id, {
                        type: 'friend_request',
                        from_id: from_id,
                        from_name: from_id,
                        character: msg.character || players.get(from_id).character,
                        timestamp: Date.now()
                    });
                    
                    if (sent) {
                        sendToPlayer(player_id, {
                            type: 'friend_request_sent',
                            to_id: to_id,
                            to_name: to_id,
                            timestamp: Date.now()
                        });
                        log(`📩 Request from ${from_id} to ${to_id}`, 'info');
                    } else {
                        pendingRequests.delete(from_id);
                        sendToPlayer(player_id, {
                            type: 'error',
                            message: 'Failed to send request'
                        });
                    }
                    break;
                
                // ===== ACCEPT REQUEST =====
                case 'friend_request_accepted':
                    if (!authenticated || !player_id) {
                        ws.send(JSON.stringify({
                            type: 'error',
                            message: 'Not authenticated'
                        }));
                        return;
                    }
                    
                    const accept_from = msg.from_id;
                    const accept_to = msg.to_id;
                    const accept_character = msg.character;
                    
                    // Verify pending request
                    if (!pendingRequests.has(accept_from) || pendingRequests.get(accept_from) !== accept_to) {
                        sendToPlayer(player_id, {
                            type: 'error',
                            message: 'No pending request found'
                        });
                        return;
                    }
                    
                    const from_player = players.get(accept_from);
                    const to_player = players.get(accept_to);
                    
                    if (!from_player || !from_player.online || !to_player || !to_player.online) {
                        sendToPlayer(player_id, {
                            type: 'error',
                            message: 'Player is no longer online'
                        });
                        pendingRequests.delete(accept_from);
                        return;
                    }
                    
                    // Create room
                    const room_id = generateRoomId();
                    const room = new Set();
                    room.add(accept_from);
                    room.add(accept_to);
                    rooms.set(room_id, room);
                    stats.roomsCreated++;
                    stats.acceptedRequests++;
                    
                    // Update players
                    from_player.room = room_id;
                    to_player.room = room_id;
                    
                    // Notify both players
                    sendToPlayer(accept_from, {
                        type: 'friend_request_accepted',
                        from_id: accept_to,
                        to_id: accept_from,
                        character: to_player.character,
                        room_id: room_id,
                        timestamp: Date.now()
                    });
                    
                    sendToPlayer(accept_to, {
                        type: 'friend_request_accepted',
                        from_id: accept_from,
                        to_id: accept_to,
                        character: from_player.character,
                        room_id: room_id,
                        timestamp: Date.now()
                    });
                    
                    pendingRequests.delete(accept_from);
                    log(`🎮 Room ${room_id} created (${accept_from} ↔ ${accept_to})`, 'success');
                    break;
                
                // ===== REJECT REQUEST =====
                case 'friend_request_rejected':
                    if (!authenticated || !player_id) {
                        ws.send(JSON.stringify({
                            type: 'error',
                            message: 'Not authenticated'
                        }));
                        return;
                    }
                    
                    const reject_from = msg.from_id;
                    const reject_to = msg.to_id;
                    
                    if (pendingRequests.has(reject_from) && pendingRequests.get(reject_from) === reject_to) {
                        sendToPlayer(reject_from, {
                            type: 'friend_request_rejected',
                            from_id: reject_to,
                            to_id: reject_from,
                            timestamp: Date.now()
                        });
                        pendingRequests.delete(reject_from);
                        stats.rejectedRequests++;
                        log(`❌ Request from ${reject_from} rejected`, 'warn');
                    }
                    break;
                
                // ===== PING =====
                case 'ping':
                    ws.send(JSON.stringify({
                        type: 'pong',
                        timestamp: Date.now()
                    }));
                    stats.messagesSent++;
                    break;
                
                // ===== GET ONLINE PLAYERS =====
                case 'get_online_players':
                    if (!authenticated || !player_id) {
                        ws.send(JSON.stringify({
                            type: 'error',
                            message: 'Not authenticated'
                        }));
                        return;
                    }
                    
                    const onlineList = [];
                    for (const [id, player] of players) {
                        if (id !== player_id && player.online) {
                            onlineList.push({
                                player_id: id,
                                name: id,
                                character: player.character
                            });
                        }
                    }
                    
                    ws.send(JSON.stringify({
                        type: 'online_players_list',
                        players: onlineList,
                        count: onlineList.length,
                        timestamp: Date.now()
                    }));
                    break;
                
                // ===== LEAVE ROOM =====
                case 'leave_room':
                    if (!authenticated || !player_id) {
                        ws.send(JSON.stringify({
                            type: 'error',
                            message: 'Not authenticated'
                        }));
                        return;
                    }
                    
                    const player = players.get(player_id);
                    if (player && player.room && rooms.has(player.room)) {
                        const room = rooms.get(player.room);
                        room.delete(player_id);
                        if (room.size === 0) {
                            rooms.delete(player.room);
                            stats.roomsDestroyed++;
                        }
                        player.room = null;
                        
                        // Notify other player in room
                        for (const [id, p] of players) {
                            if (p.room === player.room && p.online) {
                                sendToPlayer(id, {
                                    type: 'room_member_left',
                                    player_id: player_id,
                                    room_id: player.room
                                });
                            }
                        }
                        
                        ws.send(JSON.stringify({
                            type: 'room_left',
                            room_id: player.room,
                            timestamp: Date.now()
                        }));
                    }
                    break;
                
                // ===== DISCONNECT =====
                case 'disconnect':
                    log(`🔌 ${player_id || clientId} requested disconnect`, 'warn');
                    if (player_id) {
                        removePlayer(player_id, 'client_request');
                    }
                    ws.close(1000, 'client_disconnect');
                    break;
                
                // ===== GET STATS =====
                case 'get_stats':
                    if (!authenticated || !player_id) {
                        ws.send(JSON.stringify({
                            type: 'error',
                            message: 'Not authenticated'
                        }));
                        return;
                    }
                    
                    ws.send(JSON.stringify({
                        type: 'server_stats',
                        stats: {
                            connections: stats.connections,
                            totalConnections: stats.totalConnections,
                            rooms: rooms.size,
                            roomsCreated: stats.roomsCreated,
                            messagesSent: stats.messagesSent,
                            messagesReceived: stats.messagesReceived,
                            uptime: Math.floor((Date.now() - stats.startTime) / 1000),
                            peakConnections: stats.peakConnections,
                            totalRequests: stats.totalRequests,
                            acceptedRequests: stats.acceptedRequests,
                            rejectedRequests: stats.rejectedRequests
                        },
                        timestamp: Date.now()
                    }));
                    break;
                
                default:
                    log(`⚠️ Unknown message type: ${msg.type}`, 'warn');
                    ws.send(JSON.stringify({
                        type: 'error',
                        message: 'Unknown message type'
                    }));
            }
            
        } catch (error) {
            logError('Message processing error', error);
            try {
                ws.send(JSON.stringify({
                    type: 'error',
                    message: 'Invalid message format',
                    code: 'INVALID_FORMAT'
                }));
            } catch (e) {
                // Ignore
            }
        }
    });
    
    // ========== Connection Close ==========
    ws.on('close', (code, reason) => {
        if (pingTimeout) clearTimeout(pingTimeout);
        stats.connections--;
        
        if (player_id && authenticated) {
            removePlayer(player_id, `close_${code}`);
        }
        
        log(`🔌 Connection ${clientId} closed (${code}: ${reason || 'normal'})`, 'info');
    });
    
    // ========== Connection Error ==========
    ws.on('error', (error) => {
        logError(`WebSocket error for ${player_id || clientId}`, error);
        if (player_id && authenticated) {
            removePlayer(player_id, 'error');
        }
        ws.close(1011, 'server_error');
    });
});

// ========== Server Error Handling ==========
server.on('error', (error) => {
    logError('Server error', error);
});

// ========== Health Check ==========
function healthCheck() {
    const now = Date.now();
    for (const [id, player] of players) {
        if (player.online && player.ws.readyState === WebSocket.OPEN) {
            // Check for stale connections
            if (now - player.lastPong > CONFIG.PONG_TIMEOUT) {
                log(`⚠️ ${id} stale connection, terminating`, 'warn');
                try {
                    player.ws.terminate();
                } catch (e) {
                    // Ignore
                }
                removePlayer(id, 'stale');
            }
        }
    }
}

// ========== Periodic Maintenance ==========
setInterval(() => {
    healthCheck();
    
    // Cleanup old sessions
    const now = Date.now();
    for (const [id, session] of playerSessions) {
        if (now - session.connectedAt > CONFIG.SESSION_TIMEOUT) {
            playerSessions.delete(id);
        }
    }
}, CONFIG.PING_INTERVAL);

// ========== Stats Reporting ==========
setInterval(() => {
    const uptime = Math.floor((Date.now() - stats.startTime) / 1000);
    const hours = Math.floor(uptime / 3600);
    const minutes = Math.floor((uptime % 3600) / 60);
    const seconds = uptime % 60;
    
    log(`📊 STATS | Connections: ${stats.connections}/${stats.peakConnections} | Rooms: ${rooms.size} | Requests: ${stats.totalRequests} (${stats.acceptedRequests}/${stats.rejectedRequests}) | Uptime: ${hours}h ${minutes}m ${seconds}s`, 'info');
}, 60000);

// ========== Server Ready ==========
log(`🚀 WebSocket server running on port ${PORT}`, 'success');
log(`📡 Connect with: wss://friend-system-server-2.onrender.com`, 'info');
log(`👥 Max players: ${CONFIG.MAX_PLAYERS}`, 'info');
log(`🏠 Max room size: ${CONFIG.MAX_ROOM_SIZE}`, 'info');
log(`📊 Stats endpoint: http://localhost:${PORT}/stats`, 'info');

// ========== HTTP Stats Endpoint ==========
const http = require('http');
const httpServer = http.createServer((req, res) => {
    if (req.url === '/stats') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
            status: 'online',
            version: '2.0.0',
            uptime: Math.floor((Date.now() - stats.startTime) / 1000),
            connections: stats.connections,
            peakConnections: stats.peakConnections,
            totalConnections: stats.totalConnections,
            rooms: rooms.size,
            roomsCreated: stats.roomsCreated,
            messagesSent: stats.messagesSent,
            messagesReceived: stats.messagesReceived,
            errors: stats.errors,
            totalRequests: stats.totalRequests,
            acceptedRequests: stats.acceptedRequests,
            rejectedRequests: stats.rejectedRequests,
            players: getOnlinePlayers().length,
            timestamp: Date.now()
        }));
    } else if (req.url === '/health') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
            status: 'healthy',
            timestamp: Date.now()
        }));
    } else {
        res.writeHead(404);
        res.end('Not Found');
    }
});

httpServer.listen(PORT + 1, () => {
    log(`📊 HTTP stats endpoint on port ${PORT + 1}`, 'info');
});

// ========== Graceful Shutdown ==========
function gracefulShutdown() {
    log('🛑 Graceful shutdown initiated...', 'warn');
    
    const shutdownTimeout = setTimeout(() => {
        log('⚠️ Shutdown timeout, forcing exit', 'warn');
        process.exit(1);
    }, 30000);
    
    // Close HTTP server
    httpServer.close(() => {
        log('✅ HTTP server closed', 'info');
    });
    
    // Notify all players
    broadcastToAll({
        type: 'server_shutdown',
        message: 'Server is shutting down',
        timestamp: Date.now()
    });
    
    // Close all connections
    let remaining = 0;
    for (const [id, player] of players) {
        if (player.ws.readyState === WebSocket.OPEN) {
            remaining++;
            try {
                player.ws.close(1000, 'server_shutdown');
            } catch (e) {
                // Ignore
            }
        }
    }
    
    log(`📡 ${remaining} connections to close`, 'info');
    
    setTimeout(() => {
        // Close server
        server.close(() => {
            log('✅ WebSocket server closed', 'success');
            clearTimeout(shutdownTimeout);
            process.exit(0);
        });
    }, 2000);
}

process.on('SIGINT', gracefulShutdown);
process.on('SIGTERM', gracefulShutdown);

// ========== Unhandled Error Handling ==========
process.on('uncaughtException', (error) => {
    logError('Uncaught exception', error);
    if (error.code === 'ECONNRESET' || error.code === 'EPIPE') {
        // Ignore connection errors
        return;
    }
});

process.on('unhandledRejection', (error) => {
    logError('Unhandled rejection', error);
});

log('✅ Server initialization complete', 'success');
log('📡 Ready for connections', 'success');