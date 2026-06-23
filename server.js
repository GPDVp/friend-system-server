// =============================================
// server.js - سرور کامل چت گروهی
// =============================================
const WebSocket = require('ws');
const server = new WebSocket.Server({
    port: process.env.PORT || 10000,
    perMessageDeflate: true,
    maxPayload: 1048576,
    clientTracking: true
});

// ============================================================
// ✅ داده‌های سرور
// ============================================================
const chatRooms = new Map();
const players = new Map();

// ============================================================
// ✅ توابع کمکی
// ============================================================
function log(message, type = 'info') {
    const timestamp = new Date().toLocaleString();
    console.log(`[${timestamp}] ${message}`);
}

function broadcastToRoom(roomId, message, excludeId = null) {
    const room = chatRooms.get(roomId);
    if (!room) return;
    
    const data = JSON.stringify(message);
    for (const [id, player] of room) {
        if (id !== excludeId && player.online) {
            try {
                if (player.ws.readyState === WebSocket.OPEN) {
                    player.ws.send(data);
                }
            } catch (e) {
                log(`Broadcast failed: ${e.message}`, 'error');
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
            return true;
        }
    } catch (e) {
        log(`Send failed: ${e.message}`, 'error');
    }
    return false;
}

// ============================================================
// ✅ سرور WebSocket
// ============================================================
server.on('connection', (ws, req) => {
    const ip = req.socket.remoteAddress || '0.0.0.0';
    log(`🔗 New connection from ${ip}`, 'info');
    
    let player_id = null;
    let authenticated = false;
    let currentRoom = null;
    let pingTimeout = null;
    let pongReceived = true;
    
    function resetPingTimeout() {
        if (pingTimeout) clearTimeout(pingTimeout);
        pingTimeout = setTimeout(() => {
            if (!pongReceived) {
                log(`⏰ Ping timeout for ${player_id || 'unknown'}`, 'warn');
                ws.terminate();
                return;
            }
            pongReceived = false;
            try { ws.ping(); } catch(e) { ws.terminate(); }
            resetPingTimeout();
        }, 30000);
    }
    
    ws.on('pong', () => {
        pongReceived = true;
    });
    
    ws.on('message', (data) => {
        try {
            const msg = JSON.parse(data.toString());
            
            switch (msg.type) {
                // ============================================================
                // ✅ احراز هویت
                // ============================================================
                case 'auth':
                    player_id = msg.player_id;
                    const displayName = msg.display_name || msg.player_name || player_id;
                    
                    // حذف کاربر قبلی اگر آنلاین باشد
                    if (players.has(player_id) && players.get(player_id).online) {
                        const oldPlayer = players.get(player_id);
                        if (oldPlayer.ws.readyState === WebSocket.OPEN) {
                            oldPlayer.ws.close(1000, 'reconnect');
                        }
                    }
                    
                    players.set(player_id, {
                        ws: ws,
                        online: true,
                        displayName: displayName,
                        room: null,
                        connectedAt: Date.now(),
                        ip: ip
                    });
                    
                    authenticated = true;
                    
                    ws.send(JSON.stringify({
                        type: 'auth_ok',
                        player_id: player_id,
                        display_name: displayName,
                        server_time: Date.now()
                    }));
                    
                    log(`✅ ${player_id} (${displayName}) authenticated`, 'success');
                    resetPingTimeout();
                    break;
                
                // ============================================================
                // ✅ پیوستن به روم چت
                // ============================================================
                case 'join_room':
                    if (!authenticated || !player_id) {
                        ws.send(JSON.stringify({
                            type: 'error',
                            message: 'Not authenticated'
                        }));
                        return;
                    }
                    
                    const roomId = msg.room_id || 'global_chat';
                    const maxUsers = msg.max_users || 999999;
                    
                    // خروج از روم قبلی
                    if (currentRoom && chatRooms.has(currentRoom)) {
                        const oldRoom = chatRooms.get(currentRoom);
                        oldRoom.delete(player_id);
                        broadcastToRoom(currentRoom, {
                            type: 'user_left',
                            user_name: players.get(player_id)?.displayName || player_id,
                            user_count: oldRoom.size
                        }, player_id);
                        log(`👤 ${player_id} left room ${currentRoom}`, 'info');
                    }
                    
                    // ورود به روم جدید
                    if (!chatRooms.has(roomId)) {
                        chatRooms.set(roomId, new Map());
                    }
                    
                    const room = chatRooms.get(roomId);
                    
                    if (room.size >= maxUsers) {
                        ws.send(JSON.stringify({
                            type: 'room_full',
                            room_id: roomId,
                            message: 'Room is full'
                        }));
                        return;
                    }
                    
                    room.set(player_id, players.get(player_id));
                    currentRoom = roomId;
                    
                    // ✅ ارسال پاسخ room_joined به کلاینت
                    const users = [];
                    for (const [id, player] of room) {
                        users.push({
                            id: id,
                            name: player.displayName || id
                        });
                    }
                    
                    ws.send(JSON.stringify({
                        type: 'room_joined',
                        room_id: roomId,
                        user_count: room.size,
                        users: users
                    }));
                    
                    // اطلاع به دیگران
                    broadcastToRoom(roomId, {
                        type: 'user_joined',
                        user_name: players.get(player_id)?.displayName || player_id,
                        user_count: room.size
                    }, player_id);
                    
                    log(`👤 ${player_id} joined room ${roomId} (${room.size} users)`, 'info');
                    break;
                
                // ============================================================
                // ✅ پیام چت
                // ============================================================
                case 'chat_message':
                    if (!authenticated || !player_id || !currentRoom) {
                        ws.send(JSON.stringify({
                            type: 'error',
                            message: 'Not in a room'
                        }));
                        return;
                    }
                    
                    const message = msg.message;
                    if (!message || message.trim().length === 0) return;
                    
                    const sender = players.get(player_id);
                    if (!sender) return;
                    
                    const timestamp = msg.timestamp || new Date().toLocaleTimeString('fa-IR', { hour: '2-digit', minute: '2-digit' });
                    
                    broadcastToRoom(currentRoom, {
                        type: 'chat_message',
                        sender_id: player_id,
                        sender_name: sender.displayName || player_id,
                        message: message.substring(0, 500),
                        timestamp: timestamp
                    }, player_id);
                    
                    log(`💬 [${currentRoom}] ${sender.displayName}: ${message.substring(0, 50)}`, 'info');
                    break;
                
                // ============================================================
                // ✅ خروج از روم
                // ============================================================
                case 'leave_room':
                    if (player_id && currentRoom && chatRooms.has(currentRoom)) {
                        const room = chatRooms.get(currentRoom);
                        room.delete(player_id);
                        broadcastToRoom(currentRoom, {
                            type: 'user_left',
                            user_name: players.get(player_id)?.displayName || player_id,
                            user_count: room.size
                        }, player_id);
                        
                        if (room.size === 0) {
                            chatRooms.delete(currentRoom);
                            log(`🗑️ Room ${currentRoom} deleted (empty)`, 'info');
                        }
                        
                        currentRoom = null;
                    }
                    break;
                
                // ============================================================
                // ✅ Heartbeat
                // ============================================================
                case 'heartbeat':
                    ws.send(JSON.stringify({ 
                        type: 'heartbeat_ack', 
                        timestamp: Date.now() 
                    }));
                    break;
                
                // ============================================================
                // ✅ پینگ
                // ============================================================
                case 'ping':
                    ws.send(JSON.stringify({ 
                        type: 'pong', 
                        timestamp: Date.now() 
                    }));
                    break;
                
                // ============================================================
                // ✅ قطع اتصال
                // ============================================================
                case 'disconnect':
                    if (player_id) {
                        if (currentRoom && chatRooms.has(currentRoom)) {
                            const room = chatRooms.get(currentRoom);
                            room.delete(player_id);
                            broadcastToRoom(currentRoom, {
                                type: 'user_left',
                                user_name: players.get(player_id)?.displayName || player_id,
                                user_count: room.size
                            }, player_id);
                            if (room.size === 0) {
                                chatRooms.delete(currentRoom);
                            }
                            currentRoom = null;
                        }
                        
                        const p = players.get(player_id);
                        if (p) p.online = false;
                        players.delete(player_id);
                        log(`👋 ${player_id} disconnected`, 'info');
                    }
                    ws.close(1000, 'client_disconnect');
                    break;
                
                default:
                    ws.send(JSON.stringify({
                        type: 'error',
                        message: 'Unknown message type: ' + msg.type
                    }));
            }
            
        } catch (error) {
            log(`Error: ${error.message}`, 'error');
            ws.send(JSON.stringify({
                type: 'error',
                message: 'Invalid message format'
            }));
        }
    });
    
    ws.on('close', () => {
        if (pingTimeout) clearTimeout(pingTimeout);
        
        if (player_id) {
            if (currentRoom && chatRooms.has(currentRoom)) {
                const room = chatRooms.get(currentRoom);
                room.delete(player_id);
                broadcastToRoom(currentRoom, {
                    type: 'user_left',
                    user_name: players.get(player_id)?.displayName || player_id,
                    user_count: room.size
                }, player_id);
                if (room.size === 0) {
                    chatRooms.delete(currentRoom);
                }
                currentRoom = null;
            }
            
            const p = players.get(player_id);
            if (p) p.online = false;
            players.delete(player_id);
            log(`👋 ${player_id} disconnected`, 'info');
        }
    });
    
    ws.on('error', (error) => {
        log(`WebSocket error: ${error.message}`, 'error');
    });
});

// ============================================================
// ✅ HTTP Stats
// ============================================================
const http = require('http');
const httpServer = http.createServer((req, res) => {
    if (req.url === '/stats') {
        const totalUsers = Array.from(chatRooms.values()).reduce((sum, room) => sum + room.size, 0);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
            status: 'online',
            players: players.size,
            chatRooms: chatRooms.size,
            totalUsers: totalUsers,
            rooms: Array.from(chatRooms.keys()),
            timestamp: Date.now()
        }));
    } else if (req.url === '/health') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
            status: 'healthy',
            players: players.size,
            chatRooms: chatRooms.size,
            timestamp: Date.now()
        }));
    } else {
        res.writeHead(404);
        res.end('Not Found');
    }
});

const HTTP_PORT = parseInt(process.env.PORT || 10000) + 1;
httpServer.listen(HTTP_PORT, () => {
    log(`📊 HTTP stats on port ${HTTP_PORT}`, 'info');
});

log(`🚀 Chat Server running on port ${process.env.PORT || 10000}`, 'success');
log(`📡 WebSocket ready for connections`, 'success');