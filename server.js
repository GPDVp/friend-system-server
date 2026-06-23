const WebSocket = require('ws');

const server = new WebSocket.Server({
    port: process.env.PORT || 10000,
    perMessageDeflate: true,
    maxPayload: 1048576,
    clientTracking: true,
    path: '/ws'
});

const chatRooms = new Map();
const players = new Map();

function log(message, type = 'info') {
    const timestamp = new Date().toLocaleString('fa-IR');
    const emoji = type === 'success' ? '✅' : type === 'error' ? '❌' : type === 'warn' ? '⚠️' : '📌';
    console.log(`[${timestamp}] ${emoji} ${message}`);
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

server.on('connection', (ws, req) => {
    const ip = req.socket.remoteAddress || '0.0.0.0';
    log(`New connection from ${ip}`, 'info');
    
    let player_id = null;
    let authenticated = false;
    let currentRoom = null;
    let playerDisplayName = "کاربر";  // 👈 اسم کاربر رو اینجا نگه دار
    
    ws.on('message', (data) => {
        try {
            const msg = JSON.parse(data.toString());
            log(`Received: ${msg.type} from ${player_id || 'unknown'}`, 'info');
            
            switch (msg.type) {
                case 'auth':
                    // ✅ گرفتن اسم از پیام
                    player_id = msg.player_id || msg.id || `user_${Date.now()}_${Math.random().toString(36).substr(2, 6)}`;
                    playerDisplayName = msg.display_name || msg.player_name || "کاربر";
                    
                    log(`📝 Auth attempt: ID=${player_id}, Name=${playerDisplayName}`, 'info');
                    
                    // حذف کاربر قبلی اگر آنلاین باشد
                    if (players.has(player_id) && players.get(player_id).online) {
                        const oldPlayer = players.get(player_id);
                        if (oldPlayer.ws.readyState === WebSocket.OPEN) {
                            oldPlayer.ws.close(1000, 'reconnect');
                        }
                    }
                    
                    // ✅ ذخیره اطلاعات کاربر با اسم صحیح
                    players.set(player_id, {
                        ws: ws,
                        online: true,
                        displayName: playerDisplayName,  // 👈 اسم ذخیره میشه
                        room: null,
                        connectedAt: Date.now(),
                        ip: ip
                    });
                    
                    authenticated = true;
                    
                    // ✅ ارسال پاسخ با اسم صحیح
                    ws.send(JSON.stringify({
                        type: 'auth_ok',
                        id: player_id,
                        display_name: playerDisplayName,
                        server_time: Date.now()
                    }));
                    
                    log(`✅ ${player_id} (${playerDisplayName}) authenticated`, 'success');
                    break;
                
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
                            user_name: playerDisplayName,  // 👈 استفاده از اسم ذخیره‌شده
                            user_count: oldRoom.size
                        }, player_id);
                        log(`${player_id} left room ${currentRoom}`, 'info');
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
                    
                    // ✅ به‌روزرسانی اطلاعات کاربر در روم
                    const playerData = players.get(player_id);
                    if (playerData) {
                        playerData.room = roomId;
                        room.set(player_id, playerData);
                    }
                    currentRoom = roomId;
                    
                    // ارسال لیست کاربران
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
                    
                    // ✅ اطلاع به دیگران با اسم صحیح
                    broadcastToRoom(roomId, {
                        type: 'user_joined',
                        user_name: playerDisplayName,  // 👈 استفاده از اسم ذخیره‌شده
                        user_count: room.size
                    }, player_id);
                    
                    log(`👤 ${playerDisplayName} (${player_id}) joined room ${roomId} (${room.size} users)`, 'info');
                    break;
                
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
                    
                    // ✅ ارسال پیام با اسم صحیح
                    broadcastToRoom(currentRoom, {
                        type: 'chat_message',
                        sender_id: player_id,
                        sender_name: sender.displayName || playerDisplayName || "کاربر",  // 👈 استفاده از اسم ذخیره‌شده
                        message: message.substring(0, 500),
                        timestamp: timestamp
                    }, player_id);
                    
                    log(`💬 [${currentRoom}] ${sender.displayName || playerDisplayName}: ${message.substring(0, 50)}`, 'info');
                    break;
                
                case 'leave_room':
                    if (player_id && currentRoom && chatRooms.has(currentRoom)) {
                        const room = chatRooms.get(currentRoom);
                        room.delete(player_id);
                        broadcastToRoom(currentRoom, {
                            type: 'user_left',
                            user_name: playerDisplayName,  // 👈 استفاده از اسم ذخیره‌شده
                            user_count: room.size
                        }, player_id);
                        
                        if (room.size === 0) {
                            chatRooms.delete(currentRoom);
                            log(`🗑️ Room ${currentRoom} deleted (empty)`, 'info');
                        }
                        
                        currentRoom = null;
                    }
                    break;
                
                case 'heartbeat':
                    ws.send(JSON.stringify({ 
                        type: 'heartbeat_ack', 
                        timestamp: Date.now() 
                    }));
                    break;
                
                case 'ping':
                    ws.send(JSON.stringify({ 
                        type: 'pong', 
                        timestamp: Date.now() 
                    }));
                    break;
                
                case 'disconnect':
                    if (player_id) {
                        if (currentRoom && chatRooms.has(currentRoom)) {
                            const room = chatRooms.get(currentRoom);
                            room.delete(player_id);
                            broadcastToRoom(currentRoom, {
                                type: 'user_left',
                                user_name: playerDisplayName,  // 👈 استفاده از اسم ذخیره‌شده
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
                        log(`👋 ${playerDisplayName} (${player_id}) disconnected`, 'info');
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
        if (player_id) {
            if (currentRoom && chatRooms.has(currentRoom)) {
                const room = chatRooms.get(currentRoom);
                room.delete(player_id);
                broadcastToRoom(currentRoom, {
                    type: 'user_left',
                    user_name: playerDisplayName,  // 👈 استفاده از اسم ذخیره‌شده
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
            log(`👋 ${playerDisplayName} (${player_id}) disconnected`, 'info');
        }
    });
    
    ws.on('error', (error) => {
        log(`WebSocket error: ${error.message}`, 'error');
    });
});

log(`🚀 Chat Server running on port ${process.env.PORT || 10000}`, 'success');
log(`📡 WebSocket ready at /ws`, 'success');
log(`💡 Waiting for connections...`, 'info');