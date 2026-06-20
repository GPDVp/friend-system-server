// =============================================
// server.js - FINAL VERSION (NO ERRORS)
// =============================================
const WebSocket = require('ws');
const server = new WebSocket.Server({ port: process.env.PORT || 10000 });

const players = new Map();

server.on('connection', (ws) => {
    let player_id = null;
    let character = null;
    
    ws.on('message', (data) => {
        try {
            const msg = JSON.parse(data.toString());
            
            if (msg.type === 'auth') {
                player_id = msg.player_id;
                character = msg.character;
                players.set(player_id, { ws, online: true, character });
                ws.send(JSON.stringify({ type: 'auth_ok' }));
                console.log(`✅ ${player_id} authenticated as ${character}`);
                
                for (const [id, player] of players) {
                    if (id !== player_id && player.online) {
                        player.ws.send(JSON.stringify({
                            type: 'friend_online',
                            player_id: player_id,
                            character: character
                        }));
                    }
                }
            }
            else if (msg.type === 'ping') {
                ws.send(JSON.stringify({ type: 'pong' }));
            }
            else if (msg.type === 'friend_request') {
                const target = players.get(msg.to_id);
                if (target && target.online) {
                    target.ws.send(JSON.stringify({
                        type: 'friend_request',
                        from_id: msg.from_id,
                        character: msg.character
                    }));
                    ws.send(JSON.stringify({ type: 'friend_request_sent' }));
                    console.log(`📩 Request from ${msg.from_id} to ${msg.to_id}`);
                } else {
                    ws.send(JSON.stringify({ type: 'error', message: 'Friend is offline' }));
                }
            }
            else if (msg.type === 'friend_request_accepted') {
                const acceptor = players.get(msg.from_id);
                const requester = players.get(msg.to_id);
                
                if (requester && requester.online) {
                    requester.ws.send(JSON.stringify({
                        type: 'friend_request_accepted',
                        from_id: msg.from_id,
                        character: msg.character
                    }));
                    console.log(`📤 Sent to ${msg.to_id}: character ${msg.character}`);
                }
                
                if (acceptor && acceptor.online) {
                    const requesterCharacter = requester ? requester.character : 'King';
                    acceptor.ws.send(JSON.stringify({
                        type: 'friend_request_accepted',
                        from_id: msg.to_id,
                        character: requesterCharacter
                    }));
                    console.log(`📤 Sent to ${msg.from_id}: character ${requesterCharacter}`);
                }
                
                console.log(`🎮 ${msg.from_id} accepted request from ${msg.to_id}`);
            }
            else if (msg.type === 'friend_request_rejected') {
                const target = players.get(msg.from_id);
                if (target && target.online) {
                    target.ws.send(JSON.stringify({
                        type: 'friend_request_rejected',
                        from_id: msg.to_id
                    }));
                }
            }
        } catch(e) { 
            console.error('❌ Error:', e.message);
        }
    });
    
    ws.on('close', () => {
        if (player_id) {
            players.delete(player_id);
            for (const [id, player] of players) {
                if (player.online) {
                    player.ws.send(JSON.stringify({
                        type: 'friend_offline',
                        player_id: player_id
                    }));
                }
            }
            console.log(`❌ ${player_id} disconnected`);
        }
    });
});

console.log('🚀 Server running on port', process.env.PORT || 10000);