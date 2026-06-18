// =============================================
// server.js - سرور WebSocket برای Render
// =============================================
const WebSocket = require('ws');

// پورت را از Render دریافت کن
const PORT = process.env.PORT || 10000;
const server = new WebSocket.Server({ port: PORT });

// ذخیره بازیکنان
const players = new Map();

console.log(`🚀 سرور روی پورت ${PORT} اجرا شد`);

server.on('connection', (ws, req) => {
    let player_id = null;
    
    console.log(`✅ اتصال جدید از ${req.socket.remoteAddress}`);
    
    ws.on('message', (data) => {
        try {
            const msg = JSON.parse(data.toString());
            console.log(`📨 دریافت: ${msg.type} از ${player_id || 'unknown'}`);
            
            switch (msg.type) {
                case 'auth':
                    player_id = msg.player_id;
                    players.set(player_id, { 
                        ws: ws, 
                        online: true,
                        character: msg.character 
                    });
                    ws.send(JSON.stringify({ 
                        type: 'auth_ok',
                        player_id: player_id
                    }));
                    console.log(`✅ ${player_id} احراز هویت شد`);
                    break;
                
                case 'ping':
                    ws.send(JSON.stringify({ type: 'pong' }));
                    break;
                
                case 'friend_request':
                    const to_id = msg.to_id;
                    const target = players.get(to_id);
                    
                    if (target && target.online) {
                        target.ws.send(JSON.stringify({
                            type: 'friend_request',
                            from_id: msg.from_id,
                            character: msg.character
                        }));
                        ws.send(JSON.stringify({
                            type: 'friend_online',
                            player_id: to_id,
                            character: target.character
                        }));
                        console.log(`📩 درخواست از ${msg.from_id} به ${to_id}`);
                    } else {
                        ws.send(JSON.stringify({
                            type: 'error',
                            message: 'دوست شما آفلاین است'
                        }));
                        console.log(`❌ ${to_id} آفلاین است`);
                    }
                    break;
                
                case 'friend_request_accepted':
                    const from_id = msg.from_id;
                    const target_player = players.get(from_id);
                    
                    if (target_player && target_player.online) {
                        target_player.ws.send(JSON.stringify({
                            type: 'friend_request_accepted',
                            from_id: msg.to_id,
                            character: msg.character
                        }));
                        console.log(`✅ درخواست از ${from_id} قبول شد`);
                    }
                    break;
                
                case 'friend_request_rejected':
                    const reject_from = msg.from_id;
                    const reject_target = players.get(reject_from);
                    
                    if (reject_target && reject_target.online) {
                        reject_target.ws.send(JSON.stringify({
                            type: 'friend_request_rejected',
                            from_id: msg.to_id
                        }));
                        console.log(`❌ درخواست از ${reject_from} رد شد`);
                    }
                    break;
                
                case 'disconnect':
                    console.log(`🔌 ${player_id} درخواست قطع کرد`);
                    ws.close(1000, 'normal');
                    break;
                
                default:
                    console.log(`⚠️ نوع پیام ناشناخته: ${msg.type}`);
                    ws.send(JSON.stringify({
                        type: 'error',
                        message: 'نوع پیام نامعتبر'
                    }));
            }
            
        } catch (error) {
            console.error(`❌ خطا در پردازش پیام: ${error.message}`);
            ws.send(JSON.stringify({
                type: 'error',
                message: 'خطا در پردازش پیام'
            }));
        }
    });
    
    ws.on('close', (code, reason) => {
        if (player_id) {
            players.delete(player_id);
            console.log(`❌ ${player_id} قطع شد (کد: ${code})`);
        }
    });
    
    ws.on('error', (error) => {
        console.error(`❌ خطای WebSocket: ${error.message}`);
    });
});

// پینگ خودکار هر 30 ثانیه
setInterval(() => {
    for (const [id, player] of players) {
        if (player.online && player.ws.readyState === WebSocket.OPEN) {
            player.ws.ping();
        }
    }
}, 30000);

// نمایش آمار هر دقیقه
setInterval(() => {
    console.log(`📊 آمار: ${players.size} بازیکن آنلاین`);
}, 60000);