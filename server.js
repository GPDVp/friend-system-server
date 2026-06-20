// =================================================================
// server.js — Friend System WebSocket Server (Hardened / Advanced)
// =================================================================
// Upgrades over the previous version:
//  - No duplicate player IDs: a second login with an ID that is
//    already actively connected is rejected (type: "duplicate_id"),
//    while a real reconnect (same ID, old socket already dead) is
//    accepted seamlessly.
//  - Friend bonds are persistent: once two players accept each
//    other, the bond survives disconnects. On reconnect the server
//    automatically tells the client whether its friend is online,
//    so the UI can restore itself without a fresh request.
//  - Pending friend requests are tracked & validated server-side
//    (no more "ghost accepts" of requests that were never sent),
//    auto-expire, and can be cancelled by the sender.
//  - Online/offline/accepted events are sent ONLY to the relevant
//    friend instead of being broadcast to every connected player.
//  - Input is validated/sanitized (length & type checks).
//  - A real WebSocket-level heartbeat detects and cleans up dead
//    sockets (e.g. phone put to sleep, network drop without a
//    proper close frame) so stale entries never block new logins.
//  - Graceful shutdown + defensive error handling everywhere.
// =================================================================

'use strict';

const WebSocket = require('ws');

const PORT = process.env.PORT || 10000;
const MAX_ID_LENGTH = 32;
const MAX_CHAR_LENGTH = 32;
const REQUEST_TIMEOUT_MS = 15000;      // server-side pending request expiry
const HEARTBEAT_INTERVAL_MS = 25000;   // ws-level ping interval
const STALE_RECORD_TTL_MS = 24 * 60 * 60 * 1000; // cleanup of old offline records

const wss = new WebSocket.Server({ port: PORT });

// player_id -> { ws, character, friendId, online, isAlive, lastSeen }
const players = new Map();

// recipientId -> { fromId, timeout }   (one incoming pending request per recipient)
const pendingRequests = new Map();

// ----------------------------- helpers -----------------------------

function safeSend(ws, payload) {
    if (ws && ws.readyState === WebSocket.OPEN) {
        try {
            ws.send(JSON.stringify(payload));
        } catch (err) {
            console.error('⚠️ send error:', err.message);
        }
    }
}

function isValidId(id) {
    return typeof id === 'string' && id.length > 0 && id.length <= MAX_ID_LENGTH;
}

function isValidCharacter(c) {
    return typeof c === 'string' && c.length > 0 && c.length <= MAX_CHAR_LENGTH;
}

function getOutgoingPendingId(fromId) {
    for (const [toId, req] of pendingRequests) {
        if (req.fromId === fromId) return toId;
    }
    return null;
}

function clearPendingTo(recipientId) {
    const req = pendingRequests.get(recipientId);
    if (req) {
        clearTimeout(req.timeout);
        pendingRequests.delete(recipientId);
    }
}

function clearAllPendingFor(id) {
    // clear an incoming pending request addressed to `id`
    clearPendingTo(id);
    // clear an outgoing pending request started by `id`
    const outgoingTo = getOutgoingPendingId(id);
    if (outgoingTo) clearPendingTo(outgoingTo);
}

function notifyFriend(record, type, extra) {
    if (!record || !record.friendId) return;
    const friend = players.get(record.friendId);
    if (friend) {
        safeSend(friend.ws, Object.assign({ type }, extra));
    }
}

function breakBond(idA) {
    const a = players.get(idA);
    if (!a || !a.friendId) return;
    const idB = a.friendId;
    const b = players.get(idB);
    a.friendId = null;
    if (b) b.friendId = null;
}

// --------------------------- connection -----------------------------

wss.on('connection', (ws) => {
    let playerId = null; // the ID this socket is currently authenticated as

    ws.isAlive = true;
    ws.on('pong', () => { ws.isAlive = true; });

    ws.on('message', (raw) => {
        let msg;
        try {
            msg = JSON.parse(raw.toString());
        } catch (err) {
            console.error('❌ Bad JSON:', err.message);
            return;
        }
        if (!msg || typeof msg !== 'object') return;

        try {
            handleMessage(ws, msg, () => playerId, (id) => { playerId = id; });
        } catch (err) {
            console.error('❌ Handler error:', err.message);
            safeSend(ws, { type: 'error', message: 'internal_error' });
        }
    });

    ws.on('close', () => {
        if (!playerId) return;
        const record = players.get(playerId);
        if (!record) return;

        // Tell the bonded friend (if any) that we went offline, but keep
        // the bond itself so it can be restored automatically on reconnect.
        notifyFriend(record, 'friend_offline', { player_id: playerId });

        record.online = false;
        record.ws = null;
        record.lastSeen = Date.now();

        // Drop any pending requests that involved this player.
        clearAllPendingFor(playerId);

        console.log(`❌ ${playerId} disconnected`);
    });

    ws.on('error', (err) => {
        console.error('⚠️ Socket error:', err.message);
    });
});

// ----------------------------- routing --------------------------------

function handleMessage(ws, msg, getPlayerId, setPlayerId) {
    const type = msg.type;
    const currentId = getPlayerId();

    switch (type) {
        case 'auth':
            return handleAuth(ws, msg, setPlayerId);

        case 'ping':
            safeSend(ws, { type: 'pong' });
            if (currentId && players.has(currentId)) {
                players.get(currentId).lastSeen = Date.now();
            }
            return;

        case 'friend_request':
            return handleFriendRequest(ws, msg, currentId);

        case 'friend_request_cancel':
            return handleFriendRequestCancel(msg, currentId);

        case 'friend_request_accepted':
            return handleFriendRequestAccepted(msg, currentId);

        case 'friend_request_rejected':
            return handleFriendRequestRejected(msg, currentId);

        case 'leave_friend':
            return handleLeaveFriend(currentId);

        default:
            // Unknown message types are ignored silently (forward-compatible).
            return;
    }
}

// ----------------------------- handlers --------------------------------

function handleAuth(ws, msg, setPlayerId) {
    const id = msg.player_id;
    const character = msg.character;

    if (!isValidId(id) || !isValidCharacter(character)) {
        safeSend(ws, { type: 'error', message: 'invalid_auth' });
        return;
    }

    const existing = players.get(id);

    // Reject if another socket is ACTIVELY connected with this exact ID.
    if (existing && existing.online && existing.ws && existing.ws !== ws &&
        existing.ws.readyState === WebSocket.OPEN) {
        safeSend(ws, { type: 'duplicate_id' });
        console.log(`🚫 Duplicate ID rejected: ${id}`);
        return;
    }

    setPlayerId(id);

    const record = existing || { friendId: null };
    record.ws = ws;
    record.character = character;
    record.online = true;
    record.isAlive = true;
    record.lastSeen = Date.now();
    players.set(id, record);

    // Build a reconnect-aware response so the client can restore its
    // friend pairing automatically, without re-sending a request.
    const ack = { type: 'auth_ok' };
    if (record.friendId) {
        const friend = players.get(record.friendId);
        ack.friend_id = record.friendId;
        ack.friend_online = !!(friend && friend.online);
        ack.friend_character = friend ? friend.character : null;
    }
    safeSend(ws, ack);
    console.log(`✅ ${id} authenticated as ${character}`);

    // If we already had a friend bonded, let them know we're back.
    notifyFriend(record, 'friend_online', { player_id: id, character });
}

function handleFriendRequest(ws, msg, fromId) {
    const toId = msg.to_id;

    if (!fromId || !players.get(fromId) || !players.get(fromId).online) {
        safeSend(ws, { type: 'error', message: 'not_authenticated' });
        return;
    }
    if (!isValidId(toId)) {
        safeSend(ws, { type: 'error', message: 'invalid_target' });
        return;
    }
    if (toId === fromId) {
        safeSend(ws, { type: 'error', message: 'cannot_friend_self' });
        return;
    }

    const sender = players.get(fromId);
    const target = players.get(toId);

    if (sender.friendId) {
        safeSend(ws, { type: 'error', message: 'already_connected' });
        return;
    }
    if (!target || !target.online) {
        safeSend(ws, { type: 'error', message: 'friend_offline' });
        return;
    }
    if (target.friendId) {
        safeSend(ws, { type: 'error', message: 'target_busy' });
        return;
    }
    if (getOutgoingPendingId(fromId)) {
        safeSend(ws, { type: 'error', message: 'request_already_pending' });
        return;
    }
    if (pendingRequests.has(toId)) {
        safeSend(ws, { type: 'error', message: 'target_busy' });
        return;
    }

    const timeout = setTimeout(() => {
        const stillPending = pendingRequests.get(toId);
        if (stillPending && stillPending.fromId === fromId) {
            pendingRequests.delete(toId);
            const requester = players.get(fromId);
            if (requester && requester.online) {
                safeSend(requester.ws, { type: 'friend_request_timeout', to_id: toId });
            }
            const recipient = players.get(toId);
            if (recipient && recipient.online) {
                safeSend(recipient.ws, { type: 'friend_request_cancelled', from_id: fromId });
            }
        }
    }, REQUEST_TIMEOUT_MS);

    pendingRequests.set(toId, { fromId, timeout });

    safeSend(target.ws, {
        type: 'friend_request',
        from_id: fromId,
        character: sender.character
    });
    safeSend(ws, { type: 'friend_request_sent' });
    console.log(`📩 Request from ${fromId} to ${toId}`);
}

function handleFriendRequestCancel(msg, fromId) {
    if (!fromId) return;
    const toId = msg.to_id || getOutgoingPendingId(fromId);
    if (!toId) return;
    const req = pendingRequests.get(toId);
    if (req && req.fromId === fromId) {
        clearPendingTo(toId);
        const recipient = players.get(toId);
        if (recipient && recipient.online) {
            safeSend(recipient.ws, { type: 'friend_request_cancelled', from_id: fromId });
        }
        console.log(`🚫 ${fromId} cancelled request to ${toId}`);
    }
}

function handleFriendRequestAccepted(msg, acceptorId) {
    const requesterId = msg.from_id;

    if (!acceptorId || !isValidId(requesterId)) return;

    const pending = pendingRequests.get(acceptorId);
    if (!pending || pending.fromId !== requesterId) {
        const acceptor = players.get(acceptorId);
        if (acceptor && acceptor.online) {
            safeSend(acceptor.ws, { type: 'error', message: 'request_not_found' });
        }
        return;
    }

    clearPendingTo(acceptorId);

    const requester = players.get(requesterId);
    const acceptor = players.get(acceptorId);
    if (!requester || !acceptor) return;

    // Bond both players together (persists across disconnects).
    requester.friendId = acceptorId;
    acceptor.friendId = requesterId;

    if (requester.online) {
        safeSend(requester.ws, {
            type: 'friend_request_accepted',
            from_id: acceptorId,
            character: acceptor.character
        });
    }
    if (acceptor.online) {
        safeSend(acceptor.ws, {
            type: 'friend_request_accepted',
            from_id: requesterId,
            character: requester.character
        });
    }

    console.log(`🎮 ${acceptorId} accepted request from ${requesterId}`);
}

function handleFriendRequestRejected(msg, rejectorId) {
    const requesterId = msg.from_id;
    if (!rejectorId || !isValidId(requesterId)) return;

    const pending = pendingRequests.get(rejectorId);
    if (!pending || pending.fromId !== requesterId) return;

    clearPendingTo(rejectorId);

    const requester = players.get(requesterId);
    if (requester && requester.online) {
        safeSend(requester.ws, { type: 'friend_request_rejected', from_id: rejectorId });
    }
    console.log(`🙅 ${rejectorId} rejected request from ${requesterId}`);
}

function handleLeaveFriend(playerId) {
    if (!playerId) return;
    const record = players.get(playerId);
    if (!record || !record.friendId) return;

    const friendId = record.friendId;
    notifyFriend(record, 'friend_left', { player_id: playerId });
    breakBond(playerId);
    console.log(`👋 ${playerId} left friend ${friendId}`);
}

// --------------------------- maintenance --------------------------------

// WebSocket-level heartbeat: detects half-open / dead sockets that never
// sent a proper close frame, so duplicate-ID checks and friend status
// stay accurate even after abrupt disconnects (app killed, signal lost).
const heartbeat = setInterval(() => {
    wss.clients.forEach((ws) => {
        if (ws.isAlive === false) {
            ws.terminate();
            return;
        }
        ws.isAlive = false;
        try { ws.ping(); } catch (_) { /* ignore */ }
    });
}, HEARTBEAT_INTERVAL_MS);

// Periodically forget very old, offline player records so memory doesn't
// grow without bound on long-running deployments.
const cleanup = setInterval(() => {
    const now = Date.now();
    for (const [id, record] of players) {
        if (!record.online && (now - (record.lastSeen || 0)) > STALE_RECORD_TTL_MS) {
            breakBond(id);
            players.delete(id);
        }
    }
}, 60 * 60 * 1000);

wss.on('close', () => {
    clearInterval(heartbeat);
    clearInterval(cleanup);
});

function shutdown() {
    console.log('🛑 Shutting down...');
    clearInterval(heartbeat);
    clearInterval(cleanup);
    for (const [, req] of pendingRequests) clearTimeout(req.timeout);
    wss.clients.forEach((ws) => {
        try { ws.close(1001, 'server_shutdown'); } catch (_) { /* ignore */ }
    });
    wss.close(() => process.exit(0));
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

console.log('🚀 Friend system server running on port', PORT);