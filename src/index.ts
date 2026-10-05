import 'dotenv/config';
import { Server } from "socket.io";
import * as mediasoup from "mediasoup";
import { Room } from "./interface/room";
import { DtlsParameters, RtpParameters, Worker } from "mediasoup/types";
import { CONNECT_TRANSPORT, CREATE_CONSUMER, CREATE_TRANSPORT, CLOSE_SFU_CLIENT, JOIN_ROOM, CREATE_PRODUCER, RESUME_CONSUMER, PAUSE_CONSUMER, GET_PRODUCERS, PRODUCER_JOINED, ACTIVE_SPEAKER_STATE, RESUME_PRODUCER, PAUSE_PRODUCER, CLOSE_PRODUCER, CLOSE_CONSUMER, SESSION_REPLACED } from "./const/events";
import { CLOCK_TOLERANCE_S, MAX_TRANSPORTS_PER_PEER, ROUTER_CONFIG } from "./const/configs";
import { PeerData } from "./interface/peer-data";
import { createServer } from "http";
import { errors, importSPKI, jwtVerify } from 'jose';
import { TicketClaimsSchema } from './schemas/ticket-claims.schema';
import { JoinRoom, JoinRoomSchema } from './schemas/join-room.schema';
import { connect, publish } from './events/publisher';
import { randomUUID } from 'crypto';
import { CLEAR_JTI_INTERVAL_MS, HEARTBEAT_INTERVAL_MS, SNAPSHOT_INTERVAL_MS } from './const/time';
import z from 'zod';
import { ConnectTransport, ConnectTransportSchema } from './schemas/connect-transport.schema';
import { CreateProducer, CreateProducerSchema } from './schemas/create-producer.schema';
import { CreateConsumer, CreateConsumerSchema } from './schemas/create-consumer.schema';
import { PauseProducer, PauseProducerSchema } from './schemas/pause-producer.schema';
import { ResumeProducer, ResumeProducerSchema } from './schemas/resume-producer.schema';
import { ActiveSpeakerState, ActiveSpeakerStateSchema } from './schemas/active-speaker-state.schema';
import { CloseProducer, CloseProducerSchema } from './schemas/close-producer.schema';
import { CloseConsumer, CloseConsumerSchema } from './schemas/close-consumer.schema';
import { SfuServer, SfuSocket } from './interface/socket-protocol';
import { HandlerContext } from './interface/handler-context';

if (!process.env.SFU_INSTANCE) {
    throw new Error("SFU_INSTANCE env is required");
}

const server = createServer();
const io: SfuServer = new Server(server, {
    cors: {
        origin: 'https://localhost:3002',
        methods: ["GET", "POST"],
    }
});
const bootId = randomUUID();
const sfuInstance: string = process.env.SFU_INSTANCE;

const rooms = new Map<string, Promise<Room>>();
const peers = new Map<string, PeerData>();
// userId → socket.id of that user's current peer on this SFU. One voice session per user: a new join replaces the old one.
const socketByUser = new Map<string, string>();
let worker: Worker;

let ticketKey: CryptoKey;
const jtiMap: Map<string, number> = new Map();

function clearExpiredJti() {
    for (const [jti, exp] of jtiMap.entries()) {
        if (exp < Date.now() / 1000) {
            jtiMap.delete(jti);
        }
    }
}

function sendHeartbeat() {
    publish('sfu_heartbeat', {
        sfuInstance,
        bootId
    });
}

// Built and published in one synchronous step, so no peer event can be published between reading `peers` and
// sending the snapshot. On the same channel, every event queued after it describes a later change.
function sendSnapshot() {
    publish('sfu_snapshot', {
        sfuInstance,
        bootId,
        at: Date.now(),
        peers: Array.from(peers.values()).map(peer => ({
            userId: peer.userId,
            channelId: peer.channelId,
            sessionId: peer.sessionId,
            isMuted: peer.isMuted,
            isDeafened: peer.isDeafened,
        })),
    });
}

async function main() {
    worker = await mediasoup.createWorker();

    if (!process.env.TICKET_PUBLIC_KEY) {
        throw new Error('TICKET_PUBLIC_KEY env is required');
    }
    const pem = Buffer.from(process.env.TICKET_PUBLIC_KEY, 'base64').toString();
    ticketKey = await importSPKI(pem, 'ES256');
    setInterval(clearExpiredJti, CLEAR_JTI_INTERVAL_MS).unref();

    if (!process.env.AMQP_URL) {
        throw new Error('AMQP_URL env is required');
    }
    await connect(process.env.AMQP_URL, {
        heartbeat: 30,
        onConnected: () => {
            sendHeartbeat();
            publish('sfu_started', { sfuInstance, bootId, at: Date.now() });
            // Repairs joins and leaves that were dropped while the broker link was down.
            sendSnapshot();
        }
    });

    setInterval(sendHeartbeat, HEARTBEAT_INTERVAL_MS).unref();
    // Periodic too, so any drift (e.g. a peer event lost without publisher confirms) is corrected within a minute.
    setInterval(sendSnapshot, SNAPSHOT_INTERVAL_MS).unref();

    server.listen(Number(process.env.PORT), () => {
        console.log('Server listening on port', process.env.PORT);
    });
}

io.use(async (socket, next) => {
    try {
        const ticket = socket.handshake.auth?.ticket;
        if (typeof ticket !== 'string') {
            return next(new Error('Unauthorized'));
        }

        const { payload } = await jwtVerify(ticket, ticketKey, {
            algorithms: ['ES256'],
            issuer: 'guild-service',
            audience: 'sfu-service',
            maxTokenAge: '60s',
            clockTolerance: CLOCK_TOLERANCE_S
        });

        const ticketClaims = TicketClaimsSchema.safeParse(payload);
        if (!ticketClaims.success) {
            console.error('Failed parsing ticket claims', ticketClaims.error);
            return next(new Error('Unauthorized'));
        }

        const { sub, jti, channelId, exp } = ticketClaims.data;

        if (jtiMap.has(jti)) {
            console.warn('Replayed voice ticket', { jti, sub });
            return next(new Error('Unauthorized'));
        }

        socket.data = { userId: sub, channelId, sessionId: jti };

        jtiMap.set(jti, exp + CLOCK_TOLERANCE_S);

        return next();
    } catch (error) {
        if (error instanceof errors.JOSEError) {
            console.error("JWT Verification failed", error.code);
        }
        else {
            console.warn("JWT Verification failed", error);
        }
        return next(new Error('Unauthorized'));
    }

})

io.on('connection', (socket) => {
    let state: 'none' | 'joining' | 'joined' = 'none';
    let queue: Promise<unknown> = Promise.resolve();

    // Every event from this socket (disconnect included) runs one at a time, so a handler's check, await, then write
    // can't interleave with another event from the same peer: the per-peer caps rely on this, and cleanup waits for
    // an in-flight request instead of missing what it creates. fn must not reject (each one catches its own errors);
    // a task that never settles would stall this socket's events, cleanup included.
    const serialize = (fn: () => Promise<unknown>) => (queue = queue.then(fn, fn));

    // Undefined before JOIN_ROOM has finished, and again once the socket disconnected or the peer is closed (left,
    // dropped, or replaced by a newer session of the same user), so events in either window are rejected instead of
    // reaching a handler. The disconnect check also lets cleanup run right after an in-flight event, not after
    // everything queued behind it.
    const resolveContext = (): HandlerContext | undefined => {
        if (state !== 'joined' || !socket.connected) return undefined;

        const peer = peers.get(socket.id);
        if (!peer) return undefined;

        return { socket, peer, room: peer.room };
    };

    const onRequest = <S extends z.ZodType>(
        event: string,
        schema: S,
        handler: (ctx: HandlerContext, payload: z.output<S>) => unknown
    ) => {
        socket.on(event, (...args: unknown[]) => serialize(async () => {
            const ack = args.at(-1);
            if (typeof ack !== 'function') return;

            const ctx = resolveContext();
            if (!ctx) {
                ack(null);
                return;
            }

            const raw = args.length > 1 ? args[0] : undefined;
            const parsed = schema.safeParse(raw);
            if (!parsed.success) {
                console.warn(event, socket.id, parsed.error, 'invalid payload')
                ack(null);
                return;
            }

            try {
                ack(await handler(ctx, parsed.data) ?? null);
            } catch (error) {
                console.error(event, socket.id, error);
                ack(null);
            }
        }));
    }

    const onSend = <S extends z.ZodType>(
        event: string,
        schema: S,
        handler: (ctx: HandlerContext, payload: z.output<S>) => unknown
    ) => {
        socket.on(event, (...args: unknown[]) => serialize(async () => {
            const ctx = resolveContext();
            if (!ctx) return;

            const raw = args.length > 0 ? args[0] : undefined;
            const parsed = schema.safeParse(raw);
            if (!parsed.success) {
                console.warn(event, socket.id, parsed.error, 'invalid payload')
                return;
            }

            try {
                await handler(ctx, parsed.data);
            } catch (error) {
                console.error(event, socket.id, error);
            }
        }));
    }

    // JOIN_ROOM creates the peer, so it can't go through onRequest, which needs one. Same ack/validation contract.
    socket.on(JOIN_ROOM, (...args: unknown[]) => serialize(async () => {
        const ack = args.at(-1);
        if (typeof ack !== 'function') return;
        if (state !== 'none') {
            ack(null);
            return;
        }

        const raw = args.length > 1 ? args[0] : undefined;
        const parsed = JoinRoomSchema.safeParse(raw);
        if (!parsed.success) {
            console.warn(JOIN_ROOM, socket.id, parsed.error, 'invalid payload')
            ack(null);
            return;
        }

        try {
            ack(await joinRoom(parsed.data) ?? null);
        } catch (error) {
            console.error(JOIN_ROOM, socket.id, error);
            // Without this the socket would stay in 'joining', where every event (JOIN_ROOM included) is rejected.
            socket.disconnect();
            ack(null);
        }
    }));

    const joinRoom = async (payload: JoinRoom) => {
        state = 'joining';
        const result = await handleJoinRoom(socket, socket.data.userId, socket.data.channelId, payload);
        // Disconnected while joining: leave the room here (the peer has no transports yet), so the queued disconnect
        // cleanup finds no peer and publishes no peer_left for a join that never published peer_joined.
        if (socket.disconnected) {
            const peer = peers.get(socket.id);
            if (peer) removePeer(socket.id, peer);
            return;
        }
        state = 'joined';

        publish('peer_joined', {
            userId: socket.data.userId,
            channelId: socket.data.channelId,
            sessionId: socket.data.sessionId,
            bootId,
            sfuInstance,
            at: Date.now(),
            isDeafened: payload.isDeafened,
            isMuted: payload.isMuted
        });

        // After peer_joined on purpose: guild-service stores the new session first, so the old one's peer_left is
        // stale and nobody sees a leave and rejoin
        await replacePreviousSession(socket.data.userId, socket);

        return result;
    };

    onRequest(CREATE_TRANSPORT, z.undefined(), handleCreateTransport);
    onRequest(CONNECT_TRANSPORT, ConnectTransportSchema, handleConnectTransport);
    onRequest(CREATE_PRODUCER, CreateProducerSchema, handleProduce);
    onRequest(CREATE_CONSUMER, CreateConsumerSchema, handleConsume);
    onRequest(GET_PRODUCERS, z.undefined(), getProducers);

    onSend(RESUME_CONSUMER, z.undefined(), handleResumeConsumer);
    onSend(PAUSE_CONSUMER, z.undefined(), handlePauseConsumer);
    onSend(PAUSE_PRODUCER, PauseProducerSchema, handlePauseProducer);
    onSend(RESUME_PRODUCER, ResumeProducerSchema, handleResumeProducer);
    onSend(ACTIVE_SPEAKER_STATE, ActiveSpeakerStateSchema, handleUpdateActiveSpeakerState);
    onSend(CLOSE_PRODUCER, CloseProducerSchema, handleCloseProducer);
    onSend(CLOSE_CONSUMER, CloseConsumerSchema, handleCloseConsumer);
    onSend(CLOSE_SFU_CLIENT, z.undefined(), ({ socket }) => handleCloseClient(socket, 'left'));

    socket.on('disconnect', (reason) => serialize(async () => {
        try {
            await handleCloseClient(socket, reason === 'client namespace disconnect' ? 'left' : 'dropped');
        } catch (error) {
            console.error('disconnect', socket.id, error);
        }
    }));
});

async function createRoom(): Promise<Room> {
    const router = await worker.createRouter(ROUTER_CONFIG);
    return { router, producers: new Map(), peers: new Map() };
}

// Not async on purpose: nothing here yields, so the promise is in the map before any other join can look.
function getOrCreateRoom(channelId: string): Promise<Room> {
    const existing = rooms.get(channelId);
    if (existing) return existing;

    const created = createRoom();          // starts creating; doesn't wait
    rooms.set(channelId, created);         // reserve the channel immediately

    // A failed creation must not stay cached, or every later join to this channel fails with the same error.
    created.catch(() => {
        if (rooms.get(channelId) === created) rooms.delete(channelId);
    });

    return created;
}

async function handleJoinRoom(socket: SfuSocket, userId: string, roomId: string, payload: JoinRoom) {
    socket.join(roomId);

    // The last peer can leave and close the room while this join awaits it; removePeer drops the closed room from
    // `rooms` first, so asking again yields a fresh one.
    let room: Room;
    do {
        room = await getOrCreateRoom(roomId);
    } while (room.router.closed);

    // Added synchronously after the await, so the room can't be emptied and closed under this peer from here on.
    const peerData: PeerData = {
        userId: userId,
        channelId: roomId,
        sessionId: socket.data.sessionId,
        consumers: new Map(),
        producers: new Map(),
        transports: new Map(),
        room: room,
        isMuted: payload.isMuted,
        isDeafened: payload.isDeafened
    };

    room.peers.set(socket.id, peerData);
    peers.set(socket.id, peerData);

    return { rtpCapabilities: room.router.rtpCapabilities };
}

function handleUpdateActiveSpeakerState({ socket, peer }: HandlerContext, payload: ActiveSpeakerState) {
    socket.broadcast.to(peer.channelId).emit(ACTIVE_SPEAKER_STATE, { ...payload, userId: peer.userId });
}

async function handleCreateTransport({ socket, peer, room }: HandlerContext) {
    // Checked before createWebRtcTransport: each transport reserves a UDP port from the shared RTC port range.
    if (peer.transports.size >= MAX_TRANSPORTS_PER_PEER) {
        console.warn('transport cap reached', socket.id);
        return null;
    }

    const transport = await room.router.createWebRtcTransport({
        enableUdp: true,
        enableTcp: true,
        preferUdp: true,
        listenInfos: [
            {
                ip: "0.0.0.0",
                announcedAddress: process.env.HOST,
                portRange: { min: Number(process.env.RTC_MIN_PORT), max: Number(process.env.RTC_MAX_PORT) },
                protocol: "udp",
            }
        ],
    });

    // Fires on every close path (explicit close, router closed), so maps are only ever cleaned here.
    peer.transports.set(transport.id, transport);
    transport.observer.once('close', () => peer.transports.delete(transport.id));

    return ({
        id: transport.id,
        iceParameters: transport.iceParameters,
        iceCandidates: transport.iceCandidates,
        dtlsParameters: transport.dtlsParameters
    });
}

async function handleConnectTransport({ peer }: HandlerContext, payload: ConnectTransport) {
    const transport = peer.transports.get(payload.transportId);
    if (!transport) return null;

    await transport.connect({ dtlsParameters: payload.dtlsParameters as DtlsParameters });
    return true;
}

async function handleProduce({ peer, room, socket }: HandlerContext, payload: CreateProducer) {
    const transport = peer.transports.get(payload.transportId);
    if (!transport) return null;

    if (Array.from(peer.producers.values()).some(p => p.appData.mediaTag === payload.appData.mediaTag)) {
        console.warn('producer cap reached', socket.id, payload.appData.mediaTag);
        return null;
    }

    const producer = await transport.produce({ kind: payload.kind, rtpParameters: payload.rtpParameters as RtpParameters, paused: payload.paused, appData: payload.appData });
    room.producers.set(producer.id, { producer: producer, userId: peer.userId });
    peer.producers.set(producer.id, producer);

    // Fires on every close path: CLOSE_PRODUCER, the peer leaving or being replaced (its transport closes), and
    // the router closing. mediasoup closes the producer's consumers itself, and their own listeners clean the
    // consuming peers' maps. The broadcast lives here too so every path tells the room, not just some of them.
    producer.observer.once('close', () => {
        peer.producers.delete(producer.id);
        room.producers.delete(producer.id);
        socket.broadcast.to(peer.channelId).emit(CLOSE_PRODUCER, { producerId: producer.id });
    });

    socket.broadcast.to(peer.channelId).emit(PRODUCER_JOINED, { producerId: producer.id, userId: peer.userId });

    return { id: producer.id };
}

async function handleConsume({ socket, peer, room }: HandlerContext, payload: CreateConsumer) {
    const transport = peer.transports.get(payload.transportId);
    const producer = room.producers.get(payload.producerId);
    if (!transport || !producer) return null;

    // One live consumer per producer per peer: each consumer is another forwarded copy of the stream, so
    // duplicates would let one client multiply the SFU's outgoing bandwidth.
    if (Array.from(peer.consumers.values()).some(c => c.producerId === payload.producerId)) {
        console.warn('consumer cap reached', socket.id, payload.producerId);
        return null;
    }

    if (!room.router.canConsume({ producerId: payload.producerId, rtpCapabilities: payload.rtpCapabilities })) return null;

    const consumer = await transport.consume({ producerId: payload.producerId, rtpCapabilities: payload.rtpCapabilities, paused: false, appData: producer.producer.appData });

    // Fires on every close path, including the producer or this peer's transport closing.
    peer.consumers.set(consumer.id, consumer);
    consumer.observer.once('close', () => peer.consumers.delete(consumer.id));

    return { id: consumer.id, producerId: payload.producerId, kind: consumer.kind, rtpParameters: consumer.rtpParameters, appData: consumer.appData };
}

async function handleResumeConsumer({ peer }: HandlerContext) {
    const promises = [];

    try {
        for (const consumer of Array.from(peer.consumers.values())) {
            promises.push(consumer.resume());
        }
        await Promise.all(promises);
    } catch (error) {
        console.log("error resruming", peer.consumers.size);
    }


    return true;
}

async function handlePauseConsumer({ peer }: HandlerContext) {
    const promises = [];
    for (const consumer of Array.from(peer.consumers.values())) {
        promises.push(consumer.pause());
    }

    await Promise.all(promises);


    return true;
}

async function handlePauseProducer({ peer }: HandlerContext, payload: PauseProducer) {
    const producer = peer.producers.get(payload.producerId);
    if (!producer) return;
    await producer.pause();

    return true;
}


async function handleResumeProducer({ peer }: HandlerContext, payload: ResumeProducer) {
    const producer = peer.producers.get(payload.producerId);
    if (!producer) return;
    await producer.resume();

    return true;
}


/**
 * Makes `socket` the user's only session on this SFU. A previous peer of the same user, in any room, is told
 * `session_replaced` (so its client stops instead of reconnecting) and then closed. Without this, a second tab
 * would leave the first one in the call, unlisted but still sending and receiving audio.
 */
async function replacePreviousSession(userId: string, socket: SfuSocket) {
    const previousSocketId = socketByUser.get(userId);
    socketByUser.set(userId, socket.id);

    if (!previousSocketId || previousSocketId === socket.id) return;

    const previousSocket = io.sockets.sockets.get(previousSocketId);
    const previousPeer = peers.get(previousSocketId);
    if (!previousSocket || !previousPeer) return;

    // Delivered before the disconnect that handleCloseClient sends: a socket's packets arrive in order.
    previousSocket.emit(SESSION_REPLACED);
    await handleCloseClient(previousSocket, 'left');
}

/**
 * Removes the peer from both indexes and closes its room if it was the last one in it. The only place either index
 * loses a peer, so they can't drift apart.
 */
function removePeer(socketId: string, peer: PeerData) {
    peers.delete(socketId);
    peer.room.peers.delete(socketId);

    if (peer.room.peers.size === 0) {
        // Deleting by channelId can't hit a newer room for the same channel: a room reaches zero peers only once,
        // because the join loop in handleJoinRoom never adds a peer to a closed room. Removed from the map before
        // closing, so a join retrying in that loop gets a fresh room.
        rooms.delete(peer.channelId);
        peer.room.router.close();
    }
}

async function handleCloseClient(socket: SfuSocket, reason: 'left' | 'dropped') {
    const socketId = socket.id;

    const peer = peers.get(socketId);
    if (!peer) return;

    // Every producer and consumer lives on one of the peer's transports, so closing those closes everything; the
    // observer 'close' listeners clean the maps and tell the room about each closed producer.
    for (const transport of Array.from(peer.transports.values())) {
        transport.close();
    }

    removePeer(socketId, peer);
    // Only if it still points here: when a replaced session is closed, the index already points to the new one.
    if (socketByUser.get(peer.userId) === socketId) socketByUser.delete(peer.userId);
    socket.disconnect();

    publish('peer_left', {
        userId: socket.data.userId,
        channelId: socket.data.channelId,
        sessionId: socket.data.sessionId,
        at: Date.now(),
        reason
    });

    return true;
}

function getProducers({ room }: HandlerContext) {
    return { producers: Array.from(room.producers.values()).map(p => ({ userId: p.userId, producerId: p.producer.id })) };
}

// mediasoup closes the producer's consumers; the observer 'close' listeners registered in handleProduce and
// handleConsume clean the maps and broadcast CLOSE_PRODUCER.
function handleCloseProducer({ peer }: HandlerContext, payload: CloseProducer) {
    peer.producers.get(payload.producerId)?.close();
}

// The consumer's observer 'close' listener (handleConsume) removes it from peer.consumers.
function handleCloseConsumer({ peer }: HandlerContext, payload: CloseConsumer) {
    peer.consumers.get(payload.consumerId)?.close();
}

process.on('unhandledRejection', (reason) => {
    console.error('unhandled rejection', reason);
})


process.on("SIGTERM", () => {
    console.log("Shutting down SFU service...");

    // Closing the worker closes every router, and with them every transport, producer and consumer.
    worker.close();
    process.exit(0);
});


main().catch((err) => {
    console.error("Failed starting SFU service", err);
    process.exit(1);
});