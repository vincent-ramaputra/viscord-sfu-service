import 'dotenv/config';
import { Server, Socket } from "socket.io";
import * as mediasoup from "mediasoup";
import { Room } from "./interface/room";
import { DtlsParameters, RtpParameters, Worker } from "mediasoup/types";
import { CONNECT_TRANSPORT, CREATE_CONSUMER, CREATE_TRANSPORT, CLOSE_SFU_CLIENT, JOIN_ROOM, CREATE_PRODUCER, RESUME_CONSUMER, PAUSE_CONSUMER, GET_PRODUCERS, PRODUCER_JOINED, ACTIVE_SPEAKER_STATE, VOICE_MUTE, RESUME_PRODUCER, PAUSE_PRODUCER, CLOSE_PRODUCER, CLOSE_CONSUMER, SESSION_REPLACED } from "./const/events";
import { CLOCK_TOLERANCE_S, ROUTER_CONFIG } from "./const/configs";
import { PeerData } from "./interface/peer-data";
import { createServer } from "http";
import { ProducerCreatedDTO } from "./dto/producer-created.dto";
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

if (!process.env.SFU_INSTANCE) {
    throw new Error("SFU_INSTANCE env is required");
}

const server = createServer();
const io = new Server(server, {
    cors: {
        origin: 'https://localhost:3002',
        methods: ["GET", "POST"],
    }
});
const bootId = randomUUID();
const sfuInstance: string = process.env.SFU_INSTANCE;

const rooms = new Map<string, Room>();
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

        socket.data.userId = sub;
        socket.data.channelId = channelId;
        socket.data.sessionId = jti;

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

io.on('connection', (socket: Socket) => {
    let state: 'none' | 'joining' | 'joined' = 'none';

    const onRequest = <S extends z.ZodType>(
        event: string,
        schema: S,
        handler: (payload: z.output<S>) => Promise<unknown>,
        options: { requireJoin: boolean } = { requireJoin: true }
    ) => {
        socket.on(event, async (...args: unknown[]) => {
            const ack = args.at(-1);
            if (typeof ack !== 'function') return;
            if (options.requireJoin && state !== 'joined') {
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
                ack(await handler(parsed.data) ?? null);
            } catch (error) {
                console.error(event, socket.id, error);
                ack(null);
            }
        });
    }

    const onSend = <S extends z.ZodType>(
        event: string,
        schema: S,
        handler: (payload: z.output<S>) => Promise<unknown>
    ) => {
        socket.on(event, async (...args: unknown[]) => {
            if (state !== 'joined') return;

            const raw = args.length > 0 ? args[0] : undefined;
            const parsed = schema.safeParse(raw);
            if (!parsed.success) {
                console.warn(event, socket.id, parsed.error, 'invalid payload')
                return;
            }

            try {
                await handler(parsed.data);
            } catch (error) {
                console.error(event, socket.id, error);
            }
        });
    }

    onRequest(JOIN_ROOM, JoinRoomSchema, async (payload) => {
        if (state !== 'none') return null;

        state = 'joining';
        const result = await handleJoinRoom(socket, socket.data.userId, socket.data.channelId, payload);
        if (result === null) {
            socket.disconnect();
            throw new Error('Failed joining room');
        }
        if (socket.disconnected) {
            peers.delete(socket.id);
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
    }, { requireJoin: false });

    onRequest(CREATE_TRANSPORT, z.undefined(), async () => await handleCreateTransport(socket.data.channelId, socket));
    onRequest(CONNECT_TRANSPORT, ConnectTransportSchema, async (payload) => await handleConnectTranport(socket.data.channelId, payload));
    onRequest(CREATE_PRODUCER, CreateProducerSchema, async (payload) => await handleProduce(socket.data.channelId, socket, payload));
    onRequest(CREATE_CONSUMER, CreateConsumerSchema, async (payload) => await handleConsume(socket.data.channelId, payload, socket));
    onRequest(GET_PRODUCERS, z.undefined(), async () => await getProducers(socket.data.channelId));

    onSend(RESUME_CONSUMER, z.undefined(), async () => await handleResumeConsumer(socket.data.channelId, socket));
    onSend(PAUSE_CONSUMER, z.undefined(), async () => await handlePauseConsumer(socket.data.channelId, socket));
    onSend(PAUSE_PRODUCER, PauseProducerSchema, async (payload) => await handlePauseProducer(socket.data.channelId, payload, socket));
    onSend(RESUME_PRODUCER, ResumeProducerSchema, async (payload) => await handleResumeProducer(socket.data.channelId, payload, socket));
    onSend(ACTIVE_SPEAKER_STATE, ActiveSpeakerStateSchema, async (payload) => await handleUpdateActiveSpeakerState(socket.data.channelId, socket, payload));
    onSend(CLOSE_PRODUCER, CloseProducerSchema, async (payload) => await handleCloseProducer(socket.data.channelId, payload, socket));
    onSend(CLOSE_CONSUMER, CloseConsumerSchema, async (payload) => await handleCloseConsumer(socket.data.channelId, payload, socket));
    onSend(CLOSE_SFU_CLIENT, z.undefined(), async () => await handleCloseClient(socket.data.channelId, socket, 'left'));

    socket.on('disconnect', async (reason) => await handleCloseClient(socket.data.channelId, socket, reason === 'client namespace disconnect' ? 'left' : 'dropped'));
    socket.on('reconnect', async () => console.log('client reconnects'))
});

async function handleJoinRoom(socket: Socket, userId: string, roomId: string, payload: JoinRoom) {
    socket.join(roomId);

    if (!rooms.has(roomId)) {
        try {
            const router = await worker.createRouter(ROUTER_CONFIG);
            rooms.set(roomId, {
                router,
                transports: new Map(),
                consumers: new Map(),
                producers: new Map(),
            });
        } catch (error) {
            console.error(error)
            return null;
        }
    }

    peers.set(socket.id, {
        userId: userId,
        channelId: roomId,
        sessionId: socket.data.sessionId,
        consumers: new Map(),
        producers: new Map(),
        transports: new Map(),
        isMuted: payload.isMuted,
        isDeafened: payload.isDeafened
    });

    const room = rooms.get(roomId)!;
    return { rtpCapabilities: room.router.rtpCapabilities };
}

async function handleUpdateActiveSpeakerState(roomId: string, socket: Socket, payload: ActiveSpeakerState) {
    const peer = peers.get(socket.id);
    if (!peer) return null;

    socket.broadcast.to(roomId).emit(ACTIVE_SPEAKER_STATE, { ...payload, userId: peer.userId });
}

async function handleCreateTransport(roomId: string, socket: Socket) {
    const room = rooms.get(roomId)!;
    const peer = peers.get(socket.id)!;
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


    room.transports.set(transport.id, transport);
    peer.transports.set(transport.id, transport);
    return ({
        id: transport.id,
        iceParameters: transport.iceParameters,
        iceCandidates: transport.iceCandidates,
        dtlsParameters: transport.dtlsParameters
    });
}

async function handleConnectTranport(roomId: string, payload: ConnectTransport) {
    const room = rooms.get(roomId)!;

    const transport = room.transports.get(payload.transportId);
    if (!transport) return null;

    await transport.connect({ dtlsParameters: payload.dtlsParameters as DtlsParameters });
    return true;
}

async function handleProduce(roomId: string, socket: Socket, payload: CreateProducer) {
    const room = rooms.get(roomId)!;
    const transport = room.transports.get(payload.transportId);
    const peer = peers.get(socket.id);

    if (!transport || !peer) return null;

    const producer = await transport.produce({ kind: payload.kind, rtpParameters: payload.rtpParameters as RtpParameters, paused: payload.paused, appData: payload.appData });
    console.log('producer appdata', producer.appData);
    room.producers.set(producer.id, { producer: producer, userId: peer.userId });
    peer.producers.set(producer.id, producer);

    socket.broadcast.to(roomId).emit(PRODUCER_JOINED, { producerId: producer.id, userId: peer.userId } as ProducerCreatedDTO);

    return { id: producer.id };
}

async function handleConsume(roomId: string, payload: CreateConsumer, socket: Socket) {
    const room = rooms.get(roomId)!;
    const transport = room.transports.get(payload.transportId);
    const producer = room.producers.get(payload.producerId);
    const peer = peers.get(socket.id)!;

    if (!transport || !producer) return null;

    if (!room.router.canConsume({ producerId: payload.producerId, rtpCapabilities: payload.rtpCapabilities })) return null;

    const consumer = await transport.consume({ producerId: payload.producerId, rtpCapabilities: payload.rtpCapabilities, paused: false, appData: producer.producer.appData });

    room.consumers.set(consumer.id, consumer);
    peer.consumers.set(consumer.id, consumer);
    return { id: consumer.id, producerId: payload.producerId, kind: consumer.kind, rtpParameters: consumer.rtpParameters, appData: consumer.appData };
}

async function handleResumeConsumer(roomId: string, socket: Socket) {
    const peer = peers.get(socket.id);
    if (!peer) return;

    const promises = [];

    try {
        for (const consumer of Array.from(peer?.consumers.values())) {
            promises.push(consumer.resume());
        }
        await Promise.all(promises);
    } catch (error) {
        console.log("error resruming", peer.consumers.size);
    }


    socket.broadcast.to(roomId).emit(RESUME_CONSUMER, { userId: peer.userId });

    return true;
}

async function handlePauseConsumer(roomId: string, socket: Socket) {
    const room = rooms.get(roomId)!;
    const peer = peers.get(socket.id);
    if (!peer) return;

    const promises = [];
    for (const consumer of Array.from(peer?.consumers.values())) {
        promises.push(consumer.pause());
    }

    await Promise.all(promises);


    socket.broadcast.to(roomId).emit(PAUSE_CONSUMER, { userId: peer.userId });

    return true;
}

async function handlePauseProducer(roomId: string, payload: PauseProducer, socket: Socket) {
    const room = rooms.get(roomId);
    const producer = room?.producers.get(payload.producerId);
    if (!producer) return;
    await producer.producer.pause();

    socket.broadcast.to(roomId).emit(PAUSE_PRODUCER, { userId: producer.userId });
    return true;
}


async function handleResumeProducer(roomId: string, payload: ResumeProducer, socket: Socket) {
    const room = rooms.get(roomId)!;
    const producer = room?.producers.get(payload.producerId);
    if (!producer) return;
    await producer.producer.resume();

    socket.broadcast.to(roomId).emit(RESUME_PRODUCER, { userId: producer.userId });
    return true;
}


/**
 * Makes `socket` the user's only session on this SFU. A previous peer of the same user, in any room, is told
 * `session_replaced` (so its client stops instead of reconnecting) and then closed. Without this, a second tab
 * would leave the first one in the call, unlisted but still sending and receiving audio.
 */
async function replacePreviousSession(userId: string, socket: Socket) {
    const previousSocketId = socketByUser.get(userId);
    socketByUser.set(userId, socket.id);

    if (!previousSocketId || previousSocketId === socket.id) return;

    const previousSocket = io.sockets.sockets.get(previousSocketId);
    const previousPeer = peers.get(previousSocketId);
    if (!previousSocket || !previousPeer) return;

    // Delivered before the disconnect that handleCloseClient sends: a socket's packets arrive in order.
    previousSocket.emit(SESSION_REPLACED);
    await handleCloseClient(previousPeer.channelId, previousSocket, 'left');
}

async function handleCloseClient(roomId: string, socket: Socket, reason: 'left' | 'dropped') {
    const room = rooms.get(roomId)!;
    const socketId = socket.id;

    const peer = peers.get(socketId);
    if (!peer) return;

    for (const producer of Array.from(peer.producers.values())) {
        console.log(room.producers.delete(producer.id));
        producer.close();
        socket.broadcast.to(roomId).emit(CLOSE_PRODUCER, { producerId: producer.id });
    }

    for (const consumer of Array.from(peer.consumers.values())) {
        room.consumers.delete(consumer.id);
        consumer.close();
    }

    for (const transport of Array.from(peer.transports.values())) {
        transport.close();
        room.transports.delete(transport.id);
    }

    peers.delete(socketId);
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

async function getProducers(roomId: string) {
    const room = rooms.get(roomId)!;
    return { producers: Array.from(room.producers.values()).map(p => ({ userId: p.userId, producerId: p.producer.id })) };
}

async function handleCloseProducer(roomId: string, payload: CloseProducer, socket: Socket) {
    const room = rooms.get(roomId)!;
    const producer = room.producers.get(payload.producerId);
    if (!producer) return;

    const consumers = Array.from(room.consumers.values()).filter(c => c.producerId === producer?.producer.id);

    for (const consumer of consumers) {
        consumer.close();
        room.consumers.delete(consumer.id);
    }
    producer.producer.close();
    room.producers.delete(producer.producer.id);

    socket.broadcast.to(roomId).emit(CLOSE_PRODUCER, { producerId: payload.producerId });
}

function handleCloseConsumer(roomId: string, payload: CloseConsumer, socket: Socket) {
    console.log('closing consumer', payload.consumerId);
    const peer = peers.get(socket.id);
    const room = rooms.get(roomId)!;
    if (!peer) return;

    const consumer = peer.consumers.get(payload.consumerId);
    if (!consumer?.closed) consumer?.close();

    peer.consumers.delete(payload.consumerId);
    room.consumers.delete(payload.consumerId);
}

process.on('unhandledRejection', (reason) => {
    console.error('unhandled rejection', reason);
})


process.on("SIGTERM", () => {
    console.log("Shutting down SFU service...");

    for (const room of Array.from(rooms.values())) {
        for (const transport of Array.from(room.transports.values())) transport.close();
        for (const producer of Array.from(room.producers.values())) producer.producer.close();
        for (const consumer of Array.from(room.consumers.values())) consumer.close();

        room.router.close();
    }

    worker.close();
    process.exit(0);
});


main().catch((err) => {
    console.error("Failed starting SFU service", err);
    process.exit(1);
});