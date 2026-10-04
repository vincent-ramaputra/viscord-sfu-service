import 'dotenv/config';
import { Server, Socket } from "socket.io";
import * as mediasoup from "mediasoup";
import { Room } from "./interface/room";
import { Worker } from "mediasoup/types";
import { CONNECT_TRANSPORT, CREATE_CONSUMER, CREATE_TRANSPORT, CLOSE_SFU_CLIENT, JOIN_ROOM, CREATE_PRODUCER, RESUME_CONSUMER, PAUSE_CONSUMER, GET_PRODUCERS, PRODUCER_JOINED, ACTIVE_SPEAKER_STATE, VOICE_MUTE, RESUME_PRODUCER, PAUSE_PRODUCER, CLOSE_PRODUCER, CLOSE_CONSUMER } from "./const/events";
import { ConnectTransportDTO } from "./dto/connect-transport.dto";
import { CreateProducerDTO } from "./dto/create-producer.dto";
import { CreateConsumerDTO } from "./dto/create-consumer.dto";
import { CLOCK_TOLERANCE_S, ROUTER_CONFIG } from "./const/configs";
import { PeerData } from "./interface/peer-data";
import { createServer } from "http";
import { ProducerCreatedDTO } from "./dto/producer-created.dto";
import { ActiveSpeakerState as ActiveSpeakerStateDTO } from "./dto/active-speaker-state.dto";
import { errors, importSPKI, jwtVerify } from 'jose';
import { TicketClaims } from './schemas/ticket-claims.schema';
import { JoinRoom, JoinRoomSchema } from './schemas/join-room.schema';

const server = createServer();

const io = new Server(server, {
    cors: {
        origin: 'https://localhost:3002',
        methods: ["GET", "POST"],
    }
});
const rooms = new Map<string, Room>();
const peers = new Map<string, PeerData>();
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

async function main() {
    worker = await mediasoup.createWorker();

    if (!process.env.TICKET_PUBLIC_KEY) {
        throw new Error('TICKET_PUBLIC_KEY env is required');
    }
    const pem = Buffer.from(process.env.TICKET_PUBLIC_KEY, 'base64').toString();
    ticketKey = await importSPKI(pem, 'ES256');

    setInterval(clearExpiredJti, 30000).unref();

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

        const ticketClaims = TicketClaims.safeParse(payload);
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
    let currentRoomId: string;
    let state: 'none' | 'joining' | 'joined' = 'none';
    socket.on(JOIN_ROOM, async (payload: unknown, callback) => {
        if (typeof callback !== 'function') return;

        const userId = socket.data.userId;
        const channelId = socket.data.channelId;
        currentRoomId = channelId;

        if (state !== 'none') {
            callback(null);
            return;
        }

        const joinRoomSchema = JoinRoomSchema.safeParse(payload);
        if (!joinRoomSchema.success) {
            callback(null);
            return;
        }

        state = 'joining';
        const result = await handleJoinRoom(socket, userId, currentRoomId, joinRoomSchema.data);
        if (result === null) socket.disconnect();
        state = 'joined';

        callback(result);
    });
    // Every handler below needs the room and peer that JOIN_ROOM creates. Before that (or while it is
    // still awaiting createRouter) rooms.get(currentRoomId) is undefined, so reject instead of crashing.
    socket.on(CREATE_TRANSPORT, async (callback) => {
        if (state !== 'joined') return callback(null);
        callback(await handleCreateTransport(currentRoomId, socket));
    });
    socket.on(CONNECT_TRANSPORT, async (payload: ConnectTransportDTO, callback) => {
        if (state !== 'joined') return callback(null);
        callback(await handleConnectTranport(currentRoomId, payload));
    });
    socket.on(CREATE_PRODUCER, async (payload: CreateProducerDTO, callback) => {
        if (state !== 'joined') return callback(null);
        callback(await handleProduce(currentRoomId, socket, payload));
    });
    socket.on(CREATE_CONSUMER, async (payload: CreateConsumerDTO, callback) => {
        if (state !== 'joined') return callback(null);
        callback(await handleConsume(currentRoomId, payload, socket));
    });
    socket.on(GET_PRODUCERS, async (callback) => {
        if (state !== 'joined') return callback(null);
        callback(await getProducers(currentRoomId));
    });
    socket.on(RESUME_CONSUMER, async () => {
        if (state !== 'joined') return;
        await handleResumeConsumer(currentRoomId, socket);
    });
    socket.on(PAUSE_CONSUMER, async () => {
        if (state !== 'joined') return;
        await handlePauseConsumer(currentRoomId, socket);
    });
    socket.on(PAUSE_PRODUCER, async ({ producerId }: { producerId: string }) => {
        if (state !== 'joined') return;
        await handlePauseProducer(currentRoomId, producerId, socket);
    });
    socket.on(RESUME_PRODUCER, async ({ producerId }: { producerId: string }) => {
        if (state !== 'joined') return;
        await handleResumeProducer(currentRoomId, producerId, socket);
    });
    socket.on(ACTIVE_SPEAKER_STATE, async (payload: ActiveSpeakerStateDTO) => {
        if (state !== 'joined') return;
        await handleUpdateActiveSpeakerState(currentRoomId, socket, payload);
    });
    socket.on(CLOSE_PRODUCER, async ({ producerId }: { producerId: string }) => {
        if (state !== 'joined') return;
        await handleCloseProducer(currentRoomId, producerId, socket);
    });
    socket.on(CLOSE_CONSUMER, async ({ consumerId }: { consumerId: string }) => {
        if (state !== 'joined') return;
        handleCloseConsumer(currentRoomId, consumerId, socket);
    });
    // Cleanup is deliberately unguarded: it must run in any state, and handleCloseClient returns early when there is no peer.
    socket.on(CLOSE_SFU_CLIENT, async () => await handleCloseClient(currentRoomId, socket));
    socket.on('disconnect', async () => await handleCloseClient(currentRoomId, socket));
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
        consumers: new Map(),
        producers: new Map(),
        transports: new Map(),
        isMuted: payload.isMuted,
        isDeafened: payload.isDeafened
    });

    const room = rooms.get(roomId)!;
    return { rtpCapabilities: room.router.rtpCapabilities };
}

async function handleUpdateActiveSpeakerState(roomId: string, socket: Socket, payload: ActiveSpeakerStateDTO) {
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

async function handleConnectTranport(roomId: string, payload: ConnectTransportDTO) {
    const room = rooms.get(roomId)!;

    const transport = room.transports.get(payload.transportId);
    if (!transport) return null;

    await transport.connect({ dtlsParameters: payload.dtlsParameters });
    return true;
}

async function handleProduce(roomId: string, socket: Socket, payload: CreateProducerDTO) {
    const room = rooms.get(roomId)!;
    const transport = room.transports.get(payload.transportId);
    const peer = peers.get(socket.id);

    if (!transport || !peer) return null;

    const producer = await transport.produce({ kind: payload.kind, rtpParameters: payload.rtpParameters, paused: payload.paused, appData: payload.appData });
    console.log('producer appdata', producer.appData);
    room.producers.set(producer.id, { producer: producer, userId: peer.userId });
    peer.producers.set(producer.id, producer);

    socket.broadcast.to(roomId).emit(PRODUCER_JOINED, { producerId: producer.id, userId: peer.userId } as ProducerCreatedDTO);

    return { id: producer.id };
}

async function handleConsume(roomId: string, payload: CreateConsumerDTO, socket: Socket) {
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

async function handlePauseProducer(roomId: string, producerId: string, socket: Socket) {
    const room = rooms.get(roomId);
    const producer = room?.producers.get(producerId);
    if (!producer) return;
    await producer.producer.pause();

    socket.broadcast.to(roomId).emit(PAUSE_PRODUCER, { userId: producer.userId });
    return true;
}


async function handleResumeProducer(roomId: string, producerId: string, socket: Socket) {
    const room = rooms.get(roomId)!;
    const producer = room?.producers.get(producerId);
    if (!producer) return;
    await producer.producer.resume();

    socket.broadcast.to(roomId).emit(RESUME_PRODUCER, { userId: producer.userId });
    return true;
}


async function handleCloseClient(roomId: string, socket: Socket) {
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
    socket.disconnect();

    return true;
}

async function getProducers(roomId: string) {
    const room = rooms.get(roomId)!;
    return { producers: Array.from(room.producers.values()).map(p => ({ userId: p.userId, producerId: p.producer.id })) };
}

async function handleCloseProducer(roomId: string, producerId: string, socket: Socket) {
    const room = rooms.get(roomId)!;
    const producer = room.producers.get(producerId);
    if (!producer) return;

    const consumers = Array.from(room.consumers.values()).filter(c => c.producerId === producer?.producer.id);

    for (const consumer of consumers) {
        consumer.close();
        room.consumers.delete(consumer.id);
    }
    producer.producer.close();
    room.producers.delete(producer.producer.id);

    socket.broadcast.to(roomId).emit(CLOSE_PRODUCER, { producerId });
}

function handleCloseConsumer(roomId: string, consumerId: string, socket: Socket) {
    console.log('closing consumer', consumerId);
    const peer = peers.get(socket.id);
    const room = rooms.get(roomId)!;
    if (!peer) return;

    const consumer = peer.consumers.get(consumerId);
    if (!consumer?.closed) consumer?.close();

    peer.consumers.delete(consumerId);
    room.consumers.delete(consumerId);
}


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