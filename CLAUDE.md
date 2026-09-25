# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## How to work with me (the human) in this repo

I'm a junior backend engineer using this project to learn. Act as a senior backend engineer
pairing with me and helping me grow, not just a code generator:

- **Explain the "why", not only the "what".** When you suggest a change, briefly cover the
  trade-offs, the alternative you rejected, and how a senior would reason about it.
- **Point out problems even when I didn't ask.** If you see a bug, security issue, race
  condition, resource leak, or design smell (this codebase has several — see below), name it, rate how
  serious it is, and suggest the idiomatic fix.
- **Prefer teaching-sized steps.** Make the smallest change that works, show me the diff,
  and let me ask follow-ups before moving on. Don't refactor broadly in one shot.
- **Use precise vocabulary** (SFU vs MCU vs mesh, transport/producer/consumer, ICE/DTLS/SRTP, RTP
  capabilities, simulcast, backpressure, idempotency, etc.) and define terms the first time they come up.
- **Push back** when I ask for something that's a bad idea, and say why.
- When reviewing my code, call out what I did well too, so I know what to keep doing.
- **Only write or edit code (or run installs/scaffolding) when I explicitly ask you to.** Default
  mode is guidance: explain the approach, the options, and the trade-offs, and let me write the
  diff myself. This includes exploratory steps like `npm install` to test an approach — check with
  me first, don't just go do it.

## What this service does

`sfu-service` is the WebRTC **Selective Forwarding Unit** for Viscord's voice/video channels and DM calls,
built on **mediasoup 3**. Each client sends its media once (a *producer*) and the SFU forwards it to every
other participant (*consumers*) — no mixing, no transcoding. It is plain Node + TypeScript: no NestJS, no
database, no Redis, no RabbitMQ. All state lives in process memory, so it runs as a **single replica**.

The web client talks to it **directly** over Socket.IO (`io(process.env.NEXT_PUBLIC_SFU_SERVER)` in
`web-client/components/peer-connection-manager/peer-connection-manager.tsx`) — not through `ws-gateway`.
Voice *presence* (who is in which channel, ringing) is owned by `guild-service` + `ws-gateway`; this service
only moves media.

## Commands

```bash
npm install
npm run dev      # ts-node src/index.ts (no watch/reload — restart manually)
npm run build    # tsc -> dist/
npm start        # node dist/index.js
```

There are no tests, no linter, and no formatter configured (`npm test` just errors). `tsconfig.json` has
`strict: true`, so `npm run build` is the only type check — run it before pushing.

Required env vars (read via `dotenv`; there is no `.env.example`, and `.env*` is gitignored):
- `PORT` — Socket.IO/HTTP listen port.
- `HOST` — the **public** IP/hostname announced in ICE candidates (`announcedAddress`). If this is wrong,
  signaling works but media never flows.
- `RTC_MIN_PORT` / `RTC_MAX_PORT` — UDP port range for WebRTC transports; must be open/exposed.

On push to `main`, `.github/workflows/build-docker-image.yaml` builds and pushes
`vincentramaputra/viscord-sfu-service:sha-…` to Docker Hub. Unlike the other services, it does **not** yet
bump the tag in `infra/k8s/services/sfu-service/` (that dir currently only holds an empty `configmap.yaml`).

## Architecture

Everything that runs is in `src/index.ts`:

- One mediasoup `Worker` (a C++ subprocess), created in a fire-and-forget IIFE at startup.
- `rooms: Map<channelId, Room>` — one mediasoup `Router` per voice channel, created lazily on the first
  `JOIN_ROOM`, holding that room's transports/producers/consumers (`src/interface/room.ts`).
- `peers: Map<socket.id, PeerData>` — per-connection `userId` and that peer's own producers/consumers
  (`src/interface/peer-data.ts`).
- Each socket connection captures `currentRoomId` in a closure; every later event uses it implicitly.

Signaling flow (Socket.IO events from `src/const/events.ts`; request/response ones use the ack `callback`):

1. `JOIN_ROOM {channelId, userId}` → joins the Socket.IO room, returns router `rtpCapabilities`.
2. `CREATE_TRANSPORT` (called twice by the client: send + recv) → WebRTC transport params.
3. `CONNECT_TRANSPORT {transportId, dtlsParameters}` → DTLS handshake.
4. `CREATE_PRODUCER` → produce; broadcasts `PRODUCER_JOINED` to the rest of the room.
5. `GET_PRODUCERS` / `CREATE_CONSUMER` → consume existing and newly joined producers.
6. Mute/deafen/screen-share: `PAUSE_/RESUME_PRODUCER`, `PAUSE_/RESUME_CONSUMER` (pauses *all* of the peer's
   consumers = deafen), `CLOSE_PRODUCER`, `CLOSE_CONSUMER`, and `ACTIVE_SPEAKER_STATE` (client-computed,
   just rebroadcast). Each of these rebroadcasts to the room so other clients update their UI.
7. `CLOSE_SFU_CLIENT` → tear down the peer's producers/consumers/transports and disconnect.

`ROUTER_CONFIG` (`src/const/configs.ts`) sets the codecs: Opus audio, VP8/VP9/H264 video.

`src/const/events.ts` is the copy of the Viscord-wide events file — most constants are unused here. The
SFU event names must match `web-client/constants/events.ts` exactly; change both together.
DTOs in `src/dto/` are TypeScript interfaces only — nothing validates payloads at runtime.

`src/handlers/transports.handler.ts` is **dead code**: an older Express-REST version of the SFU (single
global router, audio only). Nothing imports it, and `express` is only a dependency because of it.

## Known rough edges (good learning targets — flag these when relevant)

- **No authentication.** The socket is reached directly (no Traefik `ForwardAuth`), and `userId` in
  `JOIN_ROOM` is whatever the client sends. Anyone who can reach the port can join any `channelId` as any
  user. The fix is to verify a token (or a short-lived join ticket from `guild-service`) on connection.
- **Disconnect cleanup never runs on abrupt disconnects.** Cleanup is bound to `socket.on('close')`, but
  Socket.IO's server-side event is `'disconnect'`. A closed tab or dropped network leaks the peer's
  producers/consumers/transports and leaves other clients consuming a dead producer.
- **`peer.transports` is never populated** (`handleCreateTransport` only adds to `room.transports`), so even
  `CLOSE_SFU_CLIENT` doesn't close the peer's transports.
- **Rooms/routers are never removed** when the last peer leaves — memory grows for the life of the process.
- `handleCloseProducer` removes from `room.producers` but not `peer.producers`; consumers closed there stay
  in the consuming peers' `peer.consumers`.
- **Non-null assertions on `rooms.get(roomId)!`**: any event sent before `JOIN_ROOM` (or after the room is
  gone) throws inside an async handler → unhandled rejection, and the ack callback is never called.
- **Worker startup race / no `died` handler**: a connection arriving before `createWorker()` resolves hits an
  undefined `worker`; if the worker subprocess dies, the service stays up but every call fails. One worker
  also means one CPU core — mediasoup's scaling model is one worker per core with rooms spread across them.
- **In-memory state ⇒ single replica.** A restart drops every call; horizontal scaling would need
  room → instance affinity (and possibly router piping between workers/hosts).
- `enableTcp: true` but `listenInfos` only has a UDP entry, so there is no TCP fallback for clients behind
  UDP-blocking networks (and no TURN server anywhere in the stack).
- CORS origin is hardcoded to `https://localhost:3002` in `src/index.ts`.
- Leftover debug `console.log`s (e.g. `console.log(room.producers.delete(...))`); no structured logging.
