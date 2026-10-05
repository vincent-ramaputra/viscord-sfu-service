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
database, no Redis. All media state lives in process memory, so it runs as a **single replica**.

The web client connects **directly** over Socket.IO, not through `ws-gateway`: it asks `guild-service` for a
short-lived **voice ticket** (a JWT) plus the SFU URL, then connects with `auth: { ticket }`
(`web-client/lib/voice/voice-session.ts`, `sfu-client.ts`). Voice *presence* (who is in which channel,
mute/deafen icons, ringing) is owned by `guild-service` + `ws-gateway`. This service moves media and reports
peer joins/leaves to `guild-service` over RabbitMQ so presence can be kept in sync.

## Commands

```bash
npm install
npm run dev      # ts-node src/index.ts (no watch/reload — restart manually)
npm run build    # tsc -> dist/
npm start        # node dist/index.js
```

There are no tests, no linter, and no formatter (`npm test` just errors). `tsconfig.json` has `strict`,
`noUnusedLocals` and `noUnusedParameters`, so `npm run build` is the only check — run it before pushing.
It does not check the wire contract with the client: schema/payload mismatches only show up in a real
join → mute → screen share → leave run against `web-client`.

Required env vars (read via `dotenv`; there is no `.env.example`, and `.env*` is gitignored):
- `PORT` — Socket.IO/HTTP listen port.
- `HOST` — the **public** IP/hostname announced in ICE candidates (`announcedAddress`). If this is wrong,
  signaling works but media never flows. Not validated at startup.
- `RTC_MIN_PORT` / `RTC_MAX_PORT` — UDP port range for WebRTC transports; must be open/exposed.
- `SFU_INSTANCE` — this instance's name, sent in every RabbitMQ event.
- `TICKET_PUBLIC_KEY` — **base64-encoded** SPKI PEM of `guild-service`'s ES256 ticket-signing key.
- `AMQP_URL` — RabbitMQ URL.

On push to `main`, `.github/workflows/build-docker-image.yaml` builds and pushes
`vincentramaputra/viscord-sfu-service:sha-…` to Docker Hub. Unlike the other services, it does **not** bump
the tag in `infra/k8s/services/sfu-service/` (that dir only holds an empty `configmap.yaml`). The
`Dockerfile` is multi-stage and downloads mediasoup's prebuilt `kernel6` worker binary itself
(`MEDIASOUP_WORKER_BIN`), because there is no `kernel7` prebuild and the fallback compiles from source.

Unlike what the workspace-level `CLAUDE.md` says, this file **is** tracked in this repo.

## Architecture

All runtime logic is in `src/index.ts`. Supporting files:
- `src/schemas/` — zod schemas, one per client event, each exporting `XSchema` + `type X = z.infer<…>`.
- `src/interface/socket-protocol.ts` — `SocketData`, `ServerToClientEvents`, `SfuServer`/`SfuSocket`.
  Mirrors `web-client/lib/voice/sfu-protocol.ts`; change both together.
- `src/events/publisher.ts` + `protocol.ts` — RabbitMQ publisher (auto-reconnecting `amqplib` connection),
  typed by the `SfuEvents` map. Event payload interfaces are in `src/interface/*-event.ts`.
- `src/const/events.ts` — copy of the Viscord-wide events file; most constants are unused here. SFU event
  names must match `web-client/constants/events.ts` exactly.
- `src/const/configs.ts` (router codecs: Opus, VP8/VP9/H264; clock tolerance), `src/const/time.ts` (intervals).

### State
- One mediasoup `Worker`, created in `main()` before the server starts listening.
- `rooms: Map<channelId, Room>` — one `Router` per voice channel, created lazily on the first `JOIN_ROOM`,
  plus room-wide maps of transports/producers/consumers (`src/interface/room.ts`).
- `peers: Map<socket.id, PeerData>` — the peer's user/channel/session, its own transports/producers/consumers,
  and the mute/deafen state it joined with.
- `socketByUser: Map<userId, socket.id>` — one voice session per user per SFU (see below).
- `jtiMap` — seen ticket `jti`s until expiry, for replay protection.

Objects are tracked in **both** `room.*` and `peer.*` maps by hand, which is the root cause of the remaining
bookkeeping bugs listed below.

### Connection lifecycle
1. **Ticket auth (`io.use` middleware).** Verifies `handshake.auth.ticket`: ES256 only, `iss: guild-service`,
   `aud: sfu-service`, `maxTokenAge: 60s`, claims parsed by `TicketClaimsSchema`, `jti` rejected if seen.
   Sets `socket.data = { userId: sub, channelId, sessionId: jti }`. The channel always comes from the ticket,
   never from a client payload.
2. **Per-socket state machine** in the `connection` closure: `'none' → 'joining' → 'joined'`.
3. **Per-socket event queue (`serialize`).** Every event from a socket, `disconnect` included, runs one at a
   time on a promise chain. A handler's check → `await` → write can't interleave with another event from the
   same peer; the per-peer caps depend on this, and cleanup waits for an in-flight request instead of
   missing what it creates. Queued tasks must never reject (each catches its own errors), and one that never
   settles stalls that socket's events, cleanup included.
4. **`onRequest` / `onSend` wrappers** register every post-join event. Inside the queue they check the ack is
   a function (requests only), resolve a **`HandlerContext` `{ socket, peer, room }`**
   (`src/interface/handler-context.ts`) — rejected unless the socket is joined, still connected, and its peer
   and room exist — then `safeParse` the payload and run `handler(ctx, payload)` in try/catch. Requests always
   ack exactly once: the result, or `null` on any failure — the client's `sfu-client.ts` turns `null` into an
   error. Handlers are registered directly (`onSend(PAUSE_PRODUCER, PauseProducerSchema, handlePauseProducer)`)
   and never look up or assert the peer/room themselves. JOIN_ROOM, which creates the peer, has its own
   registration with the same contract.
5. **Ownership rule:** anything a handler *acts on* is looked up in `ctx.peer.*` (transports, producers,
   consumers); only reads go through `ctx.room.*` — notably the producer in `CREATE_CONSUMER`, since consuming
   other peers' media is the point.
6. **Per-peer caps:** `MAX_TRANSPORTS_PER_PEER` (2, `src/const/configs.ts`) checked before
   `createWebRtcTransport` (each reserves a UDP port); one producer per `appData.mediaTag`; one live consumer
   per producer (otherwise one client could multiply outgoing bandwidth). Cap hits are logged at warn.
7. **Schema conventions:** mediasoup params (`dtlsParameters`, `rtpParameters`, `rtpCapabilities`) are checked
   shallowly with `z.looseObject({})` and cast at the call site — mediasoup validates them in depth and its
   errors become `ack(null)`. Anything relayed to other clients (`appData.mediaTag`, `{ speaking }`) is
   validated strictly. Fields the server doesn't use are left out so zod strips them. Producer `appData` is
   typed as `ProducerAppData` (derived from `CreateProducerSchema`) in `PeerData` and `Room`.
8. `process.on('unhandledRejection')` only logs — a safety net, not error handling.

### Signaling flow
1. `JOIN_ROOM {isMuted, isDeafened}` → creates the router if needed and the peer, publishes `peer_joined`,
   replaces any previous session of the same user, acks `{ rtpCapabilities }`.
2. `CREATE_TRANSPORT` (twice: send + recv) → WebRTC transport params.
3. `CONNECT_TRANSPORT {transportId, dtlsParameters}` → DTLS handshake.
4. `CREATE_PRODUCER {transportId, kind, rtpParameters, paused, appData: {mediaTag: 'mic'|'screen'}}` →
   broadcasts `PRODUCER_JOINED` to the rest of the room. `mediaTag` must match `kind` (mic ⇔ audio).
5. `GET_PRODUCERS` / `CREATE_CONSUMER {transportId, producerId, rtpCapabilities}` → consume.
6. `PAUSE_/RESUME_PRODUCER {producerId}` (mute), `PAUSE_/RESUME_CONSUMER` (deafen: all of the peer's
   consumers), `CLOSE_PRODUCER`, `CLOSE_CONSUMER`. Mute/deafen are **not** rebroadcast — the UI gets that
   state from `ws-gateway`. `ACTIVE_SPEAKER_STATE {speaking}` is client-computed and rebroadcast with `userId`.
7. `CLOSE_SFU_CLIENT` or socket `disconnect` → `handleCloseClient` closes the peer's producers/consumers/
   transports, broadcasts `CLOSE_PRODUCER`, and publishes `peer_left` (`reason: 'left' | 'dropped'`).

**One session per user:** on join, a previous peer of the same user (any room) is sent `SESSION_REPLACED`
and closed. `peer_joined` is published *before* the replacement on purpose, so `guild-service` stores the
new session first and treats the old one's `peer_left` as stale.

### RabbitMQ events (queue `sfu_events`, `{ pattern, data }`)
`peer_joined`, `peer_left`, `sfu_started` (on every broker (re)connect), `sfu_heartbeat` (every 10s),
`sfu_snapshot` (on connect and every 60s: all current peers). `guild-service` reconciles its voice states
for this `sfuInstance` against the snapshot, which repairs events dropped while the broker link was down.
`publish()` silently drops events while disconnected; there are no publisher confirms.

## Known rough edges (good learning targets — flag these when relevant)

- **Bookkeeping drift between `room.*` and `peer.*`.** `handleCloseProducer` doesn't remove the closed
  consumers from the consuming peers' `peer.consumers`; when a peer leaves, mediasoup closes other peers'
  consumers of its producers but the maps keep them (the consumer cap ignores `closed` ones for this reason).
  The idiomatic fix is to drive cleanup from mediasoup events (`producerclose`, `transportclose`, `observer`
  `close`) with one source of truth.
- **Rooms/routers are never removed** when the last peer leaves.
- **Router creation race:** two first joins to the same channel can both create a router; one leaks. Store
  the creation promise in the map instead.
- **Replaced sessions bypass the queue:** `replacePreviousSession` closes the *old* socket from the *new*
  socket's queue, so a request in flight on the old socket can still create a transport after cleanup.
  Event-driven cleanup would cover it.
- `handleJoinRoom` and `handleCloseClient` still use `rooms.get(…)!` (they run outside `HandlerContext`).
- Transport direction isn't checked: a peer can produce on its own recv transport or consume on its send
  transport. Low severity — only its own transports.
- `ClientToServerEvents` is still `DefaultEventsMap`: the compiler doesn't tie event name ↔ schema ↔ ack
  type, so a handler returning the wrong shape (or nothing) isn't caught.
- **No `worker.on('died')`**: if the worker dies, the process stays up and every call fails. One worker also
  means one CPU core.
- **Env vars are read ad hoc** (`Number(process.env.X)`, `HOST` unchecked) instead of parsed once at startup.
- **SIGTERM** closes everything and exits without publishing `peer_left` or closing AMQP; reconciliation
  relies on `sfu_started`/`sfu_snapshot` after restart. In-memory state ⇒ a restart drops every call, and
  horizontal scaling would need room → instance affinity.
- `enableTcp: true` but `listenInfos` only has a UDP entry, so there is no TCP fallback (and no TURN server
  anywhere in the stack).
- CORS origin is hardcoded to `https://localhost:3002`.
- Leftover debug `console.log`s; no structured logging.
- `PauseProducer`/`ResumeProducer`/`CloseProducer` schemas are three identical `{ producerId }` shapes.
