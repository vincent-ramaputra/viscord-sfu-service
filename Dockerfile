# ---- build stage: full image, has curl + the TypeScript toolchain ----
FROM node:22.13 AS build

WORKDIR /app

COPY package*.json ./

# mediasoup names its prebuilt worker after the host kernel (-kernelN) and has no
# kernel7 build yet, so on newer hosts it falls back to compiling from source.
# Fetch the kernel6 prebuilt ourselves; MEDIASOUP_WORKER_BIN skips that step.
RUN MS_VER=$(node -p "require('./package-lock.json').packages['node_modules/mediasoup'].version") \
    && ARCH=$(node -p process.arch) \
    && mkdir -p /opt/mediasoup \
    && curl -fsSL "https://github.com/versatica/mediasoup/releases/download/${MS_VER}/mediasoup-worker-${MS_VER}-linux-${ARCH}-kernel6.tgz" \
       | tar -xz -C /opt/mediasoup
ENV MEDIASOUP_WORKER_BIN=/opt/mediasoup/mediasoup-worker

RUN npm ci

COPY . .

RUN npm run build && npm prune --omit=dev

# ---- runtime stage: slim image, only what `node dist/index.js` needs ----
FROM node:22.13-slim

WORKDIR /app

ENV NODE_ENV=production
ENV MEDIASOUP_WORKER_BIN=/opt/mediasoup/mediasoup-worker

COPY --from=build /opt/mediasoup/mediasoup-worker /opt/mediasoup/mediasoup-worker
COPY --from=build /app/package.json ./
COPY --from=build /app/node_modules ./node_modules
COPY --from=build /app/dist ./dist

CMD ["node", "dist/index.js"]
