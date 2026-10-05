import { RouterOptions } from "mediasoup/types";

export const ROUTER_CONFIG: RouterOptions = {
    mediaCodecs: [
        {
            kind: 'audio',
            mimeType: "audio/opus",
            clockRate: 48000,
            channels: 2
        },
        {
            kind: "video",
            mimeType: "video/VP8",
            clockRate: 90000,
            parameters: {
                "x-google-start-bitrate": 1000,
            },
        },
        {
            kind: "video",
            mimeType: "video/VP9",
            clockRate: 90000,
            parameters: {
                "profile-id": 2,
            },
        },
        {
            kind: "video",
            mimeType: "video/H264",
            clockRate: 90000,
            parameters: {
                "packetization-mode": 1,
                "profile-level-id": "42e01f",
            },
        }
    ]
};

export const CLOCK_TOLERANCE_S = 5;
export const MAX_TRANSPORTS_PER_PEER = 2;