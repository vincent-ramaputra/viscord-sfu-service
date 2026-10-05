import { Consumer, Producer, Router, WebRtcTransport } from "mediasoup/types";

export interface Room {
    router: Router;
    transports: Map<string, WebRtcTransport>;
    consumers: Map<string, Consumer>;
    producers: Map<string, { userId: string, producer: Producer }>;
}