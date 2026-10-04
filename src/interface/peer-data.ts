import { Consumer, Producer, Transport } from "mediasoup/types";

export interface PeerData {
    userId: string;
    transports: Map<string, Transport>;
    consumers: Map<string, Consumer>;
    producers: Map<string, Producer>;
    isMuted: boolean;
    isDeafened: boolean;
}