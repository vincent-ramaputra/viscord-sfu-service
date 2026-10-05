import { Consumer, Producer, Transport } from "mediasoup/types";
import { ProducerAppData } from "../schemas/create-producer.schema";

export interface PeerData {
    userId: string;
    channelId: string;
    sessionId: string;
    transports: Map<string, Transport>;
    consumers: Map<string, Consumer>;
    producers: Map<string, Producer<ProducerAppData>>;
    isMuted: boolean;
    isDeafened: boolean;
}