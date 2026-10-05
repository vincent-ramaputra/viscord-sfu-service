import { Consumer, Producer, Transport } from "mediasoup/types";
import { ProducerAppData } from "../schemas/create-producer.schema";
import { Room } from "./room";

export interface PeerData {
    userId: string;
    channelId: string;
    sessionId: string;
    transports: Map<string, Transport>;
    consumers: Map<string, Consumer>;
    producers: Map<string, Producer<ProducerAppData>>;
    room: Room;
    isMuted: boolean;
    isDeafened: boolean;
}