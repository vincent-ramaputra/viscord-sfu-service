import { DefaultEventsMap, Server, Socket } from "socket.io";
import { ACTIVE_SPEAKER_STATE, CLOSE_PRODUCER, PRODUCER_JOINED, SESSION_REPLACED } from "../const/events";
import { ProducerCreatedDTO } from "../dto/producer-created.dto";
import { ActiveSpeakerState } from "../schemas/active-speaker-state.schema";

export interface SocketData {
    userId: string;
    channelId: string;
    sessionId: string;
}

export interface ServerToClientEvents {
    [PRODUCER_JOINED]: (dto: ProducerCreatedDTO) => void;
    [ACTIVE_SPEAKER_STATE]: (dto: ActiveSpeakerState & {userId: string}) => void;
    [CLOSE_PRODUCER]: (dto: { producerId: string }) => void;
    [SESSION_REPLACED]: () => void;
}

export type SfuServer = Server<DefaultEventsMap, ServerToClientEvents, DefaultEventsMap, SocketData>;
export type SfuSocket = Socket<DefaultEventsMap, ServerToClientEvents, DefaultEventsMap, SocketData>;