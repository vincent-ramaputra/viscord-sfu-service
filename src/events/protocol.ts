import { PEER_JOINED, PEER_LEFT } from "../const/events";
import { PeerJoinedEvent } from "../interface/peer-joined-event";
import { PeerLeftEvent } from "../interface/peer-left-event";

export interface SfuEvents {
    [PEER_JOINED]: PeerJoinedEvent,
    [PEER_LEFT]: PeerLeftEvent   
}