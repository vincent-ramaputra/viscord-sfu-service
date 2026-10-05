import { PeerData } from "./peer-data";
import { Room } from "./room";
import { SfuSocket } from "./socket-protocol";

/**
 * What every post-join handler runs against, resolved once per event by onRequest/onSend. A handler only runs
 * when both the peer and its room exist, so handlers never look them up (or assert them) themselves.
 */
export interface HandlerContext {
    socket: SfuSocket;
    peer: PeerData;
    room: Room;
}
