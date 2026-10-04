import { PEER_JOINED, PEER_LEFT, SFU_HEARTBEAT, SFU_SNAPSHOT, SFU_STARTED } from "../const/events";
import { PeerJoinedEvent } from "../interface/peer-joined-event";
import { PeerLeftEvent } from "../interface/peer-left-event";
import { SfuHeartbeatEvent } from "../interface/sfu-heartbeat-event";
import { SfuSnapshotEvent } from "../interface/sfu-snapshot-event";
import { SfuStartedEvent } from "../interface/sfu-started-event";

export interface SfuEvents {
    [PEER_JOINED]: PeerJoinedEvent;
    [PEER_LEFT]: PeerLeftEvent;
    [SFU_STARTED]: SfuStartedEvent;
    [SFU_HEARTBEAT]: SfuHeartbeatEvent;
    [SFU_SNAPSHOT]: SfuSnapshotEvent;
}