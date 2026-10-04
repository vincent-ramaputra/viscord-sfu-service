
export interface SfuSnapshotPeer {
    userId: string;
    channelId: string;
    sessionId: string;
    isMuted: boolean;
    isDeafened: boolean;
}

/**
 * Every peer this SFU process currently has. guild-service compares it with its voice states for this
 * sfuInstance and repairs the difference (joins or leaves lost while the RabbitMQ link was down).
 * isMuted / isDeafened are the values sent with JOIN_ROOM; later toggles go through the gateway, so
 * guild-service should only use them for peers it has no state for.
 */
export interface SfuSnapshotEvent {
    sfuInstance: string;
    bootId: string;
    at: number;
    peers: SfuSnapshotPeer[];
}
