
export interface PeerJoinedEvent {
    userId: string;
    channelId: string;
    sessionId: string;
    sfuInstance: string;
    bootId: string;
    isMuted: boolean;
    isDeafened: boolean;
    at: number;
}