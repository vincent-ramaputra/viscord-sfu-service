
export interface PeerLeftEvent {
    userId: string;
    channelId: string;
    sessionId: string;
    reason: 'left' | 'dropped';
    at: number;
}