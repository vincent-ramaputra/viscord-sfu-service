import z from "zod";

export const JoinRoomSchema = z.object({
    isMuted: z.boolean(),
    isDeafened: z.boolean()
});

export type JoinRoom = z.infer<typeof JoinRoomSchema>;