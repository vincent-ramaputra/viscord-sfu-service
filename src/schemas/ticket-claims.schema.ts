import z from "zod";

export const TicketClaimsSchema = z.object({
    sub: z.uuid(),
    channelId: z.uuid(),
    jti: z.uuid(),
    exp: z.number(),
});

export type TicketClaims = z.infer<typeof TicketClaimsSchema>;