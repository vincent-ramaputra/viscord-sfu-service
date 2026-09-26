import { object, uuid } from "zod";

export const TicketClaims = object({
    sub: uuid(),
    channelId: uuid(),
    jti: uuid()
})