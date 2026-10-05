import z from "zod";

export const ConnectTransportSchema = z.object({
    transportId: z.uuid(),
    dtlsParameters: z.looseObject({})
});

export type ConnectTransport = z.infer<typeof ConnectTransportSchema>;