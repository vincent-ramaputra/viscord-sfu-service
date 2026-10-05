import z from "zod";

export const CloseProducerSchema = z.object({
    producerId: z.uuid()
});

export type CloseProducer = z.infer<typeof CloseProducerSchema>;