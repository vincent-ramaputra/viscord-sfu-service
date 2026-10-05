import z from "zod";

export const PauseProducerSchema = z.object({
    producerId: z.uuid()
});

export type PauseProducer = z.infer<typeof PauseProducerSchema>;