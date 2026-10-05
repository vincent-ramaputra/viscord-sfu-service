import z from "zod";

export const PauseConsumerSchema = z.object({
    consumerId: z.uuid()
});

export type PauseConsumer = z.infer<typeof PauseConsumerSchema>;