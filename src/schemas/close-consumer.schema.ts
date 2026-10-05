import z from "zod";

export const CloseConsumerSchema = z.object({
    consumerId: z.uuid()
});

export type CloseConsumer = z.infer<typeof CloseConsumerSchema>;