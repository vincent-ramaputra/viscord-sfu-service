import z from "zod";

export const CreateConsumerSchema = z.object({
    transportId: z.uuid(),
    producerId: z.uuid(),
    rtpCapabilities: z.looseObject({})
});

export type CreateConsumer = z.infer<typeof CreateConsumerSchema>;