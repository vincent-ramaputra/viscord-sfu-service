import z from "zod";

export const ResumeConsumerSchema = z.object({
    consumerId: z.uuid()
});

export type ResumeConsumer = z.infer<typeof ResumeConsumerSchema>;