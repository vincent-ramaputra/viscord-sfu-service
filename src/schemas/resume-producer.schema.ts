import z from "zod";

export const ResumeProducerSchema = z.object({
    producerId: z.uuid()
});

export type ResumeProducer = z.infer<typeof ResumeProducerSchema>;