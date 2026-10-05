import z from "zod";

export const CreateProducerSchema = z.object({
    transportId: z.uuid(),
    kind: z.enum(['audio', 'video']),
    rtpParameters: z.looseObject({}),
    paused: z.boolean(),
    appData: z.object({
        mediaTag: z.enum(['screen', 'mic'])
    })
}).refine(p => (p.appData.mediaTag === 'mic') === (p.kind === 'audio'),
    { error: 'mediaTag does not match producer kind' });

export type CreateProducer = z.infer<typeof CreateProducerSchema>;