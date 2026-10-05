import z from "zod";

export const ActiveSpeakerStateSchema = z.object({
    speaking: z.boolean()
});

export type ActiveSpeakerState = z.infer<typeof ActiveSpeakerStateSchema>;