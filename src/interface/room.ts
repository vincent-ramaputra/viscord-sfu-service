import { Producer, Router } from "mediasoup/types";
import { ProducerAppData } from "../schemas/create-producer.schema";

export interface Room {
    router: Router;
    // Lookup index for consuming and GET_PRODUCERS; entries are removed only by each producer's observer 'close'.
    producers: Map<string, { userId: string, producer: Producer<ProducerAppData> }>;
}
