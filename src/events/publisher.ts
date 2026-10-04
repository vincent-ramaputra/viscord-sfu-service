import amqp from "amqplib"
import { SfuEvents } from "./protocol";
import { SFU_QUEUE } from "../const/events";

let connection: amqp.RecoveringChannelModel | undefined;
let channel: amqp.Channel | undefined;

export async function connect(url: string, { heartbeat, onConnected}: { heartbeat: number, onConnected: () => void}) {
    const connectionString = new URL(url);
    connectionString.searchParams.set("heartbeat", String(heartbeat))

    connection = await amqp.connect(connectionString.toString(), {
        recovery: {
            waitForConnect: false,
            maxDelay: 30000,
            setup: async (model: amqp.ChannelModel) => {
                const ch = await model.createChannel();
                await ch.assertQueue(SFU_QUEUE, { durable: true });

                ch.on('error', (err) => console.error('AMQP channel error', err));
                ch.on('close', () => {
                    if (channel === ch) channel = undefined;
                });

                channel = ch;
                onConnected();
            }
        }
    });

    connection.on('error', (err) => console.error('AMQP error', err));
    connection.on('disconnect', (err) => {
        console.error('AMQP disconnected', err);
        channel = undefined;
    });
}

export function publish<E extends keyof SfuEvents>(pattern: E, data: SfuEvents[E]) {
    if (!channel) return;

    try {
        channel.sendToQueue(SFU_QUEUE, Buffer.from(JSON.stringify({ pattern, data })), { persistent: true });
    } catch (error) {
        console.error(`Failed publishing message ${pattern}`, error);
        return;
    }
}