import { Channel, ConsumeMessage } from "amqplib";
import { IConsumer } from "./consumer.interface";
import { z, ZodSchema } from "zod";



export abstract class BaseConsumer<T extends ZodSchema> implements IConsumer {
  protected schema: T;
  private readonly MAX_RETRIES = 3;
  private readonly DLQ_SUFFIX = '.dlq';

  constructor(
    protected readonly channel: Channel,
    protected readonly queue: string,
  ) {}



  protected async handleMessage(message: ConsumeMessage) {
    const retryCount = (parseInt(message.properties.headers?.['x-retry-count'] as string) || 0);

    try {
      const jsonMessage = JSON.parse(message.content.toString());

      const parsedMessage = this.schema.parse(jsonMessage);
      await this.handleJob(parsedMessage);
      this.channel.ack(message);
    } catch (error) {
      console.error('Error processing message:', error);
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      
      if (retryCount >= this.MAX_RETRIES) {
        console.log(`Message failed after ${retryCount} retries, moving to DLQ`);
        await this.channel.sendToQueue(
          this.queue + this.DLQ_SUFFIX,
          message.content,
          { headers: { error: errorMessage, originalQueue: this.queue } }
        );
        this.channel.ack(message);
      } else {
        const newHeaders = {
          ...message.properties.headers,
          'x-retry-count': retryCount + 1
        };
        
        await this.channel.sendToQueue(
          this.queue,
          message.content,
          { headers: newHeaders }
        );
        this.channel.ack(message);
      }
    }
  }

  protected abstract handleJob(message: z.infer<T>): Promise<void>;

  public async consumeMessages() {
    console.log("Waiting for messages...");
    await this.channel.prefetch(1);
    
    this.channel.consume(
      this.queue,
      async (message: ConsumeMessage | null) => {
        if (message) {
          console.log(`Received a message :: ${message.content.toString()}`);
          await this.handleMessage(message);
        }
      }
    ).catch(err => {
      console.error('Error consuming messages:', err);
    });
  }

  public async close() {
    await this.channel.close();
  }
}



