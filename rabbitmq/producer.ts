import { Channel } from "amqplib";



export default abstract class Producer<T> {
  constructor(
    protected readonly channel: Channel,
    protected readonly queueName: "job_created" | "file_created" | "batch_created"
  ) {
    this.init();
  }

  private async init() {
    await this.channel.assertQueue(this.queueName, {
      deadLetterExchange: 'dlx',
      deadLetterRoutingKey: `${this.queueName}.dlq`,
      
      durable: true,
    });

  }

  async produceMessage(data: T) {
    console.log(`Sending message to queue: ${this.queueName}`);
    await this.channel.sendToQueue(
      this.queueName,
      Buffer.from(JSON.stringify(data)),
      {
        persistent: true,

        expiration: 24 * 60 * 60 * 1000,
        priority: 0,
        // Add message timestamp
        timestamp: Date.now(),
        // Add correlation ID for tracking
        correlationId: crypto.randomUUID()
      }
    );
  }
}
