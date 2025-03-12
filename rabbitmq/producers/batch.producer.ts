import { BatchCreatedEventType } from "../../types";
import Producer from "../producer";
import { Channel } from "amqplib";

export class BatchProducer extends Producer<BatchCreatedEventType> {
  private static instance: BatchProducer;
  private constructor(channel: Channel) {
    super(channel, "batch_created");
  }
  static getInstance(channel: Channel) {
    if (!this.instance) {
      this.instance = new BatchProducer(channel);
    }
    return this.instance;
  }
}
