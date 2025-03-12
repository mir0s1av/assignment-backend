import { JobCreatedEventType } from "../../types";
import Producer from "../producer";
import { Channel } from "amqplib";

export class JobsProducer extends Producer<JobCreatedEventType> {
  private static instance: JobsProducer;
  private constructor(channel: Channel) {
    super(channel, "job_created");
  }
  static getInstance(channel: Channel) {
    if (!this.instance) {
      this.instance = new JobsProducer(channel);
    }
    return this.instance;
  }
}
