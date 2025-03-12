import { FileCreatedEventType } from "../../types";
import Producer from "../producer";
import { Channel } from "amqplib";

export class FilesProducer extends Producer<FileCreatedEventType> {
  private static instance: FilesProducer;
  private constructor(channel: Channel) {
    super(channel, "file_created");
  }
  static getInstance(channel: Channel) {
    if (!this.instance) {
      this.instance = new FilesProducer(channel);
    }
    return this.instance;
  }
}
