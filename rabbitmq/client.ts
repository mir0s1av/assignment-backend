import { Channel, ChannelModel, connect } from "amqplib";
import config from "./config";

export class RabbitMQClient {
  private constructor() {}

  private static instance: RabbitMQClient;
  private connection: ChannelModel;
  private producerChannel: Channel;
  private isInitialazied = false;
  static getInstance() {
    if (!this.instance) {
      this.instance = new RabbitMQClient();
    }
    return this.instance;
  }
  async initialize() {
    if (this.isInitialazied) {
      return;
    }
    try {
      this.connection = await connect(config.rabbitMQ.url);

      if (!this.producerChannel) {
        this.producerChannel = await this.connection.createChannel();
      }

      this.isInitialazied = true;
    } catch (error) {
      console.error("Errored while trying to initialize RBMQ...", error);
    }
  }
  async getChannel() {
    if (!this.isInitialazied) {
      await this.initialize();
    }
    return this.producerChannel;
  }

  async close() {
    if (this.producerChannel) {
      await this.producerChannel.close();
    }
    if (this.connection) {
      await this.connection.close();
    }
    this.isInitialazied = false;
  }
}
export default RabbitMQClient.getInstance();
