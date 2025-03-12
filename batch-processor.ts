
import RabbitMQClient  from "./rabbitmq/client";
import { BatchConsumer } from "./rabbitmq/consumers/batch.consumer";


async function main() {
  try {
    await RabbitMQClient.initialize();
    const channel = await RabbitMQClient.getChannel();
    const consumer = new BatchConsumer(channel, "batch_created");
    

    process.on('SIGTERM', async () => {
      console.log('Received SIGTERM. Closing consumer...');
      await consumer.close();
      await RabbitMQClient.close();
      process.exit(0);
    });
    
    await consumer.consumeMessages();
  } catch (error) {
    console.error('Failed to start consumer:', error);
    await RabbitMQClient.close();
    process.exit(1);
  }
}

main();