import {
  CrawlJobSchema,
  FileCreatedEventType,
  JobCreatedEventType,
  ParseFileJobSchema,
} from "../../types";

import { BaseConsumer } from "../consumer";
import { JobCreatedEventSchema } from "../../types";
import { Crawler } from "../../crawler/abstract.crawler";
import { FilesProducer } from "../producers/files.producer";
import { RabbitMQClient } from "../client";
import { randomUUID } from "crypto";
export class JobsConsumer extends BaseConsumer<typeof JobCreatedEventSchema> {
  protected schema = JobCreatedEventSchema;

  protected async handleJob(jobData: JobCreatedEventType) {
    if ("filePath" in jobData.metadata) {
      const metadata = ParseFileJobSchema.parse(jobData.metadata);
      const filesProducer = FilesProducer.getInstance(
        await RabbitMQClient.getInstance().getChannel()
      );
      const filePathSplitted = metadata.filePath.split("/");


      const fileCreatedEvent: FileCreatedEventType = {
        eventType: "file_created",
        timestamp: new Date().toISOString(),
        eventId: randomUUID(),
        batchSize: jobData.batchSize,
        metadata: {
          jobId: jobData.eventId,
          file: {
            name: filePathSplitted.at(-1)!,
            path: metadata.filePath,
            parse_schema: metadata.parse_schema,
          },
        },
      };
      console.log(fileCreatedEvent);
      await filesProducer.produceMessage(fileCreatedEvent);
    } else {
      const metadata = CrawlJobSchema.parse(jobData.metadata);
      await Crawler.getInstance(metadata.crawlerType).crawl({
        jobId: jobData.eventId,
        url: metadata.url,
        year: metadata.year,
        recursive: metadata.recursive,
        batchSize: jobData.batchSize,
      });
    }
  }
}
