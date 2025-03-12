import { RabbitMQClient } from "../../rabbitmq/client";
import { FilesProducer } from "../../rabbitmq/producers/files.producer";

import { FileCreatedEventType } from "../../types";
import { months } from "../../utils";
import { AbstractCrawler } from "../abstract.crawler";
import { CrawlProps } from "../crawler.interface";
import fs from "fs";

export class HMRCCrawler extends AbstractCrawler {
  constructor(folderPath: string = "./sample_data") {
    super(folderPath);
  }
  async crawl({ url, year, recursive, batchSize, jobId }: CrawlProps): Promise<void> {
    const filesProducer = FilesProducer.getInstance(
      await RabbitMQClient.getInstance().getChannel()
    );
    try {
      for (const month of months) {
        const apiUrl = `${url}-${month}-${year}`;

        const response = await fetch(apiUrl);
        if (!response.ok) {
          continue;
        }

        const data = await response.json();

        const fileUrl = data?.details?.attachments?.[0]?.url;
        const fileName = data?.details?.attachments?.[0]?.filename;

        if (!fileUrl) {
          throw new Error(`⚠️ No file found for ${month}-${year}`);
        }
        const savedPath = await this.createFolderPath(fileUrl);

        if (!fs.existsSync(savedPath)) {
          await this.downloadFile(savedPath, fileUrl);
        }
        const eventSchema: FileCreatedEventType = {
          eventType: "file_created",
          timestamp: new Date().toISOString(),
          eventId: crypto.randomUUID(),
          batchSize,
          metadata: {
            jobId,
            file: {
              name: fileName,
              path: savedPath,
              parse_schema: "govUKdata",
            },
          },
        };
        await filesProducer.produceMessage(eventSchema);
      }
      if (recursive) {
        this.crawl({ url, year: year + 1, batchSize, jobId });
      }
    } catch (error) {
      console.error(
        `⚠️ Failed to download files for ${url}: ${year} year`,
        (error as Error).message
      );
    }
  }
}
