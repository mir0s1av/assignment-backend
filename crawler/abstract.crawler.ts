import https from "https";
import path from "path";
import fs from "fs";
import { EventSchemaType, FileParsingSchemaType } from "../types";
import { randomUUID } from "crypto";

import { CrawlProps } from "./crawler.interface";
import { createFolder } from "../load-file.main";

async function publishEvent(event: EventSchemaType) {
  try {
    const fileStream = fs.createWriteStream("./events/" + event.id + ".json");
    fileStream.write(JSON.stringify(event, null, 2));
    console.log("✅ Unprocessed event saved.");
  } catch (error) {
    console.error("⚠ Error saving unprocessed event:", error);
  }
}

export abstract class AbstractCrawler {
  protected folderPath: string;

  constructor(folderPath: string) {
    this.folderPath = folderPath;
  }
  abstract crawl({ url, year, recursive }: CrawlProps): Promise<void>;
  async downloadFile(
    savePath: string,
    fileUrl: string,
    schema: FileParsingSchemaType
  ) {
    try {
      if (!fs.existsSync(savePath)) {
        const filename = path.basename(savePath);
        console.log(`Downloading file: ${fileUrl}`);
        https.get(fileUrl, (response) => {
          const fileStream = fs.createWriteStream(savePath);

          response.pipe(fileStream);
          fileStream.on("finish", async () => {
            console.log(
              " 🥳 Data has been written to the destination file. :: " +
                filename
            );

            const eventSchema: EventSchemaType = {
              type: "file_created",
              id: randomUUID(),
              timestamp: new Date().toISOString(),
              metadata: {
                file: {
                  name: filename,
                  path: savePath,
                  url: fileUrl,
                  parse_schema: schema,
                },
              },
            };

            await publishEvent(eventSchema);
          });

          fileStream.on("error", (err) => {
            console.error("⚠️ Error writing to the destination file:", err);
          });
        });
      }
    } catch (error) {
      console.error(
        `⚠️ Failed to download ${fileUrl}:`,
        (error as Error).message
      );
    }
  }

  async createFolderPath(fileUrl: string) {
    const fileName = path.basename(fileUrl);
    createFolder(this.folderPath);

    return path.join(this.folderPath, fileName);
  }
}

export class Crawler<
  T extends new (...args: any[]) => any
> extends AbstractCrawler {
  folderPath: string;

  constructor(private crawlerClass: T, folderPath: string = "./sample_data") {
    super(folderPath);
    this.folderPath = folderPath;
  }

  async crawl(args: CrawlProps): Promise<void> {
    const crawlerInstance = new this.crawlerClass(this.folderPath);
    await crawlerInstance.crawl(args);
  }
}
