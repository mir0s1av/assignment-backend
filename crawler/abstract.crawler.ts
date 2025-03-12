import https from "https";
import path from "path";
import fs from "fs";
import { CrawlProps } from "./crawler.interface";
import { crawlerClassesData } from "../utils";




export abstract class AbstractCrawler {
  constructor(protected folderPath: string) {
    this.folderPath = folderPath;
  }
  abstract crawl({ url, year, recursive }: CrawlProps): Promise<void>;
  protected async downloadFile(savePath: string, fileUrl: string): Promise<void> {
    if (fs.existsSync(savePath)) {
      return;
    }

    const filename = path.basename(savePath);
    console.log(`Downloading file: ${fileUrl}`);

    return new Promise((resolve, reject) => {
      const fileStream = fs.createWriteStream(savePath);

      https.get(fileUrl, (response) => {
        response.pipe(fileStream);

        fileStream.on("finish", () => {
          console.log(`🥳 Data has been written to: ${filename}`);
          fileStream.close();
          resolve();
        });

        fileStream.on("error", (err) => {
          fs.unlink(savePath, () => {
            console.error("⚠️ Error writing to file:", err);
            reject(err);
          });
        });
      }).on("error", (err) => {
        fs.unlink(savePath, () => {
          console.error(`⚠️ Failed to download ${fileUrl}:`, err.message);
          reject(err);
        });
      });
    });
  }

  protected async createFolderPath(fileUrl: string) {
    const fileName = path.basename(fileUrl);
    fs.mkdirSync(this.folderPath, { recursive: true });

    return path.join(this.folderPath, fileName);
  }
}

export class Crawler extends AbstractCrawler {
  private static instance: Crawler;
  private constructor(
    private crawlerClass: new (...args: any[]) => any,
    folderPath: string = "./sample_data"
  ) {
    super(folderPath);
    this.folderPath = folderPath;
  }
  static getInstance(key: keyof typeof crawlerClassesData) {
    if (!this.instance) {
      this.instance = new Crawler(crawlerClassesData[key]);
    }
    return this.instance;
  }
  async crawl(args: CrawlProps): Promise<void> {
    const crawlerInstance = new this.crawlerClass(this.folderPath);
    await crawlerInstance.crawl(args);
  }

}
