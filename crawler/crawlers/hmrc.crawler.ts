import { months } from "../../utils";
import { AbstractCrawler } from "../abstract.crawler";
import { CrawlProps } from "../crawler.interface";

export class HMRCCrawler extends AbstractCrawler {
  constructor(folderPath: string = "./sample_data") {
    super(folderPath);
  }
  async crawl({ url, year, recursive }: CrawlProps): Promise<void> {
    try {
      for (const month of months) {
        const apiUrl = `${url}-${month}-${year}`;
        console.log({ apiUrl });
        const response = await fetch(apiUrl);
        if (!response.ok) {
          continue;
        }

        const data = await response.json();

        const fileUrl = data?.details?.attachments?.[0]?.url;
        console.log({ fileUrl });
        if (!fileUrl) {
          throw new Error(`⚠️ No file found for ${month}-${year}`);
        }
        const savedPath = await this.createFolderPath(fileUrl);
        await this.downloadFile(savedPath, fileUrl, "hmrc.govUKdata");
      }
      if (recursive) {
        this.crawl({ url, year: year + 1 });
      }
    } catch (error) {
      console.error(
        `⚠️ Failed to download files for ${url}: ${year} year`,
        (error as Error).message
      );
      process.exit(0);
    }
  }
}
