import { months } from "./utils";
import path from "path";
import fs from "fs";

async function downloadFile(url: string, savePath: string): Promise<void> {
  try {
    const response = await fetch(url);
    if (!response.ok)
      throw new Error(`Failed to fetch file: ${response.statusText}`);
    console.log(`File downloaded: ${savePath}`);
    const buffer = await response.arrayBuffer();

    fs.writeFileSync(savePath, Buffer.from(buffer));
    console.log(`File saved: ${savePath}`);
  } catch (error) {
    console.error(`Failed to download ${url}:`, (error as Error).message);
  }
}

//TODO: it would be beneficial to run this process in parallel
async function main(url: string, year: number) {
  // TODO: Implement scraping spend data from gov.uk websites
  try {
    for (const month of months) {
      const apiUrl = `${url}-${month}-${year}`;
      console.log({ apiUrl });
      const response = await fetch(apiUrl);
      if (!response.ok) {
        throw new Error(`Failed to fetch API: ${response.statusText}`);
      }

      const data = await response.json();

      const fileUrl = data?.details?.attachments?.[0]?.url;

      if (!fileUrl) {
        throw new Error(`No file found for ${month}-${year}`);
      }
      const splitUrl = url.split("/");
      const saveDir = path.join(
        __dirname,
        "sample_data",
        splitUrl[splitUrl.length - 1],
        year.toString()
      );
      await fs.mkdirSync(saveDir, { recursive: true });
      const fileName = path.basename(fileUrl);
      const savePath = path.join(saveDir, fileName);

      await downloadFile(fileUrl, savePath);
    }
    main(url, year + 1);
  } catch (error) {
    console.error(
      `Failed to download files for ${url}: ${year} year`,
      (error as Error).message
    );
    process.exit(0);
  }
}

main(
  "https://www.gov.uk/api/content/government/publications/dft-spending-over-25000",
  2021
);
