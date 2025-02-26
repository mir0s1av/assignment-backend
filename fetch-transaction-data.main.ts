import { months } from "./utils";
import path from "path";
import fs from "fs";
import https from "https";
async function downloadFile(url: string, savePath: string): Promise<void> {
  try {
    if (!fs.existsSync(savePath)) {
      const filename = path.basename(savePath);
      console.log(`Downloading file: ${url}`);
      https.get(url, (response) => {
        const fileStream = fs.createWriteStream(savePath);
        response.pipe(fileStream);
        fileStream.on("finish", () => {
          console.log(
            "Data has been written to the destination file. :: " + filename
          );
        });

        fileStream.on("error", (err) => {
          console.error("Error writing to the destination file:", err);
        });
      });
    }
  } catch (error) {
    console.error(`Failed to download ${url}:`, (error as Error).message);
  }
}

//TODO: it would be beneficial to run this process in parallel
async function main(url: string, year: number) {
  try {
    for (const month of months) {
      const apiUrl = `${url}-${month}-${year}`;

      const response = await fetch(apiUrl);
      if (!response.ok) {
        throw new Error(`Failed to fetch API: ${response.statusText}`);
      }

      const data = await response.json();

      const fileUrl = data?.details?.attachments?.[0]?.url;

      if (!fileUrl) {
        throw new Error(`No file found for ${month}-${year}`);
      }

      const fileName = path.basename(fileUrl);
      const savePath = path.join("./sample_data", fileName);

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
