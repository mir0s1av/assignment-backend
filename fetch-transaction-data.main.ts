import { getArg, selector } from "./utils";
import path from "path";
import fs from "fs";
import https from "https";
import { Crawler } from "./crawler/abstract.crawler";
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
            " 🥳 Data has been written to the destination file. :: " + filename
          );
        });

        fileStream.on("error", (err) => {
          console.error("⚠️ Error writing to the destination file:", err);
        });
      });
    }
  } catch (error) {
    console.error(`⚠️ Failed to download ${url}:`, (error as Error).message);
  }
}

//TODO: it would be beneficial to run this process in parallel

type selectorType = "hmrc";
const modeArg = getArg("mode");
const urlArg = getArg("url");
const yearArg = getArg("year");
const folderPathArg = getArg("folderPath");
const crawlerArg = getArg<selectorType>("crawler");

if (!urlArg || !yearArg || !crawlerArg) {
  throw new Error("🚨 Please provide urlArg and yearArg and crawlerArg");
}

new Crawler(selector(crawlerArg), folderPathArg).crawl({
  url: urlArg,
  year: parseInt(yearArg),
  recursive: modeArg === "recursive" ? true : false,
});
