import fs from "fs";
import { getDBConnection } from "./db";

import { EventSchemaType, SpendTransaction } from "./types";
import { getArg, mapGovUKData, TEMP_BATCH_FILE } from "./utils";
import path from "path";
import { FileParser } from "./parser/file.parser";

const EVENTS_FOLDER = "./events";
async function loadUnprocessedEvents(): Promise<EventSchemaType[]> {
  try {
    // Check if folder exists
    const files = await fs.readdirSync(EVENTS_FOLDER);
    if (files.length === 0) return [];

    let allEvents: EventSchemaType[] = [];

    for (const file of files) {
      const filePath = path.join(EVENTS_FOLDER, file);

      try {
        const data = await fs.readFileSync(filePath, "utf8");
        const events = JSON.parse(data) as EventSchemaType;
        allEvents.push(events);

        console.log(`✅ Processed: ${file}`);

        await fs.unlinkSync(filePath);
      } catch (error) {
        console.error(`⚠ Error processing file ${file}:`, error);
      }
    }

    return allEvents;
  } catch (error) {
    console.error("⚠ Error loading events:", error);
    return [];
  }
}

async function eventListener(batchSize: number = 100) {
  const existingEvents = await loadUnprocessedEvents();
  console.log(existingEvents.length);

  console.log("👂 Listening to the events");
  fs.watch(EVENTS_FOLDER, async (eventType, fileName) => {
    if (eventType === "rename") {
      const filePath = path.join(EVENTS_FOLDER, fileName!);
      console.log(`🆕 New event added: ${fileName}`);
      console.log(`📖 Reading ${fileName}.`);
      const eventData = fs.readFileSync(filePath, { encoding: "utf8" });
      const event = JSON.parse(eventData) as EventSchemaType;

      existingEvents.push(event);
    }
  });
}
//
// ---------------- DEFAULT BEHAVIOUR----------------
async function processBatches(
  batch: SpendTransaction[],
  batchSize: number,
  knexDb: Awaited<ReturnType<typeof getDBConnection>>
) {
  let lastPersistTime = Date.now();
  let isProcessing = false;
  let totalPRocessed = 0;
  setInterval(async () => {
    if (isProcessing) return;
    const now = Date.now();

    if (
      batch.length >= batchSize ||
      (batch.length > 0 && now - lastPersistTime >= 1000)
    ) {
      const batchToPersist = batch.splice(0, batchSize);
      console.log(`🚀 Persisting batch :: ${batchToPersist.length}`);
      isProcessing = true;
      await knexDb.batchInsert(
        "spend_transactions",
        batchToPersist,
        batchToPersist.length
      );
      totalPRocessed += batchToPersist.length;
      lastPersistTime = Date.now();
      isProcessing = false;
    }
  }, 500);
}
async function saveUnprocessedBatches(batch: SpendTransaction[]) {
  try {
    fs.writeFileSync(TEMP_BATCH_FILE, JSON.stringify(batch, null, 2), "utf8");
    console.log("✅ Unprocessed batches saved.");
    process.exit(0);
  } catch (error) {
    console.error("⚠ Error saving unprocessed batches:", error);
  }
}
async function loadUnprocessedBatches() {
  return new Promise<SpendTransaction[] | []>((resolve, reject) => {
    if (!fs.existsSync(TEMP_BATCH_FILE)) {
      return resolve([]); // Return empty array if file doesn't exist
    }

    const readStream = fs.createReadStream(TEMP_BATCH_FILE, {
      encoding: "utf8",
    });

    let jsonData = "";

    readStream.on("data", (chunk) => {
      jsonData += chunk; // Collecting chunks
    });

    readStream.on("end", () => {
      try {
        console.log("🔄 Restoring unprocessed batches...");
        resolve(JSON.parse(jsonData));
      } catch (error) {
        console.error("⚠ Error parsing unprocessed batches:", error);
        resolve([]); // Return empty array on error
      }
    });

    readStream.on("error", (error) => {
      console.error("⚠ Error reading file:", error);
      reject([]);
    });
  });
}
export function createFolder(folderPath: string) {
  if (!fs.existsSync(folderPath)) {
    console.log("🗂️ Creating folder: " + folderPath);
    fs.mkdirSync(folderPath, { recursive: true });
  }
}
/**
 * This script loads a csv file containg spending data in gov.uk/HMRC format
 * into the `spend_transactions` table in a SQLite database.
 *
 * Some basic validation is performed.
 */

async function listener(folderPath: string, batchSize: number = 100) {
  createFolder(folderPath);
  console.log("👂 Listening to folder :: " + folderPath);
  const knexDb = await getDBConnection();

  let batch = await loadUnprocessedBatches();
  if (batch.length > 0) {
    console.log(`We found ${batch.length} unprocessed batches.`);
  }

  let unprocessedFiles = fs.readdirSync(folderPath);
  if (unprocessedFiles.length > 0) {
    console.log(
      `There are still ${unprocessedFiles.length} unprocessed file(s).`
    );
  }

  const parser = new FileParser().setConfig({
    command: "fromCSV",
    configMeta: {
      batch,
      conf: { header: true, skipEmptyLines: true },
      mapFields: mapGovUKData,
    },
  });

  processBatches(batch, batchSize, knexDb);
  if (unprocessedFiles.length > 0) {
    for (const file of unprocessedFiles) {
      const filePath = path.join(folderPath, file);
      const csvContent = fs.createReadStream(filePath, { encoding: "utf8" });
      console.log(`📖 Reading ${file}.`);
      await parser.parseFile(csvContent, "fromCSV");
    }
  }
  fs.watch(folderPath, async (eventType, fileName) => {
    if (eventType === "rename") {
      const filePath = path.join(folderPath, fileName!);

      if (fs.existsSync(filePath)) {
        console.log(`🆕 New file added: ${fileName}`);
        console.log(`📖 Reading ${fileName}.`);
        const csvContent = fs.createReadStream(filePath, { encoding: "utf8" });
        await parser.parseFile(csvContent, "fromCSV");
      }
    }
  });
  process.on("SIGINT", async () => await saveUnprocessedBatches(batch));
  process.on("SIGTERM", async () => await saveUnprocessedBatches(batch));
}

async function loadFile(filePath: string, batchSize: number = 100) {
  if (!fs.existsSync(filePath)) {
    throw new Error(`File ${filePath} does not exist.`);
  }
  const knexDb = await getDBConnection();

  let batch: SpendTransaction[] = [];
  const parser = new FileParser().setConfig({
    command: "fromCSV",
    configMeta: {
      batch,
      conf: { header: true, skipEmptyLines: true },
      mapFields: mapGovUKData,
    },
  });
  console.log(`Reading ${filePath}.`);
  const csvContent = fs.createReadStream(filePath, { encoding: "utf8" });
  await parser.parseFile(csvContent, "fromCSV");
  // await parseCSVFile(csvContent, batch);
  let batchNumber = 1;

  while (batch.length > 0) {
    const batchToPersist = batch.splice(0, batchSize);

    console.log(`Persisting batch #${batchNumber} :: ${batchToPersist.length}`);
    await knexDb.batchInsert(
      "spend_transactions",
      batchToPersist,
      batchToPersist.length
    );
    batchNumber++;
  }

  await knexDb.destroy();
  console.log("Finished writing to the DB.");
  process.exit(0);
}

const modeArg = getArg("mode");
const filePathArg = getArg("filePath");
const batchSizeArg = getArg("batchSize");
const folderPathArg = getArg("folderPath");

try {
  if (modeArg === "eventListener") {
    eventListener(batchSizeArg ? parseInt(batchSizeArg) : undefined);
  } else if (modeArg === "listener") {
    if (!folderPathArg) {
      throw new Error("🚨 folderPathArg is missing");
    }
    listener(folderPathArg, batchSizeArg ? parseInt(batchSizeArg) : undefined);
  } else {
    if (!filePathArg) {
      throw new Error("🚨 filePathArg is missing");
    }
    loadFile(filePathArg, batchSizeArg ? parseInt(batchSizeArg) : undefined);
  }
} catch (error) {
  console.error((error as Error).message);
  process.exit(1);
}
