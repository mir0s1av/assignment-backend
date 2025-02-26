import Papa from "papaparse";
import fs from "fs";
import { getDBConnection } from "./db";
import { parseAmount } from "./scraperUtils";
import { GovUKDataSchema, SpendTransaction } from "./types";
import { getArg, TEMP_BATCH_FILE, transformKeys } from "./utils";
import path from "path";

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
      console.log(`Persisting batch :: ${batchToPersist.length}`);
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
    console.log({ totalPRocessed });
  }, 500);
}

async function parseCSVFile(
  csvContent: fs.ReadStream,
  batch: SpendTransaction[]
) {
  return await new Promise<void>((res, rej) => {
    Papa.parse(csvContent, {
      header: true,
      skipEmptyLines: true,
      step: async (row) => {
        const parsedKeysData = transformKeys(row.data);

        const data = GovUKDataSchema.parse(parsedKeysData);

        batch.push({
          buyer_name: data["entity"],
          supplier_name: data["supplier"],
          amount: parseAmount(data["amount"]),
          transaction_timestamp: data["date"],
        });
      },
      complete: async () => {
        fs.rmSync(csvContent.path, { force: true });
        res();
      },
      error: (error) => {
        console.error("Error parsing CSV:", error);
        rej();
      },
    });
  });
}
function saveUnprocessedBatches(batch: SpendTransaction[]) {
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
        resolve(JSON.parse(jsonData) as SpendTransaction[]);
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
/**
 * This script loads a csv file containg spending data in gov.uk/HMRC format
 * into the `spend_transactions` table in a SQLite database.
 *
 * Some basic validation is performed.
 */

async function listener(folderPath: string, batchSize: number = 100) {
  if (!fs.existsSync(folderPath)) {
    console.log("Creating folder: " + folderPath);
    fs.mkdirSync(folderPath, { recursive: true });
  }
  console.log("Listening to folder :: " + folderPath);
  const knexDb = await getDBConnection();

  let batch: SpendTransaction[] = await loadUnprocessedBatches();
  processBatches(batch, batchSize, knexDb);
  fs.watch(folderPath, async (eventType, fileName) => {
    if (eventType === "rename") {
      const filePath = path.join(folderPath, fileName!);

      // Check if it's a new file (exists in the directory)
      if (fs.existsSync(filePath)) {
        console.log(`🆕 New file added: ${fileName}`);
        console.log(`Reading ${filePath}.`);
        const csvContent = fs.createReadStream(filePath, { encoding: "utf8" });
        await parseCSVFile(csvContent, batch);
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

  console.log(`Reading ${filePath}.`);
  const csvContent = fs.createReadStream(filePath, { encoding: "utf8" });
  await parseCSVFile(csvContent, batch);
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
  if (modeArg === "listener") {
    if (!folderPathArg) {
      throw new Error("Please provide folderPathArg");
    }
    listener(folderPathArg, batchSizeArg ? parseInt(batchSizeArg) : undefined);
  } else {
    if (!filePathArg) {
      throw new Error("Please provide filePathArg");
    }
    loadFile(filePathArg, batchSizeArg ? parseInt(batchSizeArg) : undefined);
  }
} catch (error) {
  console.error((error as Error).message);
  process.exit(1);
}
