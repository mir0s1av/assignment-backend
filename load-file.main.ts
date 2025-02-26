import Papa from "papaparse";
import fs from "fs";
import { getDBConnection } from "./db";
import { parseAmount } from "./scraperUtils";
import { GovUKDataSchema, SpendTransaction } from "./types";
import { transformKeys } from "./utils";

/**
 * This script loads a csv file containg spending data in gov.uk/HMRC format
 * into the `spend_transactions` table in a SQLite database.
 *
 * Some basic validation is performed.
 */

async function main(fileName: string, batchSize: number = 100) {
  const csvPath = `./sample_data/${fileName}`;
  const knexDb = await getDBConnection();

  let batch: SpendTransaction[] = [];

  console.log(`Reading ${csvPath}.`);
  const csvContent = fs.createReadStream(csvPath, { encoding: "utf8" });
  await new Promise<void>((res, rej) => {
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
        res();
      },
      error: (error) => {
        console.error("Error parsing CSV:", error);
        rej();
      },
    });
  });

  let batchNumber = 1;
  while (batch.length > 0) {
    const batchToPersist = batch.splice(0, 10);

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

main("HMRC_spending_over_25000_for_August_2023.csv");
