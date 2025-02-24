import Papa from "papaparse";
import fs from "fs";
import { DateTime } from "luxon";
import { getDBConnection } from "./db";
import { parseAmount } from "./scraperUtils";
import { GovUKDataSchema, SpendTransaction } from "./types";
import { parseDate, transformKeys } from "./utils";

/**
 * This script loads a csv file containg spending data in gov.uk/HMRC format
 * into the `spend_transactions` table in a SQLite database.
 *
 * Some basic validation is performed.
 */

async function main(fileName: string, batchSize: number = 100) {
  const csvPath = `./sample_data/${fileName}`;

  console.log(`Reading ${csvPath}.`);
  const csvContent = fs.readFileSync(csvPath, { encoding: "utf8" });
  const csvData = Papa.parse(csvContent, {
    header: true,
    skipEmptyLines: true, // some files have empty newlines at the end
  });

  console.log(`Read ${csvData.data.length} transactions.`);
  console.debug(`First row: ${JSON.stringify(csvData.data[0])}`);

  const knexDb = await getDBConnection();

  let rowNum = 1;
  let batch: SpendTransaction[] = [];
  let batchNumber = 0;
  for (const row of csvData.data) {
    try {
      const isLastRow = rowNum === csvData.data.length;

      const spendDataRow = GovUKDataSchema.parse(transformKeys(row));
      // Some files have hundreds of rows with no data at the end, just commas.
      // It's safe to skip these.
      if (spendDataRow.entity === "") {
        continue;
      }

      const isoTsp = parseDate(spendDataRow["date"]);
      if (!isoTsp) {
        throw new Error(
          `Invalid transaction timestamp ${spendDataRow["date"]}.`
        );
      }

      /**
       * Note that we're not specifying `id` here which will be automatically generated,
       * but knex complains about sqlite not supporting default values.
       * It's ok to ignore that warning.
       */

      batch.push({
        buyer_name: spendDataRow["entity"],
        supplier_name: spendDataRow["supplier"],
        amount: parseAmount(spendDataRow["amount"]),
        transaction_timestamp: isoTsp,
      });
      if (batch.length >= batchSize) {
        console.log(`Persisting batch #${batchNumber} :: ${batch.length}`);
        await knexDb.batchInsert("spend_transactions", batch, batch.length);
        batch = [];
        batchNumber++;
      }
      if (isLastRow) {
        console.log(`Persisting last batch ${batchNumber} ::  ${batch.length}`);

        await knexDb.batchInsert("spend_transactions", batch, batch.length);
      }
      ++rowNum;
    } catch (e) {
      // Re-throw all errors, but log some useful info
      console.error(`Failed to process row ${rowNum}: ${JSON.stringify(row)}`);
      throw e;
    }
  }

  console.log("Finished writing to the DB.");
  await knexDb.destroy();
}

main("HMRC_spending_over_25000_for_August_2023.csv");
