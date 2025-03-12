import {
  BatchCreatedEventType,
  FileCreatedEventSchema,
  FileCreatedEventType,
} from "../../types";
import { BaseConsumer } from "../consumer";
import { FileParser } from "../../parser/file.parser";
import { getMapping } from "../../utils";

import fs from "fs";
import { randomUUID } from "crypto";
import { BatchProducer } from "../producers/batch.producer";
import { getDBConnection } from "../../db";

export class FilesConsumer extends BaseConsumer<typeof FileCreatedEventSchema> {
  protected schema = FileCreatedEventSchema;

  protected async handleJob({
    metadata: { file },
    batchSize,
  }: FileCreatedEventType) {
    const parser = FileParser.getInstance();
    const mappingConfig = getMapping(file.parse_schema);
    const batch: ReturnType<typeof mappingConfig.func>[] = [];
    const batchProducer = BatchProducer.getInstance(this.channel);

    parser.setConfig({
      command: "fromCSV",
      configMeta: {
        conf: { header: true, skipEmptyLines: true },
        mapFields: mappingConfig.func,
      },
    });

    try {
      console.log(`📖 Reading ${file.name}.`);

      const knexDb = await getDBConnection();
      const existingFile = await knexDb("parsed_files")
        .where("file_name", file.name)
        .first();
      if (existingFile) {
        console.log(`📖 ${file.name} already parsed. Skipping...`);
        return;
      }

      const content = fs.createReadStream(file.path, { encoding: "utf8" });

      await parser.parseFile(content, "fromCSV", batch);
      let i = 1;
      while (batch.length > 0) {
        const batchChunk = batch.splice(0, batchSize);

        const eventId = randomUUID();

        const batchEvent: BatchCreatedEventType = {
          eventId,
          timestamp: new Date().toISOString(),
          eventType: "batch_created",
          batchSize,
          metadata: {
            batchId: `${file.name}-${eventId}/${i}`,
            table: mappingConfig.table,
            batch: batchChunk,
            file: {
              name: file.name,
              path: file.path,
              parse_schema: file.parse_schema,
            },
          },
        };

        await batchProducer.produceMessage(batchEvent);
        i++
      }
      await knexDb("parsed_files").insert({
        file_name: file.name,
        file_url: file.path,
      });
    } catch (error) {
      console.error("Error processing file:", error);
      throw Error("Error processing file");
    }
  }
}
