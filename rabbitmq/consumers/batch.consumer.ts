
import { BaseConsumer } from "../consumer";
import { BatchCreatedEventSchema, BatchCreatedEventType } from "../../types";
import { getDBConnection } from "../../db";

export class BatchConsumer extends BaseConsumer<typeof BatchCreatedEventSchema> {
    protected schema = BatchCreatedEventSchema;
    protected async handleJob({metadata: {file,batch,table,batchId}}: BatchCreatedEventType) {

      const knexDb = await getDBConnection();
      try {
        await knexDb.batchInsert(
            table,
            batch,
            batch.length
        );
        console.log(`✅ Processed batch of ${batch.length} records from ${file.name} :: ${batchId}`);
      } catch (error) {
        console.error('Error inserting batch:', error);
        throw error;
      } finally {
        await knexDb.destroy();
      }
    }
  }