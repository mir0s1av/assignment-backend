import { z } from "zod";
import { parseDate } from "./utils";
// Common data format of _some_ of the spend files.
// Might have to support other formats in the future but this is ok for HMRC & DfT
export const GovUKDataSchema = z.object({
  department_family: z.string().optional(),
  entity: z.string(),
  date: z.string().transform((date) => {
    let parsedDate = parseDate(date === "" ? "2020-11-11" : date);

    if (!parsedDate) {
      throw new Error(`Invalid date: ${date}`);
    }
    return parsedDate;
  }),
  expense_type: z.string(),
  expense_area: z.string().optional(),
  supplier: z.string().min(1, "Supplier name cannot be empty"),
  transaction_number: z.string().min(1, "Transaction number cannot be empty"),
  amount: z.string().transform((val) => {
    const cleaned = val.replace(/[^0-9.-]/g, "");
    const parsed = parseFloat(cleaned); 
  
    return isNaN(parsed) ? "0.00" : parsed.toFixed(2); 
  }),
  description: z.string().optional(),
  supplier_postcode: z.string().optional(),
});

export type GovUKData = z.infer<typeof GovUKDataSchema>;

export const SpendTransactionSchema = z.object({
  buyer_name: z.string(),
  supplier_name: z.string(),
  amount: z.number(),
  transaction_timestamp: z.string().refine((date) => !isNaN(Date.parse(date)), {
    message: "Invalid ISO timestamp",
  }),
});

export type SpendTransactionType = z.infer<typeof SpendTransactionSchema>;

export type argNames =
  | "year"
  | "url"
  | "filePath"
  | "mode"
  | "batchSize"
  | "folderPath"
  | "crawler";

export const TopSuppliersDtoSchema = z.object({
  buyer_name: z.string().optional(),
  limit: z.number().default(5),
  from_date: z.string({ message: "Please provide 'from_date'!!!" }),
  to_date: z.string({ message: "Please provide 'to_date'!!!" }),
});

export type TopSuppliersDto = z.infer<typeof TopSuppliersDtoSchema>;

export const FileParsingSchema = ["govUKdata"] as const;
export type FileParsingSchemaType = (typeof FileParsingSchema)[number];

export const EventTypebaseSchema = z.object({
  eventType: z.enum(["job_created", "file_created", "batch_created"]),
  timestamp: z.string().default(new Date().toISOString()),
  eventId: z.string(),
  batchSize: z.number().default(1000),

})


export const CrawlJobSchema = z.object({
  url: z.string(),
  year: z.number(),
  crawlerType: z.enum(["hmrc"]),
  recursive: z.boolean().default(false),
  
});

export type CrawlJobType = z.infer<typeof CrawlJobSchema>;

export const ParseFileJobSchema = z.object({
  filePath: z.string(),
  parse_schema: z.enum(FileParsingSchema),
})


export const JobCreatedEventSchema = EventTypebaseSchema.extend({
  
  metadata: z.union([CrawlJobSchema, ParseFileJobSchema])
});

export type JobCreatedEventType = z.infer<typeof JobCreatedEventSchema>;

const FileSchema = z.object({
  name: z.string(),
  path: z.string(),
  parse_schema: z.enum(FileParsingSchema),
})

export const FileCreatedEventSchema = EventTypebaseSchema.extend({
  metadata: z.object({
    jobId: z.string(),
    file: FileSchema,
  })
});

export type FileCreatedEventType = z.infer<typeof FileCreatedEventSchema>;

export const BatchCreatedEventSchema = EventTypebaseSchema.extend({
  metadata: z.object({
    file: FileSchema,
    batchId: z.string(),
    table: z.enum(["spend_transactions"]),
    batch: z.array(SpendTransactionSchema)
  
  })
});

export type BatchCreatedEventType = z.infer<typeof BatchCreatedEventSchema>;
