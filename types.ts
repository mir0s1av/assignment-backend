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
  supplier: z.string(),
  transaction_number: z.string(),
  amount: z.string().transform((val) => {
    const cleaned = val.replace(",", "");

    return parseFloat(isNaN(parseInt(cleaned)) ? "0" : cleaned).toFixed(2);
  }),
  description: z.string().optional(),
  supplier_postcode: z.string().optional(), // Some suppliers might not have a postcode
});

export type GovUKData = z.infer<typeof GovUKDataSchema>;

export const SpendTransactionSchema = z.object({
  buyer_name: z.string(),
  supplier_name: z.string(),
  amount: z.number().positive("Amount must be a positive number"),
  transaction_timestamp: z.string().refine((date) => !isNaN(Date.parse(date)), {
    message: "Invalid ISO timestamp",
  }),
});

export type SpendTransaction = z.infer<typeof SpendTransactionSchema>;

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

export const FilePrsingSchema = ["hmrc.govUKdata", "nhs.whatEver"] as const;
export type FileParsingSchemaType = (typeof FilePrsingSchema)[number];
export const eventTypeDescription = ["file_created", "file_parsed"] as const;
export const EventSchema = z.object({
  type: z.enum(eventTypeDescription),
  id: z.string(),
  timestamp: z.string(),
  metadata: z.object({
    file: z.object({
      name: z.string(),
      path: z.string(),
      url: z.string(),
      parse_schema: z.enum(FilePrsingSchema),
    }),
  }),
});

export type EventSchemaType = z.infer<typeof EventSchema>;

const BatchTypeSchema = z.object({
  id: z.string(),
  metadata: z.object({
    batch_data: z.array(
      z.union([SpendTransactionSchema, TopSuppliersDtoSchema])
    ),
    database: z.enum(["spend_transactions"]),
    created_at: z.string(),
  }),
});

export type BatchSchemaType = z.infer<typeof BatchTypeSchema>;
