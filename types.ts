import { z } from "zod";
import { parseDate } from "./utils";
// Common data format of _some_ of the spend files.
// Might have to support other formats in the future but this is ok for HMRC & DfT
export const GovUKDataSchema = z.object({
  department_family: z.string(),
  entity: z.string(),
  date: z.string().transform((date) => {
    const parsedDate = parseDate(date);
    if (!parsedDate) {
      throw new Error(`Invalid date: ${date}`);
    }
    return parsedDate;
  }),
  expense_type: z.string(),
  expense_area: z.string(),
  supplier: z.string(),
  transaction_number: z.string(),
  amount: z.string().transform((val) => {
    const cleaned = val.replace(",", "");

    return parseFloat(cleaned).toFixed(2);
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
  | "folderPath";
