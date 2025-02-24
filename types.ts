import { z } from "zod";
// Common data format of _some_ of the spend files.
// Might have to support other formats in the future but this is ok for HMRC & DfT
export const GovUKDataSchema = z.object({
  department_family: z.string(),
  entity: z.string(),
  date: z.string(),
  expense_type: z.string(),
  expense_area: z.string(),
  supplier: z.string(),
  transaction_number: z.string(),
  amount: z.string().transform((val) => {
    const cleaned = val.replace(",", "");
    const num = parseFloat(cleaned).toFixed(2);
    return num;
  }),
  description: z.string(),
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
