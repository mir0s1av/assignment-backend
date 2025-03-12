import { DateTime } from "luxon";
import {
  argNames,
  FileParsingSchema,
  GovUKDataSchema,
  SpendTransactionType,
  SpendTransactionSchema,
} from "./types";
import { parseAmount } from "./scraperUtils";
import { HMRCCrawler } from "./crawler/crawlers/hmrc.crawler";


import z, { ZodSchema } from "zod";

export function transformKeys(obj: unknown) {
  if (typeof obj !== "object" || obj === null) {
    throw new Error("Invalid input: Expected an object.");
  }
  return Object.entries(obj).reduce((acc, [key, value]) => {
    let newKey = key
      .normalize("NFKD")
      .replace(/[^\w\s]/gi, "")
      .toLowerCase()
      .trim()
      .replace(" ", "_")
      .replace("No", "number")
      .replace("no", "number");
    if (newKey === "") {
      newKey = "amount";
      value = value.replace(/[^\d.,]/g, "");
    }

    acc[newKey] = value;
    return acc;
  }, {} as Record<string, string>);
}

export const dateFormats = [
  "dd-MM-yyyy",
  "dd.MM.yyyy",
  "yyyy",
  "yyyy-MM",
  "yyyyMM",
  "yyyy-MM-dd",
  "yyyyMMdd",
  "yyyy-MM-dd'T'HH",
  "yyyy-MM-dd'T'HH:mm",
  "yyyy-MM-dd'T'HH:mm:ss",
  "yyyy-MM-dd'T'HH:mm:ss.SSS",
  "yyyy-MM-dd'T'HHmm",
  "yyyy-MM-dd'T'HHmmss",
  "yyyy-MM-dd'T'HHmmss.SSS",
  "yyyy-MM-dd'T'HH:mm:ss,SSS",
  "yyyy-'W'ww-e",
  "yyyy'W'wwe",
  "yyyy-'W'ww-e'T'HH:mm:ss.SSS",
  "yyyy'W'wwe'T'HH:mm:ss.SSS",
  "yyyy-DDD",
  "yyyyDDD",
  "yyyy-DDD'T'HH:mm:ss.SSS",
  "HH:mm",
  "HH:mm:ss",
  "HH:mm:ss.SSS",
  "HH:mm:ss,SSS",
  "dd/MM/yyyy",
];

export function parseDate(dateStr: string) {
  for (const format of dateFormats) {
    const dt = DateTime.fromFormat(dateStr, format);
    if (dt.isValid) return dt.toISO();
  }
}

export const months = [
  "january",
  "february",
  "march",
  "april",
  "may",
  "june",
  "july",
  "august",
  "september",
  "october",
  "november",
  "december",
];

export function parseDateFromISO(date: string) {
  return `${date.substring(0, 4)}-${date.substring(4, 6)}-${date.substring(
    6,
    8
  )}`;
}

export function getArg<T extends string>(argName: argNames) {
  return process.argv
    .slice(2)
    .find((arg) => arg.startsWith(`${argName}=`))
    ?.split("=")[1] as T;
}

export const TEMP_BATCH_FILE = "./unprocessed_batches.json";


export const crawlerClassesData = {
  hmrc: HMRCCrawler,
};
export const selector = (key: keyof typeof crawlerClassesData) => {
  return crawlerClassesData[key];
};

interface MappingConfig<T extends ZodSchema> {
  func: (row: Papa.ParseStepResult<unknown>) => z.infer<T>;
  table: 'spend_transactions';
}
export const mapGovUKData = (
  row: Papa.ParseStepResult<unknown>
): SpendTransactionType => {
  const parsedKeysData = transformKeys(row.data);
  const data = GovUKDataSchema.parse(parsedKeysData);
  return {
    buyer_name: data.entity,
    supplier_name: data.supplier,
    amount: parseAmount(data.amount),
    transaction_timestamp: data.date,
  };
};
const mappingConfigs = new Map<typeof FileParsingSchema[number], MappingConfig<typeof SpendTransactionSchema>>([
  ['govUKdata', {
    func: mapGovUKData,
    table: 'spend_transactions',
  }]
]);

export function getMapping(key: typeof FileParsingSchema[number]) {
  const config = mappingConfigs.get(key);
  if (!config) throw new Error(`No mapping config found for key: ${key}`);
  return config;
}
