import { DateTime } from "luxon";
import { argNames } from "./types";

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

export function getArg(argName: argNames) {
  return process.argv
    .slice(2)
    .find((arg) => arg.startsWith(`${argName}=`))
    ?.split("=")[1];
}

export const TEMP_BATCH_FILE = "./unprocessed_batches.json";
