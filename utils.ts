import { DateTime } from "luxon";

export function transformKeys(obj: unknown) {
  if (typeof obj !== "object" || obj === null) {
    throw new Error("Invalid input: Expected an object.");
  }
  return Object.keys(obj).reduce((acc, key) => {
    const newKey = key.toLowerCase().replace(" ", "_");
    const val = (obj as Record<string, string>)[key];
    acc[newKey] = val;
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
];

export function parseDate(dateStr: string) {
  for (const format of dateFormats) {
    const dt = DateTime.fromFormat(dateStr, format);
    if (dt.isValid) return dt.toISO();
  }
}
