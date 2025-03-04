import Papa, { ParseLocalConfig } from "papaparse";
import fs from "fs";

const commands = ["fromCSV"] as const;
type CommandsType = (typeof commands)[number];
type ConfigPropsType = {
  command: CommandsType;
  configMeta: {
    batch: any[];
    conf: Omit<ParseLocalConfig<unknown, any>, "complete">;
    mapFields: (data: Papa.ParseStepResult<unknown>) => any;
  };
};

export class FileParser {
  private config: ConfigPropsType[];
  constructor() {
    this.config = [];
  }
  setConfig(config: ConfigPropsType) {
    this.config.push(config);
    return this;
  }
  parseFile(content: fs.ReadStream, command: CommandsType) {
    for (const config of this.config!) {
      if (config.command === command) {
        this[command](
          content,
          config.configMeta.batch,
          config.configMeta.conf,
          config.configMeta.mapFields
        );
      }
    }
  }

  private fromCSV<T>(
    csvContent: fs.ReadStream,
    batch: T[],
    config: Omit<ParseLocalConfig<unknown, any>, "complete">,
    mapFields: (data: Papa.ParseStepResult<unknown>) => T
  ) {
    return new Promise<void>((res, rej) => {
      Papa.parse(csvContent, {
        ...config,
        step: async (row) => {
          batch.push(mapFields(row));
        },
        complete: async () => {
          console.log("complete");
          await fs.rmSync(csvContent.path, { force: true });
          res();
        },
        error: (error) => {
          console.error("Error parsing CSV:", error);
          rej();
        },
      });
    });
  }
  // async fromCSVEvents<T>(
  //   // batch: T[],
  //   config: Omit<ParseLocalConfig<unknown, any>, "complete">,
  //   mapFields: (data: Papa.ParseStepResult<unknown>) => T
  // ) {
  //   return new Promise<{ id: string; data: T[] }>((res, rej) => {
  //     const batch: T[] = [];
  //     Papa.parse(this.content!, {
  //       ...config,
  //       step: async (row) => {
  //         batch.push(mapFields(row));
  //       },
  //       complete: async () => {
  //         //fs.rmSync(this.content.path, { force: true });
  //         res({ id: randomUUID(), data: batch });
  //       },
  //       error: (error) => {
  //         console.error("Error parsing CSV:", error);
  //         rej();
  //       },
  //     });
  //   });
  // }
}
