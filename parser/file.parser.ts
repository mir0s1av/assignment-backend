import Papa, { ParseLocalConfig } from "papaparse";
import fs from "fs";

const commands = ["fromCSV"] as const;

type CommandsType = (typeof commands)[number];
type ConfigPropsType = {
  command: CommandsType;
  configMeta: {
    conf: Omit<ParseLocalConfig<unknown, any>, "complete">;
    mapFields: (data: Papa.ParseStepResult<unknown>) => any;
  };
};

export class FileParser {
  private static instance: FileParser;
  private config: Map<CommandsType,ConfigPropsType>;

  private constructor() {
    this.config = new Map<CommandsType,ConfigPropsType>();
  }

  public static getInstance(): FileParser {
    if (!FileParser.instance) {
      FileParser.instance = new FileParser();
    }
    return FileParser.instance;
  }

  setConfig(config: ConfigPropsType) {
    this.config.set(config.command,config);
    return this;
  }

  async parseFile<T>(content: fs.ReadStream, command: CommandsType,batch:T[]) {
    const config= this.config.get(command);
    if(!config){
      throw new Error(`Command ${command} not found`);
    }
    await this[command](
      content,
      batch,
      config.configMeta.conf,
      config.configMeta.mapFields
    )
    
  }

  private async fromCSV<T>(
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
          console.log("completed");
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
}
