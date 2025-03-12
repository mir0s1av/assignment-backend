export interface CrawlProps {
  url: string;
  year: number;
  
  batchSize: number;
  jobId: string;
  recursive?: boolean;
}
