# Stotles Backend Enginer work sample assignment

## Problem statement

All the instructions are available [here](https://www.notion.so/stotles/Backend-engineer-work-sample-assignment-15b1dd4d10d3430a8735cd3b2f12ade7).

### Summary of requirements

See the instructions (linked above) for warm-up task and full problem statement.

The core requirements are:

1. Fix the failing test so that we can correctly parse transaction amounts.
2. `fetch-transaction-data` should load all files published since 2020 from the following links:
   1. https://www.gov.uk/government/collections/spending-over-25-000
   2. https://www.gov.uk/government/collections/dft-departmental-spending-over-25000
3. A new API `/api/top_suppliers` should accept a POST request containing (optional) buyer name and time range (from/to timestamps in ISO format) and return an object containing an array supplier names & total values

   Sample request:

   ```tsx
   {
      "buyer_name": "HMRC",
      "from_date": "20210101",
      "to_date": "20210131",
   }
   ```

   or:

   ```tsx
   {
      "from_date": "20210101",
      "to_date": "20210131",
   }
   ```

   Sample response:

   ```tsx
   {
      "top_suppliers": [
         { "name": "Stotles", "total_amount": 1234567.0 }
      ]
   }
   ```

4. In the README file, please make a note of the result of the query for HMRC for all transactions in 2021.

## Code structure

The codebase is composed of:

- `load-file.main.ts` - script used to load a single CSV file from disk
- `fetch-transaction-data.main.ts` - script used to fetch data from gov.uk API
- `query-service.main.ts` - HTTP API server for querying the data

Some shared code has been extracted to other files - `db.ts` & `scraperUtils.ts` -
feel free to refactor the code more if needed.

### Libraries

The code makes use of the following libraries:

- expressjs - [documentation](https://expressjs.com/)
- knex - [documentation](https://knexjs.org/)
- luxon - [documentation](https://moment.github.io/luxon/)

## Getting started

You can run `ts-node` to execute each of these or use scripts defined in package.json:

```bash
# Starts the query service with --watch so it auto-reloads
npm run dev-query-service
# Runs the scraper
npm run dev-fetch-transaction-data
# Runs the file loader
npm run dev-load-file
```

The first time you run any script that accesses the db (calls `getDBConnection()`),
it will create db.sqlite3 file if it doesn't exist.

At any point you can delete that file and it will be recreated from scratch.

### Browsing the database

You should start by looking at the migration in `./migrations` folder.
If you prefer to browse the DB using SQL, you can use the sqlite command line (just run `sqlite3 ./db.sqlite3`)
or any other SQL client that supports sqlite.

If for any reason the database becomes unusable, you can just delete the db.sqlite3 file and it will be recreated (including running the migrations) next time you run any script.

### Disabling/Enabling TypeScript

If you prefer to completely disable TypeScript for a file, add `// @ts-nocheck` on the first line.
If you just want to disable strict type checking, modify `tsconfig.json` according to your needs.

# Candidate's notes

TODO: Add your notes here

Completed tasks:

1.  Tests are fixed
2.  load-file.main is now parsing and loading data from provided files as well as supporting batching
3.  api/top_suppliers POST request is not ready and working
    example (` "top_suppliers": [
    {
        "name": "FUJITSU SERVICES LTD",
        "total": 12735180.529999997
    },
    {
        "name": "CAPGEMINI",
        "total": 12668005.490000002
    },
    {
        "name": "EQUAL EXPERTS UK LTD",
        "total": 3883243.77
    },
    {
        "name": "Amazon Web Services UK",
        "total": 3726500.06
    },
    {
        "name": "INTEGRATED DEBT SERVICES LTD T/AS",
        "total": 3722464.23
    }
]`)

TODO Tasks:

1.  We need to verify the bpdy payload for the post request
2.  Files saving can be improved, need to figure out what's the best approach (use name and year folders or just dump everything in one folder)
3.  I ran out of time and was able only process data from HMRC_spending_over_25000 files
4.  Need to figure out what is the best approach to read files data( can be done so it reads the whole folder rather than running files manually)
