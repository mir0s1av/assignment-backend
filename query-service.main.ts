import express, { NextFunction, Request, Response } from "express";
import { getDBConnection } from "./db";

import { parseDateFromISO } from "./utils";

/**
 * This file has little structure and doesn't represent production quality code.
 * Feel free to refactor it or add comments on what could be improved.
 *
 * Feel free to use knex's query builder or just plain sql using `knexDb.raw()`.
 *
 */

const app = express();

app.set("port", process.env.PORT || 3000);

app.use(express.json());

type SpendStatsResponse = {
  unique_buyers: number;
  transaction_count: number;
  unique_suppliers: number;
};

app.get("/", (_req, res) => {
  // route documentation here
  res.send(`
    <h1>API Routes</h1>
    <p>GET <a href="/api/stats">/api/stats</a> - This operation exposes basic high level stats of the transaction database.</p>
    <p>POST /api/supplier_stats - This operation returns stats for a specific supplier.</p>
    <p>POST /api/top_suppliers - This operation returns the top suppliers by transaction value.</p>
  `);
});

/**
 * This operation exposes basic high level stats of the transaction database.
 */
app.get("/api/stats", async (_req, res) => {
  const knexDb = await getDBConnection();

  const result = await knexDb("spend_transactions").select(
    knexDb.raw("COUNT(*) AS transaction_count"),
    knexDb.raw("COUNT(DISTINCT supplier_name) as unique_suppliers"),
    knexDb.raw("COUNT(DISTINCT buyer_name) as unique_buyers")
  );

  const response: SpendStatsResponse = {
    unique_buyers: result[0].unique_buyers,
    unique_suppliers: result[0].unique_suppliers,
    transaction_count: result[0].transaction_count,
  };

  res.json(response);
});

type SupplierStatsRequest = {
  supplier_name: string;
};

type SupplierStatsResponse = {
  unique_buyers: number;
  transaction_count: number;
  total_transaction_value: number;
};

/**
 * This operation returns stats for a specific supplier.
 */
app.post("/api/supplier_stats", async (req, res, next) => {
  try {
    const knexDb = await getDBConnection();
    const requestPayload = req.body as SupplierStatsRequest;
    if (!requestPayload.supplier_name) {
      throw new Error("`supplier_name` must be specified.");
    }

    const result = await knexDb("spend_transactions")
      .where({ supplier_name: requestPayload.supplier_name })
      .select(
        knexDb.raw("COUNT(*) AS transaction_count"),
        knexDb.raw("SUM(amount) as total_value"),
        knexDb.raw("COUNT(DISTINCT buyer_name) as unique_buyers")
      );

    console.log(JSON.stringify(result));
    const response: SupplierStatsResponse = {
      unique_buyers: result[0].unique_buyers,
      total_transaction_value: result[0].total_value,
      transaction_count: result[0].transaction_count,
    };

    res.json(response);
  } catch (err) {
    next(err);
  }
});

app.post("/api/top_suppliers", async (_req, _res, next) => {
  try {
    const knexDb = await getDBConnection();

    //TODOL Add body validation
    const body = _req.body;
    const { buyer_name, from_date, to_date, limit = 5 } = body;
    if (!from_date || !to_date) {
      throw new Error("from_date and to_date must be specified");
    }
    let baseQuery = knexDb("spend_transactions")
      .select("supplier_name as name")
      .whereBetween("transaction_timestamp", [
        parseDateFromISO(from_date),
        parseDateFromISO(to_date),
      ])
      .sum("amount as total");

    if (buyer_name) {
      baseQuery = baseQuery.where("buyer_name", buyer_name);
    }

    const results = await baseQuery
      .groupBy("supplier_name")
      .orderBy("total", "desc")
      .limit(limit);
    console.log({ results });
    return _res.json({ top_suppliers: results });
  } catch (err) {
    next(err);
  }
});

/**
 * Simple error handling
 */
app.use((err: unknown, _req: Request, res: Response, next: NextFunction) => {
  console.error(err);
  res.status(500).json({
    error: err instanceof Error ? err.message : "Unknown error occured",
  });
});

app.listen(app.get("port"), () => {
  console.log("  App is running at http://localhost:%d", app.get("port"));
  console.log("  Press CTRL-C to stop\n");
});
