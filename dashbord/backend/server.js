const express = require("express");
const cors = require("cors");
const axios = require("axios");
const { Pool } = require("pg");

const app = express();

app.use(cors());
app.use(express.json());

// ========================================
// PostgreSQL
// ========================================

const pool = new Pool({
  host: "localhost",
  port: 5434,
  database: "airflow",
  user: "airflow",
  password: "airflow",
});

// ========================================
// เช็ค API
// ========================================

app.get("/", (req, res) => {
  res.json({
    message: "Gold Dashboard API is running",
  });
});

// ========================================
// ราคาทองย้อนหลัง
// ========================================

app.get("/api/gold-prices", async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT
        price_date,
        gold_type,
        buy_price,
        sell_price,
        change_price,
        change_percent
      FROM gold_prices
      WHERE gold_type = 'ทองคำแท่ง'
      ORDER BY price_date ASC
    `);

    res.json(result.rows);
  } catch (error) {
    console.error("Gold Prices Error:", error);

    res.status(500).json({
      error: "ไม่สามารถดึงข้อมูลราคาทองได้",
    });
  }
});

// ========================================
// Prediction ล่าสุด
// ========================================

app.get("/api/prediction", async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT
        prediction_date,
        gold_type,
        predicted_price,
        model_name
      FROM gold_predictions
      WHERE gold_type = 'ทองคำแท่ง'
      ORDER BY prediction_date DESC, id DESC
      LIMIT 1
    `);

    res.json(result.rows[0] || null);
  } catch (error) {
    console.error("Prediction Error:", error);

    res.status(500).json({
      error: "ไม่สามารถดึงข้อมูล prediction ได้",
    });
  }
});

// ========================================
// Prediction ทั้งหมด
// ========================================

app.get("/api/predictions", async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT
        prediction_date,
        gold_type,
        predicted_price,
        model_name
      FROM gold_predictions
      WHERE gold_type = 'ทองคำแท่ง'
      ORDER BY prediction_date ASC
    `);

    res.json(result.rows);
  } catch (error) {
    console.error("Predictions Error:", error);

    res.status(500).json({
      error: "ไม่สามารถดึงข้อมูล predictions ได้",
    });
  }
});

// ========================================
// BACKTEST
// Actual vs Predicted
// ========================================

app.get("/api/backtest-results", async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT
        prediction_date,
        gold_type,
        actual_price,
        predicted_price,
        error,
        accuracy_percent,
        model_name,
        created_at
      FROM gold_prediction_results
      WHERE gold_type = 'ทองคำแท่ง'
      ORDER BY prediction_date ASC
    `);

    res.json(result.rows);
  } catch (error) {
    console.error("Backtest Error:", error);

    res.status(500).json({
      error: "ไม่สามารถดึงข้อมูล Backtest ได้",
    });
  }
});

// ========================================
// BACKTEST ล่าสุดก่อน
// ใช้สำหรับตาราง History
// ========================================

app.get("/api/backtest-latest", async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT
        prediction_date,
        gold_type,
        actual_price,
        predicted_price,
        error,
        accuracy_percent,
        model_name
      FROM gold_prediction_results
      WHERE gold_type = 'ทองคำแท่ง'
      ORDER BY prediction_date DESC
    `);

    res.json(result.rows);
  } catch (error) {
    console.error("Backtest Latest Error:", error);

    res.status(500).json({
      error: "ไม่สามารถดึงประวัติ Backtest ได้",
    });
  }
});

// ========================================
// BACKTEST PERFORMANCE
// MAE / RMSE / R²
// ========================================

app.get("/api/backtest-performance", async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT
        COUNT(*) AS count,

        AVG(ABS(actual_price - predicted_price))
          AS mae,

        SQRT(
          AVG(
            POWER(actual_price - predicted_price, 2)
          )
        ) AS rmse,

        (
          1 -
          (
            SUM(
              POWER(actual_price - predicted_price, 2)
            )
            /
            NULLIF(
              SUM(
                POWER(
                  actual_price -
                  AVG(actual_price) OVER (),
                  2
                )
              ),
              0
            )
          )
        ) AS r2

      FROM gold_prediction_results
      WHERE gold_type = 'ทองคำแท่ง'
    `);

    res.json(result.rows[0]);
  } catch (error) {
    console.error("Performance Error:", error);

    // ถ้า PostgreSQL ไม่ยอมให้ใช้ window function
    // ใน aggregate query ให้คำนวณด้วย query แบบแยก
    try {
      const data = await pool.query(`
        SELECT
          actual_price,
          predicted_price
        FROM gold_prediction_results
        WHERE gold_type = 'ทองคำแท่ง'
      `);

      const rows = data.rows;

      if (rows.length === 0) {
        return res.json({
          count: 0,
          mae: 0,
          rmse: 0,
          r2: 0,
        });
      }

      const actual = rows.map((r) => Number(r.actual_price));
      const predicted = rows.map((r) => Number(r.predicted_price));

      const n = actual.length;

      // MAE
      const mae =
        actual.reduce(
          (sum, value, i) =>
            sum + Math.abs(value - predicted[i]),
          0
        ) / n;

      // RMSE
      const rmse = Math.sqrt(
        actual.reduce(
          (sum, value, i) =>
            sum + Math.pow(value - predicted[i], 2),
          0
        ) / n
      );

      // R²
      const mean =
        actual.reduce((sum, value) => sum + value, 0) / n;

      const ssRes = actual.reduce(
        (sum, value, i) =>
          sum + Math.pow(value - predicted[i], 2),
        0
      );

      const ssTot = actual.reduce(
        (sum, value) =>
          sum + Math.pow(value - mean, 2),
        0
      );

      const r2 =
        ssTot === 0
          ? 0
          : 1 - ssRes / ssTot;

      res.json({
        count: n,
        mae,
        rmse,
        r2,
      });
    } catch (fallbackError) {
      console.error(
        "Performance fallback error:",
        fallbackError
      );

      res.status(500).json({
        error: "ไม่สามารถคำนวณ Performance ได้",
      });
    }
  }
});

// ========================================
// Old Prediction History
// ========================================

app.get("/api/prediction-history", async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT
        p.prediction_date,
        p.predicted_price,
        p.gold_type,
        p.model_name,
        g.sell_price AS actual_price,

        CASE
          WHEN g.sell_price IS NOT NULL
          THEN ABS(p.predicted_price - g.sell_price)
          ELSE NULL
        END AS error

      FROM gold_predictions p

      LEFT JOIN gold_prices g
        ON p.prediction_date = g.price_date
        AND p.gold_type = g.gold_type

      WHERE p.gold_type = 'ทองคำแท่ง'

      ORDER BY p.prediction_date ASC
    `);

    res.json(result.rows);
  } catch (error) {
    console.error("Prediction History Error:", error);

    res.status(500).json({
      error: "ไม่สามารถดึงประวัติการพยากรณ์ได้",
    });
  }
});

// ========================================
// Interactive ML Prediction
// React → Node → Python ML API
// ========================================

app.post("/api/predict", async (req, res) => {
  try {
    const { current_price } = req.body;

    // ตรวจสอบข้อมูล
    if (
      current_price === undefined ||
      current_price === null ||
      Number(current_price) <= 0
    ) {
      return res.status(400).json({
        error: "กรุณากรอกราคาทองที่ถูกต้อง",
      });
    }

    console.log(
      `กำลังส่งราคาทอง ${current_price} ไปยัง ML API...`
    );

    // ส่งข้อมูลไป Python Predictor
    const response = await axios.post(
      "http://localhost:8000/predict",
      {
        current_price: Number(current_price),
      }
    );

    console.log(
      "Prediction result:",
      response.data
    );

    // ส่งผลกลับ React
    res.json(response.data);

  } catch (error) {
    console.error(
      "Prediction API Error:",
      error.response?.data || error.message
    );

    res.status(500).json({
      error: "ไม่สามารถทำนายราคาทองได้",
      detail:
        error.response?.data?.detail ||
        error.message,
    });
  }
});

// ========================================
// Start Server
// ========================================

app.listen(3000, () => {
  console.log(
    "Dashboard API running at http://localhost:3000"
  );
});