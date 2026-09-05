const express = require("express");
const cors = require("cors");
const { Pool } = require("pg");

const app = express();

app.use(cors());
app.use(express.json());

const pool = new Pool({
  host: "localhost",
  port: 5434,
  database: "airflow",
  user: "airflow",
  password: "airflow",
});

// เช็ค API
app.get("/", (req, res) => {
  res.json({
    message: "Gold Dashboard API is running",
  });
});

// ราคาทองย้อนหลัง
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
      ORDER BY price_date ASC, id ASC
    `);

    res.json(result.rows);
  } catch (error) {
    console.error(error);
    res.status(500).json({
      error: "ไม่สามารถดึงข้อมูลราคาทองได้",
    });
  }
});

// Prediction ล่าสุด
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
    console.error(error);
    res.status(500).json({
      error: "ไม่สามารถดึงข้อมูล prediction ได้",
    });
  }
});

// Prediction ทั้งหมด
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
    console.error(error);
    res.status(500).json({
      error: "ไม่สามารถดึงข้อมูล predictions ได้",
    });
  }
});

app.listen(3000, () => {
  console.log("Dashboard API running at http://localhost:3000");
});