import { useEffect, useState } from "react";
import axios from "axios";

import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from "recharts";

import "./App.css";

const API_BASE = "http://localhost:3000/api";

// Chart colors mirrored from the CSS palette in App.css.
const COLOR_MAROON = "#6e1423";
const COLOR_GOLD = "#c8962c";
const COLOR_INK_DIM = "#8a7d6b";
const COLOR_HAIRLINE = "#ece2cf";
const COLOR_PAPER = "#fffdf8";

function App() {
  // =========================
  // Data จาก Backend
  // =========================
  const [prices, setPrices] = useState([]);
  const [prediction, setPrediction] = useState(null);
  const [predictionHistory, setPredictionHistory] = useState([]);

  const [loading, setLoading] = useState(true);
  const [status, setStatus] = useState("");

  // =========================
  // ML Prediction Input
  // =========================
  const [inputPrice, setInputPrice] = useState("");
  const [interactivePrediction, setInteractivePrediction] = useState(null);
  const [predictLoading, setPredictLoading] = useState(false);
  const [predictError, setPredictError] = useState("");

  // =========================
  // โหลดข้อมูล Dashboard
  // =========================
  useEffect(() => {
    fetchDashboardData();
  }, []);

  const fetchDashboardData = async () => {
    try {
      setLoading(true);
      setStatus("");

      const [pricesRes, predictionRes, backtestRes] = await Promise.all([
        axios.get(`${API_BASE}/gold-prices`),
        axios.get(`${API_BASE}/prediction`),
        axios.get(`${API_BASE}/backtest-results`),
      ]);

      setPrices(pricesRes.data || []);
      setPrediction(predictionRes.data || null);
      setPredictionHistory(backtestRes.data || []);

      if (pricesRes.data?.length > 0) {
        const latest = pricesRes.data[pricesRes.data.length - 1];
        if (latest?.sell_price) {
          setInputPrice(Number(latest.sell_price));
        }
      }
    } catch (error) {
      console.error("Dashboard Error:", error);
      setStatus(
        error.response?.data?.error || "ไม่สามารถโหลดข้อมูลแดชบอร์ดได้ ลองรีเฟรชหน้าอีกครั้ง"
      );
    } finally {
      setLoading(false);
    }
  };

  // =========================
  // เรียก ML Prediction
  // =========================
  const handlePredict = async () => {
    if (!inputPrice || Number(inputPrice) <= 0) {
      setPredictError("กรุณากรอกราคาทองเป็นตัวเลขที่มากกว่า 0");
      setInteractivePrediction(null);
      return;
    }

    try {
      setPredictLoading(true);
      setPredictError("");
      setInteractivePrediction(null);

      const response = await axios.post(`${API_BASE}/predict`, {
        current_price: Number(inputPrice),
      });

      setInteractivePrediction(response.data);
    } catch (error) {
      console.error("Prediction Error:", error);
      setPredictError(
        error.response?.data?.detail ||
          error.response?.data?.error ||
          "ไม่สามารถทำนายราคาทองได้ ลองใหม่อีกครั้ง"
      );
    } finally {
      setPredictLoading(false);
    }
  };

  const handleKeyDown = (event) => {
    if (event.key === "Enter") {
      handlePredict();
    }
  };

  // =========================
  // Format ตัวเลข
  // =========================
  const formatNumber = (value, digits = 2) => {
    if (value === null || value === undefined || value === "") {
      return "-";
    }
    return Number(value).toLocaleString("th-TH", {
      minimumFractionDigits: digits,
      maximumFractionDigits: digits,
    });
  };

  // แปลงวันที่เป็น ค.ศ. เช่น 03/09/2026
  const formatDate = (dateString) => {
    if (!dateString) return "-";

    const date = new Date(dateString);

    return date.toLocaleDateString("en-GB", {
      day: "2-digit",
      month: "2-digit",
      year: "numeric",
      timeZone: "Asia/Bangkok",
    });
  };

  // ทิศทางของการเปลี่ยนแปลง: คืนทั้งป้ายข้อความ ไอคอน และคลาสสี
  // (ไม่พึ่งพาสีเพียงอย่างเดียว เพื่อให้อ่านง่ายสำหรับทุกคน)
  const getTrend = (delta) => {
    if (delta === null || delta === undefined || Number.isNaN(delta)) {
      return { label: "ไม่มีข้อมูล", icon: "–", className: "flat" };
    }
    if (delta > 0) return { label: "ราคาขึ้น", icon: "▲", className: "up" };
    if (delta < 0) return { label: "ราคาลง", icon: "▼", className: "down" };
    return { label: "ราคาคงที่", icon: "■", className: "flat" };
  };

  // =========================
  // ข้อมูลล่าสุด
  // =========================
  const latestPrice = prices.length > 0 ? prices[prices.length - 1] : null;

  const predictionDelta =
    prediction && latestPrice
      ? Number(prediction.predicted_price) - Number(latestPrice.sell_price)
      : null;
  const predictionTrend = getTrend(predictionDelta);

  // =========================
  // เตรียมข้อมูลสำหรับกราฟราคา
  // =========================
  const priceChartData = prices.map((item) => ({
    date: formatDate(item.price_date),
    buy: Number(item.buy_price),
    sell: Number(item.sell_price),
  }));

  // =========================
  // เตรียมข้อมูล Actual vs Prediction
  // =========================
  const actualPredictionChartData = predictionHistory
    .filter((item) => item.actual_price !== null && item.predicted_price !== null)
    .map((item) => ({
      date: formatDate(item.prediction_date),
      actual: Number(item.actual_price),
      predicted: Number(item.predicted_price),
    }));

  // =========================
  // คำนวณ Model Performance จาก prediction history
  // =========================
  const validHistory = predictionHistory.filter(
    (item) => item.actual_price !== null && item.predicted_price !== null
  );

  let historyMAE = null;
  let historyRMSE = null;
  let historyR2 = null;

  if (validHistory.length > 0) {
    const actual = validHistory.map((item) => Number(item.actual_price));
    const predicted = validHistory.map((item) => Number(item.predicted_price));

    const absoluteErrors = actual.map((value, index) => Math.abs(value - predicted[index]));
    historyMAE = absoluteErrors.reduce((sum, value) => sum + value, 0) / absoluteErrors.length;

    const squaredErrors = actual.map((value, index) => Math.pow(value - predicted[index], 2));
    historyRMSE = Math.sqrt(
      squaredErrors.reduce((sum, value) => sum + value, 0) / squaredErrors.length
    );

    const actualMean = actual.reduce((sum, value) => sum + value, 0) / actual.length;
    const ssTotal = actual.reduce((sum, value) => sum + Math.pow(value - actualMean, 2), 0);
    const ssResidual = actual.reduce(
      (sum, value, index) => sum + Math.pow(value - predicted[index], 2),
      0
    );

    if (ssTotal !== 0) {
      historyR2 = 1 - ssResidual / ssTotal;
    }
  }

  // =========================
  // Loading
  // =========================
  if (loading) {
    return (
      <div className="dashboard">
        <div className="state-message" role="status">
          <div className="spinner" aria-hidden="true" />
          <p>กำลังโหลดข้อมูลราคาทอง...</p>
        </div>
      </div>
    );
  }

  return (
    <div className="dashboard">
      {/* Header */}
      <header>
        <div className="header-inner">
          <p className="eyebrow">แดชบอร์ดราคาทองคำ</p>
          <h1>Gold Price Intelligence</h1>
          <p className="subtitle">
            ติดตามราคาทองคำและดูแนวโน้มวันถัดไปจากโมเดล Machine Learning
            {latestPrice?.price_date ? ` · ข้อมูล ณ วันที่ ${formatDate(latestPrice.price_date)}` : ""}
          </p>
        </div>
      </header>

      {status && (
        <div className="table-section">
          <div className="state-message error" role="alert">
            {status}
          </div>
        </div>
      )}

      {/* Latest price cards */}
      <section className="cards" aria-label="สรุปราคาทองล่าสุด">
        <div className="card">
          <h3>ราคาทองคำแท่ง (ขายออก)</h3>
          <p className="price">
            {latestPrice ? formatNumber(latestPrice.sell_price) : "-"}
            <span className="unit">บาท</span>
          </p>
        </div>

        <div className="card">
          <h3>ราคารับซื้อคืน</h3>
          <p className="price">
            {latestPrice ? formatNumber(latestPrice.buy_price) : "-"}
            <span className="unit">บาท</span>
          </p>
        </div>

        <div className="card prediction">
          <h3>คาดการณ์วันถัดไป{prediction?.prediction_date ? ` (${formatDate(prediction.prediction_date)})` : ""}</h3>
          <p className="price">
            {prediction ? formatNumber(prediction.predicted_price) : "-"}
            <span className="unit">บาท</span>
          </p>
          {prediction && (
            <p className={`delta ${predictionTrend.className}`}>
              {predictionTrend.icon} {predictionTrend.label}
              {predictionDelta !== null ? ` ${formatNumber(Math.abs(predictionDelta))} บาท` : ""}
            </p>
          )}
        </div>
      </section>

      {/* Interactive prediction */}
      <section className="prediction-input-section" aria-labelledby="predict-heading">
        <div className="section-header">
          <h2 id="predict-heading">ลองทำนายราคาทองด้วยตัวเอง</h2>
          <p>กรอกราคาทองปัจจุบัน แล้วให้โมเดล Random Forest คาดการณ์ราคาของวันถัดไป</p>
        </div>

        <div className="prediction-form">
          <div className="input-with-suffix">
            <label htmlFor="gold-price-input" className="visually-hidden">
              ราคาทองปัจจุบัน (บาท)
            </label>
            <input
              id="gold-price-input"
              type="number"
              value={inputPrice}
              onChange={(e) => setInputPrice(e.target.value)}
              onKeyDown={handleKeyDown}
              placeholder="เช่น 69150"
              min="1"
              aria-describedby="gold-price-hint"
            />
            <span aria-hidden="true">บาท</span>
          </div>

          <button onClick={handlePredict} disabled={predictLoading}>
            {predictLoading ? "กำลังทำนาย..." : "ทำนายราคา"}
          </button>
        </div>
        <p id="gold-price-hint" className="field-hint">
          กด Enter หรือคลิก “ทำนายราคา” เพื่อดูผล
        </p>

        {predictError && (
          <div className="state-message error inline" role="alert">
            {predictError}
          </div>
        )}

        {interactivePrediction && (
          <div className="prediction-result" aria-live="polite">
            <div className="result-item">
              <span>ราคาปัจจุบัน</span>
              <strong>{formatNumber(interactivePrediction.current_price)} บาท</strong>
            </div>

            <div className="result-item">
              <span>ราคาที่คาดการณ์</span>
              <strong>{formatNumber(interactivePrediction.predicted_price)} บาท</strong>
            </div>

            <div className="result-item">
              <span>การเปลี่ยนแปลง</span>
              <strong>
                {interactivePrediction.predicted_change > 0 ? "+" : ""}
                {formatNumber(interactivePrediction.predicted_change)} บาท
              </strong>
            </div>

            <div className="result-item">
              <span>แนวโน้ม</span>
              <strong className={`change ${getTrend(interactivePrediction.predicted_change).className}`}>
                {getTrend(interactivePrediction.predicted_change).icon}{" "}
                {interactivePrediction.direction || getTrend(interactivePrediction.predicted_change).label}
              </strong>
            </div>
          </div>
        )}
      </section>

      {/* Historical price chart */}
      <section className="chart-section" aria-labelledby="history-chart-heading">
        <div className="section-head">
          <div>
            <h2 id="history-chart-heading">ราคาทองย้อนหลัง</h2>
            <p>เปรียบเทียบราคาซื้อและราคาขายในแต่ละวัน</p>
          </div>
        </div>

        <div className="chart-container">
          {priceChartData.length > 0 ? (
            <ResponsiveContainer width="100%" height={360}>
              <LineChart data={priceChartData} margin={{ top: 10, right: 20, left: 0, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke={COLOR_HAIRLINE} />
                <XAxis dataKey="date" tick={{ fill: COLOR_INK_DIM, fontSize: 12 }} />
                <YAxis domain={["auto", "auto"]} tick={{ fill: COLOR_INK_DIM, fontSize: 12 }} width={70} />
                <Tooltip
                  formatter={(value) => `${formatNumber(value)} บาท`}
                  contentStyle={{
                    background: COLOR_PAPER,
                    border: `1px solid ${COLOR_HAIRLINE}`,
                    borderRadius: 4,
                  }}
                />
                <Legend />
                <Line type="monotone" dataKey="buy" name="ราคาซื้อ" stroke={COLOR_GOLD} strokeWidth={2} dot={false} />
                <Line
                  type="monotone"
                  dataKey="sell"
                  name="ราคาขาย"
                  stroke={COLOR_MAROON}
                  strokeWidth={2}
                  dot={false}
                />
              </LineChart>
            </ResponsiveContainer>
          ) : (
            <p className="empty-state">ยังไม่มีข้อมูลราคาทอง</p>
          )}
        </div>
      </section>

      {/* Actual vs prediction chart */}
      <section className="chart-section" aria-labelledby="accuracy-chart-heading">
        <div className="section-head">
          <div>
            <h2 id="accuracy-chart-heading">ราคาจริงเทียบกับราคาที่ทำนาย</h2>
            <p>ดูว่าโมเดลทำนายใกล้เคียงกับราคาจริงมากแค่ไหนในแต่ละวัน</p>
          </div>
        </div>

        <div className="chart-container">
          {actualPredictionChartData.length > 0 ? (
            <ResponsiveContainer width="100%" height={360}>
              <LineChart
                data={actualPredictionChartData}
                margin={{ top: 10, right: 20, left: 0, bottom: 0 }}
              >
                <CartesianGrid strokeDasharray="3 3" stroke={COLOR_HAIRLINE} />
                <XAxis dataKey="date" tick={{ fill: COLOR_INK_DIM, fontSize: 12 }} />
                <YAxis domain={["auto", "auto"]} tick={{ fill: COLOR_INK_DIM, fontSize: 12 }} width={70} />
                <Tooltip
                  formatter={(value) => `${formatNumber(value)} บาท`}
                  contentStyle={{
                    background: COLOR_PAPER,
                    border: `1px solid ${COLOR_HAIRLINE}`,
                    borderRadius: 4,
                  }}
                />
                <Legend />
                <Line
                  type="monotone"
                  dataKey="actual"
                  name="ราคาจริง"
                  stroke={COLOR_MAROON}
                  strokeWidth={3}
                  dot={{ r: 3 }}
                />
                <Line
                  type="monotone"
                  dataKey="predicted"
                  name="ราคาที่ทำนาย"
                  stroke={COLOR_GOLD}
                  strokeWidth={2}
                  strokeDasharray="5 5"
                  dot={{ r: 3 }}
                />
              </LineChart>
            </ResponsiveContainer>
          ) : (
            <p className="empty-state">ยังไม่มีข้อมูลราคาจริงเทียบกับราคาที่ทำนาย</p>
          )}
        </div>
      </section>

      {/* Model performance */}
      <section className="performance-section" aria-labelledby="performance-heading">
        <div className="section-head">
          <div>
            <h2 id="performance-heading">ความแม่นยำของโมเดล</h2>
            <p>คำนวณจากผล Walk-Forward Backtest ที่มีราคาจริงเปรียบเทียบแล้ว</p>
          </div>
        </div>

        <dl className="performance-strip">
          <div className="performance-item">
            <dt>MAE</dt>
            <dd>{historyMAE !== null ? `${formatNumber(historyMAE)} บาท` : "-"}</dd>
            <small>ค่าเฉลี่ยความคลาดเคลื่อนสัมบูรณ์</small>
          </div>

          <div className="performance-item">
            <dt>RMSE</dt>
            <dd>{historyRMSE !== null ? `${formatNumber(historyRMSE)} บาท` : "-"}</dd>
            <small>รากที่สองของค่าเฉลี่ยความคลาดเคลื่อนกำลังสอง</small>
          </div>

          <div className="performance-item">
            <dt>R²</dt>
            <dd>{historyR2 !== null ? historyR2.toFixed(4) : "-"}</dd>
            <small>สัดส่วนความแปรปรวนที่โมเดลอธิบายได้</small>
          </div>

          <div className="performance-item">
            <dt>จำนวนที่ตรวจสอบแล้ว</dt>
            <dd>{validHistory.length}</dd>
            <small>วันที่ที่นำมาใช้ตรวจสอบ Actual เทียบกับ Predicted</small>
          </div>
        </dl>

        <p className="field-hint" style={{ marginTop: "12px" }}>
          หมายเหตุ: Accuracy % ในตารางเป็นค่าความใกล้เคียงของราคาที่ทำนายกับราคาจริง
          ส่วน MAE, RMSE และ R² เป็นตัวชี้วัดหลักสำหรับประเมินโมเดล
        </p>
      </section>

      {/* Prediction history table */}
      <section className="table-section" aria-labelledby="history-table-heading">
        <div className="section-head">
          <div>
            <h2 id="history-table-heading">ประวัติการทำนาย</h2>
            <p>ผลการทดสอบโมเดลแบบ Walk-Forward Backtest</p>
          </div>
        </div>

        <div className="table-scroll">
          <table>
            <thead>
              <tr>
                <th scope="col">วันที่</th>
                <th scope="col">ราคาจริง</th>
                <th scope="col">ราคาที่ทำนาย</th>
                <th scope="col">ผลต่าง</th>
                <th scope="col">ความใกล้เคียง</th>
                <th scope="col">โมเดล</th>
              </tr>
            </thead>
            <tbody>
              {predictionHistory.length > 0 ? (
                predictionHistory.map((item, index) => (
                  <tr key={`${item.prediction_date}-${index}`}>
                    <td>{formatDate(item.prediction_date)}</td>
                    <td>{item.actual_price !== null ? `${formatNumber(item.actual_price)} บาท` : "-"}</td>
                    <td>
                      {item.predicted_price !== null ? `${formatNumber(item.predicted_price)} บาท` : "-"}
                    </td>
                    <td>{item.error !== null ? `${formatNumber(item.error)} บาท` : "-"}</td>
                    <td>
                      {item.accuracy_percent !== null && item.accuracy_percent !== undefined
                        ? `${formatNumber(item.accuracy_percent, 2)}%`
                        : "-"}
                    </td>
                    <td>{item.model_name || "-"}</td>
                  </tr>
                ))
              ) : (
                <tr>
                  <td colSpan="6" className="empty-state">
                    ยังไม่มีประวัติการทำนาย
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </section>

      <footer className="dashboard-footer">
        <p>Gold Price Intelligence — ระบบติดตามและพยากรณ์ราคาทองคำ</p>
        <p className="stack-note">ประมวลผลข้อมูลด้วย Airflow · PostgreSQL · Random Forest</p>
      </footer>
    </div>
  );
}

export default App;