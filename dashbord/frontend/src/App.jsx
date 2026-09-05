import { useEffect, useState } from "react";
import axios from "axios";
import "./App.css";

const API_BASE = "http://localhost:3000/api";

function formatThaiDate(dateString) {
  if (!dateString) return "-";
  const date = new Date(dateString);
  if (Number.isNaN(date.getTime())) return dateString;
  return date.toLocaleDateString("th-TH", {
    day: "numeric",
    month: "short",
    year: "numeric",
  });
}

function formatBaht(value) {
  if (value === null || value === undefined || value === "") return "-";
  return Number(value).toLocaleString("th-TH");
}

function App() {
  const [prices, setPrices] = useState([]);
  const [prediction, setPrediction] = useState(null);
  const [status, setStatus] = useState("loading"); // loading | ready | error

  const [buyPrice, setBuyPrice] = useState("");
  const [quantity, setQuantity] = useState(1);

  useEffect(() => {
    let cancelled = false;

    async function loadData() {
      setStatus("loading");
      try {
        const [pricesRes, predictionRes] = await Promise.all([
          axios.get(`${API_BASE}/gold-prices`),
          axios.get(`${API_BASE}/prediction`),
        ]);
        if (cancelled) return;
        setPrices(pricesRes.data ?? []);
        setPrediction(predictionRes.data ?? null);
        setStatus("ready");
      } catch (err) {
        console.error(err);
        if (!cancelled) setStatus("error");
      }
    }

    loadData();
    return () => {
      cancelled = true;
    };
  }, []);

  const latest = prices[prices.length - 1];
  const currentSellPrice = latest
  ? Number(latest.sell_price)
  : 0;

  const profitLoss =
  buyPrice !== ""
    ? (currentSellPrice - Number(buyPrice)) * Number(quantity)
    : null;
  const diff =
    latest && prediction
      ? Number(prediction.predicted_price) - Number(latest.sell_price)
      : null;

  return (
    <div className="dashboard">
      <header>
        <div className="header-inner">
          <p className="eyebrow">ร้านทองออนไลน์</p>
          <h1>ราคาทองคำวันนี้</h1>
          <p className="subtitle">ติดตามราคาซื้อ-ขาย พร้อมคาดการณ์ราคาวันถัดไป</p>
        </div>
      </header>

      {status === "loading" && (
        <div className="state-message">
          <div className="spinner" aria-hidden="true" />
          กำลังโหลดราคาทองล่าสุด…
        </div>
      )}

      {status === "error" && (
        <div className="state-message error">
          โหลดข้อมูลไม่สำเร็จ ตรวจสอบว่าเซิร์ฟเวอร์ที่ localhost:3000 เปิดอยู่หรือไม่
        </div>
      )}

      {status === "ready" && (
        <>
          <section className="cards">
            <div className="card">
              <h3>ราคาซื้อล่าสุด</h3>
              <div className="price">
                <span className="unit">฿</span>
                {formatBaht(latest?.buy_price)}
              </div>
            </div>

            <div className="card">
              <h3>ราคาขายล่าสุด</h3>
              <div className="price">
                <span className="unit">฿</span>
                {formatBaht(latest?.sell_price)}
              </div>
            </div>

            <div className="card prediction">
              <h3>คาดการณ์วันถัดไป</h3>
              <div className="price">
                <span className="unit">฿</span>
                {formatBaht(prediction?.predicted_price)}
              </div>
              {diff !== null && (
                <p className={`delta ${diff >= 0 ? "up" : "down"}`}>
                  {diff >= 0 ? "▲" : "▼"} {formatBaht(Math.abs(diff))} บาท
                  จากราคาขายล่าสุด
                </p>
              )}
            </div>
          </section>

          <section className="calculator">
  <div className="calculator-content">
    <div>
      <p className="eyebrow">คำนวณการลงทุน</p>
      <h2>คำนวณกำไร / ขาดทุน</h2>
      <p className="calculator-desc">
        กรอกราคาที่ซื้อมา ระบบจะคำนวณกำไรหรือขาดทุน
        จากราคาขายทองคำปัจจุบัน
      </p>
    </div>

    <div className="form-row">
      <div className="input-group">
        <label>ราคาที่ซื้อมา (บาท)</label>
        <input
          type="number"
          placeholder="เช่น 65000"
          value={buyPrice}
          onChange={(e) => setBuyPrice(e.target.value)}
        />
      </div>

      <div className="input-group">
        <label>จำนวน</label>
        <input
          type="number"
          min="1"
          value={quantity}
          onChange={(e) => setQuantity(e.target.value)}
        />
      </div>
    </div>

    {profitLoss !== null && (
      <div className={`profit-result ${profitLoss >= 0 ? "profit" : "loss"}`}>
        <span>
          {profitLoss >= 0 ? "กำไรโดยประมาณ" : "ขาดทุนโดยประมาณ"}
        </span>

        <strong>
          {profitLoss >= 0 ? "+" : "-"}
          ฿{formatBaht(Math.abs(profitLoss))}
        </strong>
      </div>
    )}
  </div>
</section>

          <section className="table-section">
            <div className="section-head">
              <h2>ราคาทองย้อนหลัง</h2>
              <p>{prices.length} รายการ</p>
            </div>

            <div className="table-scroll">
              <table>
                <thead>
                  <tr>
                    <th>วันที่</th>
                    <th>ราคาซื้อ</th>
                    <th>ราคาขาย</th>
                    <th>เปลี่ยนแปลง</th>
                  </tr>
                </thead>
                <tbody>
                  {prices
                    .slice()
                    .reverse()
                    .map((item) => {
                      const change = item.change_price;
                      const changeNum = change === null || change === undefined
                        ? null
                        : Number(change);
                      return (
                        <tr key={`${item.price_date}-${item.gold_type}`}>
                          <td>{formatThaiDate(item.price_date)}</td>
                          <td>฿{formatBaht(item.buy_price)}</td>
                          <td>฿{formatBaht(item.sell_price)}</td>
                          <td>
                            {changeNum === null ? (
                              "-"
                            ) : (
                              <span
                                className={`change ${
                                  changeNum > 0
                                    ? "up"
                                    : changeNum < 0
                                    ? "down"
                                    : "flat"
                                }`}
                              >
                                {changeNum > 0 ? "▲" : changeNum < 0 ? "▼" : "–"}{" "}
                                {formatBaht(Math.abs(changeNum))}
                              </span>
                            )}
                          </td>
                        </tr>
                      );
                    })}
                </tbody>
              </table>
            </div>
          </section>
        </>
      )}
    </div>
  );
}

export default App;