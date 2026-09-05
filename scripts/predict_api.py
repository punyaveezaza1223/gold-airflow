from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import joblib
import pandas as pd
import psycopg2
import os


app = FastAPI(title="Gold Price Prediction API")


# =========================================
# Model
# =========================================

MODEL_PATH = "/opt/airflow/models/gold_models/prediction_model.pkl"

model = None


def load_model():
    global model

    if not os.path.exists(MODEL_PATH):
        raise FileNotFoundError(
            f"ไม่พบโมเดล: {MODEL_PATH}"
        )

    model = joblib.load(MODEL_PATH)

    print("โหลดโมเดลสำเร็จ")


# =========================================
# PostgreSQL
# =========================================

DB_CONFIG = {
    "host": "postgres",
    "port": 5432,
    "database": "airflow",
    "user": "airflow",
    "password": "airflow",
}


def get_historical_prices():
    conn = psycopg2.connect(**DB_CONFIG)

    query = """
        SELECT
            price_date,
            sell_price
        FROM gold_prices
        WHERE gold_type = 'ทองคำแท่ง'
        ORDER BY price_date DESC
        LIMIT 7
    """

    df = pd.read_sql(query, conn)

    conn.close()

    if len(df) < 7:
        raise ValueError(
            "ข้อมูลราคาทองย้อนหลังไม่เพียงพอ ต้องมีอย่างน้อย 7 วัน"
        )

    return df


# =========================================
# Request
# =========================================

class PredictionRequest(BaseModel):
    current_price: float


# =========================================
# Startup
# =========================================

@app.on_event("startup")
def startup_event():
    load_model()


# =========================================
# Health Check
# =========================================

@app.get("/")
def root():
    return {
        "message": "Gold Price Prediction API is running"
    }


# =========================================
# Prediction
# =========================================

@app.post("/predict")
def predict(request: PredictionRequest):

    try:

        if request.current_price <= 0:
            raise HTTPException(
                status_code=400,
                detail="ราคาทองต้องมากกว่า 0"
            )


        # ---------------------------------
        # โหลดราคาย้อนหลัง
        # ---------------------------------

        df = get_historical_prices()

        prices = df["sell_price"].astype(float).tolist()


        # เนื่องจาก query DESC
        # prices[0] = ราคาล่าสุดใน database
        # prices[1] = ราคาก่อนหน้า
        # ...

        lag_1 = prices[0]
        lag_2 = prices[1]
        lag_3 = prices[2]
        lag_7 = prices[6]


        # ---------------------------------
        # Feature Engineering
        # ให้ตรงกับ ML Pipeline
        # ---------------------------------

        current_price = float(
            request.current_price
        )

        rolling_mean_3 = (
            current_price
            + lag_1
            + lag_2
        ) / 3


        rolling_mean_7 = (
            current_price
            + sum(prices[:6])
        ) / 7


        price_change_1 = (
            current_price - lag_1
        )


        price_change_3 = (
            current_price - lag_3
        )


        # ---------------------------------
        # สร้าง Feature DataFrame
        # ---------------------------------

        features = pd.DataFrame([{
            "current_price": current_price,
            "lag_1": lag_1,
            "lag_2": lag_2,
            "lag_3": lag_3,
            "lag_7": lag_7,
            "rolling_mean_3": rolling_mean_3,
            "rolling_mean_7": rolling_mean_7,
            "price_change_1": price_change_1,
            "price_change_3": price_change_3,
        }])


        # ---------------------------------
        # Predict
        # ---------------------------------

        predicted_change = float(
            model.predict(features)[0]
        )


        predicted_price = (
            current_price
            + predicted_change
        )


        # ---------------------------------
        # Response
        # ---------------------------------

        return {
            "current_price": current_price,
            "predicted_change": round(
                predicted_change,
                2
            ),
            "predicted_price": round(
                predicted_price,
                2
            ),
            "direction": (
                "ขึ้น"
                if predicted_change > 0
                else "ลง"
                if predicted_change < 0
                else "คงที่"
            ),
            "features": {
                "lag_1": lag_1,
                "lag_2": lag_2,
                "lag_3": lag_3,
                "lag_7": lag_7,
                "rolling_mean_3": round(
                    rolling_mean_3,
                    2
                ),
                "rolling_mean_7": round(
                    rolling_mean_7,
                    2
                ),
                "price_change_1": round(
                    price_change_1,
                    2
                ),
                "price_change_3": round(
                    price_change_3,
                    2
                ),
            }
        }


    except HTTPException:
        raise


    except Exception as e:

        print(
            "Prediction error:",
            str(e)
        )

        raise HTTPException(
            status_code=500,
            detail=str(e)
        )