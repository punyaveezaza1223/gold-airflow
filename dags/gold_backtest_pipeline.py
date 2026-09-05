import os
import logging
import psycopg2
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score


# =========================
# CONFIG
# =========================

MODEL_NAME = "RandomForestRegressor_Backtest"
N_ESTIMATORS = 300

FEATURES = [
    "current_price",
    "lag_1",
    "lag_2",
    "lag_3",
    "lag_7",
    "rolling_mean_3",
    "rolling_mean_7",
    "price_change_1",
    "price_change_3",
]


# =========================
# DATABASE
# =========================

def get_db_connection():
    return psycopg2.connect(
        host="postgres",
        port=5432,
        database="airflow",
        user="airflow",
        password="airflow",
    )


# =========================
# LOAD GOLD DATA
# =========================

def load_gold_data():
    conn = get_db_connection()

    query = """
        SELECT
            price_date,
            sell_price
        FROM gold_prices
        WHERE gold_type = 'ทองคำแท่ง'
        ORDER BY price_date ASC, id ASC;
    """

    df = pd.read_sql(query, conn)

    conn.close()

    if df.empty:
        raise ValueError("ไม่พบข้อมูลทองคำ")

    # ถ้ามีหลาย record ในวันเดียว
    df = (
        df.groupby("price_date", as_index=False)
        .last()
        .sort_values("price_date")
        .reset_index(drop=True)
    )

    df["sell_price"] = pd.to_numeric(
        df["sell_price"],
        errors="coerce"
    )

    df = df.dropna(subset=["sell_price"]).reset_index(drop=True)

    logging.info(
        f"พบข้อมูลทั้งหมด {len(df)} วัน"
    )

    return df


# =========================
# CREATE FEATURES
# =========================

def create_features(df):
    data = df.copy()

    data["current_price"] = data["sell_price"]

    data["lag_1"] = data["sell_price"].shift(1)
    data["lag_2"] = data["sell_price"].shift(2)
    data["lag_3"] = data["sell_price"].shift(3)
    data["lag_7"] = data["sell_price"].shift(7)

    data["rolling_mean_3"] = (
        data["sell_price"]
        .rolling(3)
        .mean()
    )

    data["rolling_mean_7"] = (
        data["sell_price"]
        .rolling(7)
        .mean()
    )

    data["price_change_1"] = (
        data["sell_price"]
        - data["sell_price"].shift(1)
    )

    data["price_change_3"] = (
        data["sell_price"]
        - data["sell_price"].shift(3)
    )

    # Target = การเปลี่ยนแปลงของราคาวันถัดไป
    data["next_price"] = data["sell_price"].shift(-1)

    data["target"] = (
        data["next_price"]
        - data["sell_price"]
    )

    return data


# =========================
# SAVE RESULT
# =========================

def save_result(
    prediction_date,
    actual_price,
    predicted_price,
    error,
    accuracy_percent
):

    conn = get_db_connection()
    cur = conn.cursor()

    query = """
        INSERT INTO gold_prediction_results
        (
            prediction_date,
            gold_type,
            actual_price,
            predicted_price,
            error,
            accuracy_percent,
            model_name
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)

        ON CONFLICT (
            prediction_date,
            gold_type,
            model_name
        )
        DO UPDATE SET
            actual_price = EXCLUDED.actual_price,
            predicted_price = EXCLUDED.predicted_price,
            error = EXCLUDED.error,
            accuracy_percent = EXCLUDED.accuracy_percent,
            created_at = CURRENT_TIMESTAMP;
    """

    cur.execute(
        query,
        (
            prediction_date,
            "ทองคำแท่ง",
            float(actual_price),
            float(predicted_price),
            float(error),
            float(accuracy_percent),
            MODEL_NAME,
        )
    )

    conn.commit()

    cur.close()
    conn.close()


# =========================
# WALK-FORWARD BACKTEST
# =========================

def run_backtest():

    df = load_gold_data()

    if len(df) < 20:
        raise ValueError(
            f"ข้อมูลมีเพียง {len(df)} วัน "
            "ต้องมีอย่างน้อย 20 วัน"
        )

    feature_df = create_features(df)

    predictions = []
    actuals = []

    logging.info(
        "เริ่ม Walk-Forward Backtest..."
    )

    # ---------------------------------
    # เริ่มทดสอบตั้งแต่วันที่มีข้อมูล
    # lag_7 เพียงพอ
    # ---------------------------------

    for test_idx in range(15, len(df) - 1):

        current_date = df.loc[
            test_idx,
            "price_date"
        ]

        current_price = float(
            df.loc[test_idx, "sell_price"]
        )

        next_date = df.loc[
            test_idx + 1,
            "price_date"
        ]

        actual_price = float(
            df.loc[test_idx + 1, "sell_price"]
        )

        # =========================
        # TRAIN DATA
        # =========================

        train_df = feature_df.iloc[
            :test_idx
        ].copy()

        train_df = train_df.dropna(
            subset=FEATURES + ["target"]
        )

        if len(train_df) < 10:
            continue

        X_train = train_df[FEATURES]
        y_train = train_df["target"]

        # =========================
        # TRAIN MODEL
        # =========================

        model = RandomForestRegressor(
            n_estimators=N_ESTIMATORS,
            random_state=42,
            max_features="sqrt",
            min_samples_leaf=1,
            n_jobs=-1,
        )

        model.fit(
            X_train,
            y_train
        )

        # =========================
        # CURRENT DAY FEATURES
        # =========================

        test_row = feature_df.iloc[
            test_idx
        ]

        if test_row[FEATURES].isna().any():
            continue

        X_test = pd.DataFrame(
            [test_row[FEATURES].values],
            columns=FEATURES
        )

        # =========================
        # PREDICT NEXT DAY CHANGE
        # =========================

        predicted_change = float(
            model.predict(X_test)[0]
        )

        predicted_price = (
            current_price
            + predicted_change
        )

        # =========================
        # ERROR
        # =========================

        error = abs(
            actual_price
            - predicted_price
        )

        # Accuracy แบบ percentage
        accuracy_percent = max(
            0,
            (
                1
                - error / actual_price
            )
            * 100
        )

        # =========================
        # SAVE
        # =========================

        save_result(
            prediction_date=next_date,
            actual_price=actual_price,
            predicted_price=predicted_price,
            error=error,
            accuracy_percent=accuracy_percent,
        )

        predictions.append(
            predicted_price
        )

        actuals.append(
            actual_price
        )

        logging.info(
            f"{current_date} → {next_date} | "
            f"Actual={actual_price:.2f} | "
            f"Predicted={predicted_price:.2f} | "
            f"Error={error:.2f}"
        )

    # =========================
    # OVERALL METRICS
    # =========================

    if predictions:

        mae = mean_absolute_error(
            actuals,
            predictions
        )

        rmse = mean_squared_error(
            actuals,
            predictions
        ) ** 0.5

        r2 = r2_score(
            actuals,
            predictions
        )

        logging.info(
            "================================"
        )

        logging.info(
            "BACKTEST RESULT"
        )

        logging.info(
            f"จำนวน Prediction = {len(predictions)}"
        )

        logging.info(
            f"MAE = {mae:.2f}"
        )

        logging.info(
            f"RMSE = {rmse:.2f}"
        )

        logging.info(
            f"R2 = {r2:.4f}"
        )

        logging.info(
            "================================"
        )

    else:
        logging.warning(
            "ไม่สามารถสร้าง prediction ได้"
        )


# =========================
# AIRFLOW DAG
# =========================

with DAG(
    dag_id="gold_backtest_pipeline",
    start_date=datetime(2026, 9, 1),
    schedule=None,
    catchup=False,
    tags=["gold", "ml", "backtest"],
) as dag:

    backtest = PythonOperator(
        task_id="walk_forward_backtest",
        python_callable=run_backtest,
    )

    backtest