"""
gold_ml_pipeline.py
===================

Gold Price ML Pipeline
Apache Airflow + PostgreSQL + RandomForestRegressor

Pipeline:

Extract
    ↓
Prepare
    ↓
Train
    ↓
Evaluate
    ↓
Smoke Test / Prediction
    ↓
Save Prediction
    ↓
Branch
   ↙   ↘
Deploy  Skip Deploy
   ↘   ↙
     Log


แนวคิดของ Model
----------------
ให้ Model ทำนาย "การเปลี่ยนแปลงของราคาทอง"

ตัวอย่าง:

ราคาวันนี้ = 69,150
Model ทำนาย change = +250

Prediction = 69,400

Target:
    price_change_next_day
"""

import os
import shutil

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import (
    PythonOperator,
    BranchPythonOperator,
)


# ================================================================
# CONFIG
# ================================================================

default_args = {
    "owner": "gold-ml",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


# ================================================================
# MODEL CONFIG
# ================================================================

MODEL_DIR = "/opt/airflow/models/gold_models"

CURRENT_MODEL_PATH = os.path.join(
    MODEL_DIR,
    "current_model.pkl",
)

MAE_THRESHOLD = 300.0

N_ESTIMATORS = 300


# ================================================================
# DATABASE CONNECTION
# ================================================================

def get_db_connection():

    import psycopg2

    return psycopg2.connect(
        host="postgres",
        port=5432,
        database="airflow",
        user="airflow",
        password="airflow",
    )


# ================================================================
# 1. EXTRACT
# ================================================================

def extract_data(**kwargs):

    import pandas as pd

    conn = None

    try:

        conn = get_db_connection()

        query = """
            SELECT
                id,
                price_date,
                sell_price
            FROM gold_prices
            WHERE gold_type = 'ทองคำแท่ง'
              AND sell_price IS NOT NULL
            ORDER BY price_date ASC, id ASC
        """

        df = pd.read_sql(
            query,
            conn,
        )

    finally:

        if conn is not None:
            conn.close()


    if df.empty:

        raise ValueError(
            "ไม่พบข้อมูลทองคำแท่งใน gold_prices"
        )


    # ------------------------------------------------------------
    # เลือก Record ล่าสุดของแต่ละวัน
    # ------------------------------------------------------------

    df = (
        df
        .sort_values(
            ["price_date", "id"]
        )
        .groupby(
            "price_date",
            as_index=False
        )
        .last()
    )


    df = df[
        [
            "price_date",
            "sell_price",
        ]
    ]


    # ------------------------------------------------------------
    # Convert Data Type
    # ------------------------------------------------------------

    df["price_date"] = pd.to_datetime(
        df["price_date"]
    )

    df["sell_price"] = pd.to_numeric(
        df["sell_price"],
        errors="coerce",
    )


    df = df.dropna(
        subset=[
            "price_date",
            "sell_price",
        ]
    )


    df = (
        df
        .sort_values("price_date")
        .reset_index(drop=True)
    )


    if len(df) < 15:

        raise ValueError(
            f"ข้อมูลมีเพียง {len(df)} วัน "
            "ต้องมีอย่างน้อย 15 วัน"
        )


    # ------------------------------------------------------------
    # Convert เป็น List
    # ------------------------------------------------------------

    records = []

    for _, row in df.iterrows():

        records.append(
            {
                "price_date": row[
                    "price_date"
                ].strftime("%Y-%m-%d"),

                "sell_price": float(
                    row["sell_price"]
                ),
            }
        )


    # ------------------------------------------------------------
    # XCom
    # ------------------------------------------------------------

    ti = kwargs["ti"]

    ti.xcom_push(
        key="gold_data",
        value=records,
    )


    # ------------------------------------------------------------
    # LOG
    # ------------------------------------------------------------

    print("======================================")
    print("          EXTRACT GOLD DATA")
    print("======================================")

    print(
        f"จำนวนข้อมูล : {len(records)} วัน"
    )

    print(
        f"วันที่เริ่มต้น : "
        f"{records[0]['price_date']}"
    )

    print(
        f"วันที่ล่าสุด : "
        f"{records[-1]['price_date']}"
    )

    print(
        f"ราคาล่าสุด : "
        f"{records[-1]['sell_price']:,.2f} บาท"
    )

    print("Extract สำเร็จ")

    print("======================================")


# ================================================================
# 2. PREPARE
# ================================================================

def prepare_data(**kwargs):

    import pandas as pd

    ti = kwargs["ti"]


    # ------------------------------------------------------------
    # ดึงข้อมูล
    # ------------------------------------------------------------

    records = ti.xcom_pull(
        task_ids="extract_data",
        key="gold_data",
    )


    if not records:

        raise ValueError(
            "ไม่พบข้อมูลจาก extract_data"
        )


    df = pd.DataFrame(records)


    if len(df) < 15:

        raise ValueError(
            f"ข้อมูลมีเพียง {len(df)} วัน"
        )


    # ------------------------------------------------------------
    # Convert
    # ------------------------------------------------------------

    df["price_date"] = pd.to_datetime(
        df["price_date"]
    )

    df["sell_price"] = pd.to_numeric(
        df["sell_price"],
        errors="coerce",
    )


    df = df.dropna(
        subset=[
            "price_date",
            "sell_price",
        ]
    )


    df = (
        df
        .sort_values("price_date")
        .reset_index(drop=True)
    )


    # ============================================================
    # FEATURE ENGINEERING
    # ============================================================

    df["current_price"] = (
        df["sell_price"]
    )


    # Lag

    df["lag_1"] = (
        df["sell_price"].shift(1)
    )

    df["lag_2"] = (
        df["sell_price"].shift(2)
    )

    df["lag_3"] = (
        df["sell_price"].shift(3)
    )

    df["lag_7"] = (
        df["sell_price"].shift(7)
    )


    # Rolling Mean

    df["rolling_mean_3"] = (
        df["sell_price"]
        .rolling(window=3)
        .mean()
    )

    df["rolling_mean_7"] = (
        df["sell_price"]
        .rolling(window=7)
        .mean()
    )


    # Price Change

    df["price_change_1"] = (
        df["sell_price"]
        - df["lag_1"]
    )

    df["price_change_3"] = (
        df["sell_price"]
        - df["lag_3"]
    )


    # ============================================================
    # TARGET
    # ============================================================

    next_price = (
        df["sell_price"].shift(-1)
    )


    df["target"] = (
        next_price
        - df["sell_price"]
    )


    # ============================================================
    # FEATURES
    # ============================================================

    features = [

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


    # ------------------------------------------------------------
    # Drop NaN
    # ------------------------------------------------------------

    df = df.dropna(
        subset=features + ["target"]
    ).reset_index(
        drop=True
    )


    if len(df) < 10:

        raise ValueError(
            f"หลังสร้าง Feature เหลือ "
            f"{len(df)} แถว"
        )


    # ============================================================
    # TRAIN / TEST SPLIT
    # ============================================================

    split_index = int(
        len(df) * 0.8
    )


    if split_index < 8:

        raise ValueError(
            "Train data น้อยเกินไป"
        )


    if len(df) - split_index < 2:

        raise ValueError(
            "Test data น้อยเกินไป"
        )


    train_df = df.iloc[
        :split_index
    ].copy()


    test_df = df.iloc[
        split_index:
    ].copy()


    # ============================================================
    # X / Y
    # ============================================================

    X_train = train_df[
        features
    ]

    y_train = train_df[
        "target"
    ]


    X_test = test_df[
        features
    ]

    y_test = test_df[
        "target"
    ]


    # ============================================================
    # XCom
    # ============================================================

    ti.xcom_push(
        key="X_train",
        value=X_train.values.tolist(),
    )

    ti.xcom_push(
        key="y_train",
        value=y_train.values.tolist(),
    )

    ti.xcom_push(
        key="X_test",
        value=X_test.values.tolist(),
    )

    ti.xcom_push(
        key="y_test",
        value=y_test.values.tolist(),
    )


    # ============================================================
    # LATEST FEATURE
    # ============================================================

    latest = df.iloc[-1]


    latest_features = [

        float(
            latest[feature]
        )

        for feature in features

    ]


    ti.xcom_push(
        key="latest_features",
        value=[
            latest_features
        ],
    )


    # ------------------------------------------------------------
    # Current Price
    # ------------------------------------------------------------

    current_price = float(
        latest["sell_price"]
    )


    ti.xcom_push(
        key="current_price",
        value=current_price,
    )


    # ------------------------------------------------------------
    # Latest Date
    # ------------------------------------------------------------

    latest_date = (
        latest["price_date"]
        .strftime("%Y-%m-%d")
    )


    ti.xcom_push(
        key="latest_date",
        value=latest_date,
    )


    # ============================================================
    # LOG
    # ============================================================

    print("======================================")
    print("          PREPARE GOLD DATA")
    print("======================================")

    print(
        f"ข้อมูลหลัง Feature : {len(df)} แถว"
    )

    print(
        f"Train : {len(train_df)} แถว"
    )

    print(
        f"Test  : {len(test_df)} แถว"
    )

    print()

    print("Features:")

    for index, feature in enumerate(
        features,
        start=1,
    ):

        print(
            f"  {index}. {feature}"
        )

    print()

    print(
        f"วันที่ล่าสุด : {latest_date}"
    )

    print(
        f"ราคาล่าสุด : "
        f"{current_price:,.2f} บาท"
    )

    print(
        "Target = การเปลี่ยนแปลงราคา "
        "ของวันถัดไป"
    )

    print("Prepare สำเร็จ")

    print("======================================")


# ================================================================
# 3. TRAIN MODEL
# ================================================================

def train_model(**kwargs):

    import joblib

    from sklearn.ensemble import (
        RandomForestRegressor
    )


    ti = kwargs["ti"]


    X_train = ti.xcom_pull(
        task_ids="prepare_data",
        key="X_train",
    )


    y_train = ti.xcom_pull(
        task_ids="prepare_data",
        key="y_train",
    )


    if not X_train:

        raise ValueError(
            "ไม่พบ X_train"
        )


    if not y_train:

        raise ValueError(
            "ไม่พบ y_train"
        )


    # ============================================================
    # RANDOM FOREST
    # ============================================================

    model = RandomForestRegressor(

        n_estimators=N_ESTIMATORS,

        random_state=42,

        max_features="sqrt",

        min_samples_leaf=1,

        n_jobs=-1,

    )


    model.fit(
        X_train,
        y_train,
    )


    # ============================================================
    # MODEL DIRECTORY
    # ============================================================

    os.makedirs(
        MODEL_DIR,
        exist_ok=True,
    )


    run_id = (
        kwargs["run_id"]
        .replace(":", "-")
        .replace("+", "-")
        .replace("/", "-")
        .replace(" ", "-")
    )


    candidate_path = os.path.join(
        MODEL_DIR,
        f"candidate_{run_id}.pkl",
    )


    joblib.dump(
        model,
        candidate_path,
    )


    if not os.path.exists(
        candidate_path
    ):

        raise FileNotFoundError(
            "สร้าง Candidate Model ไม่สำเร็จ"
        )


    ti.xcom_push(
        key="candidate_model_path",
        value=candidate_path,
    )


    # ============================================================
    # LOG
    # ============================================================

    print("======================================")
    print("             TRAIN MODEL")
    print("======================================")

    print(
        "Model : RandomForestRegressor"
    )

    print(
        f"Estimators : {N_ESTIMATORS}"
    )

    print(
        "Target : Price Change"
    )

    print(
        f"Training data : "
        f"{len(X_train)} แถว"
    )

    print(
        f"Candidate Model : "
        f"{candidate_path}"
    )

    print("Train สำเร็จ")

    print("======================================")


# ================================================================
# 4. EVALUATE MODEL
# ================================================================

def evaluate_model(**kwargs):

    import joblib
    import numpy as np

    from sklearn.metrics import (
        mean_absolute_error,
        mean_squared_error,
        r2_score,
    )


    ti = kwargs["ti"]


    # ------------------------------------------------------------
    # Candidate Model
    # ------------------------------------------------------------

    candidate_path = ti.xcom_pull(
        task_ids="train_model",
        key="candidate_model_path",
    )


    if not candidate_path:

        raise ValueError(
            "ไม่พบ Candidate Model"
        )


    if not os.path.exists(
        candidate_path
    ):

        raise FileNotFoundError(
            f"ไม่พบ Model: {candidate_path}"
        )


    # ------------------------------------------------------------
    # Test Data
    # ------------------------------------------------------------

    X_test = ti.xcom_pull(
        task_ids="prepare_data",
        key="X_test",
    )


    y_test_change = ti.xcom_pull(
        task_ids="prepare_data",
        key="y_test",
    )


    if not X_test:

        raise ValueError(
            "ไม่พบ X_test"
        )


    if not y_test_change:

        raise ValueError(
            "ไม่พบ y_test"
        )


    # ------------------------------------------------------------
    # Load Model
    # ------------------------------------------------------------

    model = joblib.load(
        candidate_path
    )


    # ------------------------------------------------------------
    # Predict Change
    # ------------------------------------------------------------

    predicted_change = model.predict(
        X_test
    )


    # ------------------------------------------------------------
    # Current Price
    # ------------------------------------------------------------

    current_prices = np.array(
        X_test
    )[:, 0]


    # ------------------------------------------------------------
    # Actual Price
    # ------------------------------------------------------------

    actual_change = np.array(
        y_test_change
    )


    actual_prices = (
        current_prices
        + actual_change
    )


    # ------------------------------------------------------------
    # Predicted Price
    # ------------------------------------------------------------

    predicted_prices = (
        current_prices
        + predicted_change
    )


    # ============================================================
    # METRICS
    # ============================================================

    mae = mean_absolute_error(
        actual_prices,
        predicted_prices,
    )


    rmse = np.sqrt(
        mean_squared_error(
            actual_prices,
            predicted_prices,
        )
    )


    r2 = r2_score(
        actual_prices,
        predicted_prices,
    )


    # ============================================================
    # XCom
    # ============================================================

    ti.xcom_push(
        key="mae",
        value=float(mae),
    )

    ti.xcom_push(
        key="rmse",
        value=float(rmse),
    )

    ti.xcom_push(
        key="r2",
        value=float(r2),
    )


    # ============================================================
    # LOG
    # ============================================================

    print("======================================")
    print("          MODEL EVALUATION")
    print("======================================")

    print(
        "Target : Next Day Gold Price"
    )

    print(
        "Prediction Method : "
        "Current Price + Predicted Change"
    )

    print()

    print(
        f"MAE  : {mae:,.2f} บาท"
    )

    print(
        f"RMSE : {rmse:,.2f} บาท"
    )

    print(
        f"R2   : {r2:.4f}"
    )

    print()

    print(
        f"เกณฑ์ Deploy : "
        f"MAE <= {MAE_THRESHOLD:,.2f} บาท"
    )


    if mae <= MAE_THRESHOLD:

        print(
            "ผลประเมิน : PASS"
        )

    else:

        print(
            "ผลประเมิน : NOT PASS"
        )


    print("======================================")


# ================================================================
# 5. PREDICT / SMOKE TEST
# ================================================================

def smoke_test(**kwargs):
    """
    ใช้ Candidate Model ทำนายราคาวันถัดไป

    จุดสำคัญ:
    Task นี้ทำก่อน Decide Deploy

    ดังนั้นถึง Model จะไม่ผ่าน MAE
    ก็ยังสามารถสร้าง Prediction ได้
    """

    import joblib

    ti = kwargs["ti"]


    # ------------------------------------------------------------
    # Candidate Model
    # ------------------------------------------------------------

    candidate_path = ti.xcom_pull(
        task_ids="train_model",
        key="candidate_model_path",
    )


    if not candidate_path:

        raise ValueError(
            "ไม่พบ Candidate Model"
        )


    if not os.path.exists(
        candidate_path
    ):

        raise FileNotFoundError(
            f"ไม่พบ Candidate Model: "
            f"{candidate_path}"
        )


    # ------------------------------------------------------------
    # Load Candidate Model
    # ------------------------------------------------------------

    model = joblib.load(
        candidate_path
    )


    # ------------------------------------------------------------
    # Latest Features
    # ------------------------------------------------------------

    latest_features = ti.xcom_pull(
        task_ids="prepare_data",
        key="latest_features",
    )


    current_price = ti.xcom_pull(
        task_ids="prepare_data",
        key="current_price",
    )


    latest_date = ti.xcom_pull(
        task_ids="prepare_data",
        key="latest_date",
    )


    if not latest_features:

        raise ValueError(
            "ไม่พบ latest_features"
        )


    if current_price is None:

        raise ValueError(
            "ไม่พบ current_price"
        )


    # ============================================================
    # PREDICT CHANGE
    # ============================================================

    predicted_change = model.predict(
        latest_features
    )[0]


    predicted_change = float(
        predicted_change
    )


    # ============================================================
    # FINAL PRICE
    # ============================================================

    predicted_price = (
        float(current_price)
        + predicted_change
    )


    # ============================================================
    # XCom
    # ============================================================

    ti.xcom_push(
        key="predicted_change",
        value=predicted_change,
    )


    ti.xcom_push(
        key="predicted_price",
        value=predicted_price,
    )


    # ============================================================
    # LOG
    # ============================================================

    print("======================================")
    print("       SMOKE TEST / PREDICTION")
    print("======================================")

    print(
        f"ข้อมูลล่าสุด : {latest_date}"
    )

    print(
        f"ราคาปัจจุบัน : "
        f"{current_price:,.2f} บาท"
    )

    print(
        f"Predicted Change : "
        f"{predicted_change:+,.2f} บาท"
    )

    print(
        f"ราคาที่คาดการณ์ : "
        f"{predicted_price:,.2f} บาท"
    )

    print()

    print(
        "ใช้ Candidate Model สำหรับ Prediction"
    )

    print(
        "Smoke Test สำเร็จ"
    )

    print("======================================")


# ================================================================
# 6. SAVE PREDICTION
# ================================================================

def save_prediction(**kwargs):
    """
    บันทึก Prediction ลง PostgreSQL

    Task นี้ทำก่อน Decide Deploy

    ดังนั้น:
        Model ผ่าน  -> Save Prediction
        Model ไม่ผ่าน -> Save Prediction

    การ Deploy เป็นอีกเรื่องหนึ่ง
    """

    ti = kwargs["ti"]


    # ------------------------------------------------------------
    # Prediction
    # ------------------------------------------------------------

    prediction = ti.xcom_pull(
        task_ids="smoke_test",
        key="predicted_price",
    )


    predicted_change = ti.xcom_pull(
        task_ids="smoke_test",
        key="predicted_change",
    )


    # ------------------------------------------------------------
    # Latest Date
    # ------------------------------------------------------------

    latest_date = ti.xcom_pull(
        task_ids="prepare_data",
        key="latest_date",
    )


    if prediction is None:

        raise ValueError(
            "ไม่พบ Prediction"
        )


    if latest_date is None:

        raise ValueError(
            "ไม่พบ latest_date"
        )


    # ============================================================
    # Prediction Date
    # ============================================================

    prediction_date = (
        datetime.strptime(
            latest_date,
            "%Y-%m-%d",
        )
        + timedelta(days=1)
    ).date()


    # ============================================================
    # DATABASE
    # ============================================================

    conn = None
    cursor = None


    try:

        conn = get_db_connection()

        cursor = conn.cursor()


        # --------------------------------------------------------
        # ลบ Prediction เดิมของวันเดียวกัน
        # --------------------------------------------------------

        cursor.execute(
            """
            DELETE FROM gold_predictions
            WHERE prediction_date = %s
              AND gold_type = %s
            """,
            (
                prediction_date,
                "ทองคำแท่ง",
            ),
        )


        # --------------------------------------------------------
        # Insert
        # --------------------------------------------------------

        cursor.execute(
            """
            INSERT INTO gold_predictions
            (
                prediction_date,
                gold_type,
                predicted_price,
                model_name
            )
            VALUES (%s, %s, %s, %s)
            """,
            (
                prediction_date,
                "ทองคำแท่ง",
                prediction,
                "RandomForestRegressor",
            ),
        )


        conn.commit()


    except Exception:

        if conn is not None:
            conn.rollback()

        raise


    finally:

        if cursor is not None:
            cursor.close()

        if conn is not None:
            conn.close()


    # ============================================================
    # LOG
    # ============================================================

    print("======================================")
    print("          SAVE PREDICTION")
    print("======================================")

    print(
        f"Prediction date : "
        f"{prediction_date}"
    )

    print(
        f"Predicted change : "
        f"{predicted_change:+,.2f} บาท"
    )

    print(
        f"Predicted price : "
        f"{prediction:,.2f} บาท"
    )

    print(
        "Gold type : ทองคำแท่ง"
    )

    print(
        "Model : RandomForestRegressor"
    )

    print(
        "บันทึก Prediction สำเร็จ"
    )

    print("======================================")


# ================================================================
# 7. DECIDE DEPLOY
# ================================================================

def decide_deploy(**kwargs):
    """
    ตัดสินใจเฉพาะเรื่อง Deploy

    Prediction ถูกบันทึกไปแล้ว
    ไม่ว่าจะผ่านหรือไม่ผ่าน

    ถ้า MAE <= 300
        -> deploy_model

    ถ้า MAE > 300
        -> skip_deploy
    """

    ti = kwargs["ti"]


    mae = ti.xcom_pull(
        task_ids="evaluate_model",
        key="mae",
    )


    if mae is None:

        raise ValueError(
            "ไม่พบ MAE"
        )


    # ------------------------------------------------------------
    # PASS
    # ------------------------------------------------------------

    if mae <= MAE_THRESHOLD:

        print(
            f"MAE {mae:,.2f} <= "
            f"{MAE_THRESHOLD:,.2f}"
        )

        print(
            "ผลลัพธ์ : PASS"
        )

        print(
            "เลือกเส้นทาง -> deploy_model"
        )

        return "deploy_model"


    # ------------------------------------------------------------
    # FAIL
    # ------------------------------------------------------------

    print(
        f"MAE {mae:,.2f} > "
        f"{MAE_THRESHOLD:,.2f}"
    )

    print(
        "ผลลัพธ์ : NOT PASS"
    )

    print(
        "เลือกเส้นทาง -> skip_deploy"
    )

    return "skip_deploy"


# ================================================================
# 8. DEPLOY MODEL
# ================================================================

def deploy_model(**kwargs):

    ti = kwargs["ti"]


    candidate_path = ti.xcom_pull(
        task_ids="train_model",
        key="candidate_model_path",
    )


    if not candidate_path:

        raise ValueError(
            "ไม่พบ Candidate Model"
        )


    if not os.path.exists(
        candidate_path
    ):

        raise FileNotFoundError(
            f"ไม่พบ Candidate Model: "
            f"{candidate_path}"
        )


    os.makedirs(
        MODEL_DIR,
        exist_ok=True,
    )


    # ------------------------------------------------------------
    # Deploy
    # ------------------------------------------------------------

    shutil.copyfile(
        candidate_path,
        CURRENT_MODEL_PATH,
    )


    if not os.path.exists(
        CURRENT_MODEL_PATH
    ):

        raise FileNotFoundError(
            "Deploy ไม่สำเร็จ"
        )


    # ============================================================
    # LOG
    # ============================================================

    print("======================================")
    print("             DEPLOY MODEL")
    print("======================================")

    print(
        f"Candidate : {candidate_path}"
    )

    print(
        f"Current   : {CURRENT_MODEL_PATH}"
    )

    print("Deploy สำเร็จ")

    print("======================================")


# ================================================================
# 9. SKIP DEPLOY
# ================================================================

def skip_deploy(**kwargs):

    import os

    ti = kwargs["ti"]


    mae = ti.xcom_pull(
        task_ids="evaluate_model",
        key="mae",
    )


    print("======================================")
    print("             SKIP DEPLOY")
    print("======================================")


    if mae is not None:

        print(
            f"MAE : {mae:,.2f} บาท"
        )

        print(
            f"เกณฑ์ : "
            f"{MAE_THRESHOLD:,.2f} บาท"
        )

        print(
            "ผลลัพธ์ : ไม่ผ่านเกณฑ์"
        )


    print(
        "ข้ามการ Deploy Model รอบนี้"
    )


    if os.path.exists(
        CURRENT_MODEL_PATH
    ):

        print(
            "Model เดิมยังคงใช้งานต่อ"
        )

    else:

        print(
            "ยังไม่มี Current Model"
        )


    print("======================================")


# ================================================================
# 10. LOG RESULT
# ================================================================

def log_result(**kwargs):

    ti = kwargs["ti"]


    # ------------------------------------------------------------
    # Metrics
    # ------------------------------------------------------------

    mae = ti.xcom_pull(
        task_ids="evaluate_model",
        key="mae",
    )


    rmse = ti.xcom_pull(
        task_ids="evaluate_model",
        key="rmse",
    )


    r2 = ti.xcom_pull(
        task_ids="evaluate_model",
        key="r2",
    )


    # ------------------------------------------------------------
    # Prediction
    # ------------------------------------------------------------

    prediction = ti.xcom_pull(
        task_ids="smoke_test",
        key="predicted_price",
    )


    predicted_change = ti.xcom_pull(
        task_ids="smoke_test",
        key="predicted_change",
    )


    # ------------------------------------------------------------
    # Date
    # ------------------------------------------------------------

    latest_date = ti.xcom_pull(
        task_ids="prepare_data",
        key="latest_date",
    )


    # ============================================================
    # LOG
    # ============================================================

    print()
    print("======================================")
    print("       GOLD ML PIPELINE RESULT")
    print("======================================")


    print(
        "Model : RandomForestRegressor"
    )

    print(
        "Target : Next Day Price Change"
    )

    print()


    # ------------------------------------------------------------
    # Metrics
    # ------------------------------------------------------------

    if mae is not None:

        print(
            f"MAE   : {mae:,.2f} บาท"
        )

    else:

        print(
            "MAE   : N/A"
        )


    if rmse is not None:

        print(
            f"RMSE  : {rmse:,.2f} บาท"
        )

    else:

        print(
            "RMSE  : N/A"
        )


    if r2 is not None:

        print(
            f"R2    : {r2:.4f}"
        )

    else:

        print(
            "R2    : N/A"
        )


    print()


    print(
        f"เกณฑ์ Deploy : "
        f"MAE <= {MAE_THRESHOLD:,.2f} บาท"
    )


    if latest_date is not None:

        print(
            f"ข้อมูลล่าสุด : "
            f"{latest_date}"
        )


    print()


    # ------------------------------------------------------------
    # Prediction
    # ------------------------------------------------------------

    if prediction is not None:

        print(
            f"Prediction วันถัดไป : "
            f"{prediction:,.2f} บาท"
        )

        if predicted_change is not None:

            print(
                f"Predicted Change : "
                f"{predicted_change:+,.2f} บาท"
            )

        print(
            "Result : PREDICTION SAVED"
        )

    else:

        print(
            "Result : NO PREDICTION"
        )


    print("======================================")


# ================================================================
# DAG
# ================================================================

with DAG(

    dag_id="gold_ml_pipeline",

    default_args=default_args,

    description=(
        "Gold Price ML Pipeline "
        "with RandomForestRegressor"
    ),

    schedule=None,

    start_date=datetime(
        2026,
        9,
        1,
    ),

    catchup=False,

    tags=[
        "gold",
        "ml",
        "random-forest",
    ],

) as dag:


    # ============================================================
    # TASK 1 - EXTRACT
    # ============================================================

    extract_task = PythonOperator(

        task_id="extract_data",

        python_callable=extract_data,

    )


    # ============================================================
    # TASK 2 - PREPARE
    # ============================================================

    prepare_task = PythonOperator(

        task_id="prepare_data",

        python_callable=prepare_data,

    )


    # ============================================================
    # TASK 3 - TRAIN
    # ============================================================

    train_task = PythonOperator(

        task_id="train_model",

        python_callable=train_model,

    )


    # ============================================================
    # TASK 4 - EVALUATE
    # ============================================================

    evaluate_task = PythonOperator(

        task_id="evaluate_model",

        python_callable=evaluate_model,

    )


    # ============================================================
    # TASK 5 - SMOKE TEST / PREDICTION
    # ============================================================

    smoke_test_task = PythonOperator(

        task_id="smoke_test",

        python_callable=smoke_test,

    )


    # ============================================================
    # TASK 6 - SAVE PREDICTION
    # ============================================================

    save_prediction_task = PythonOperator(

        task_id="save_prediction",

        python_callable=save_prediction,

    )


    # ============================================================
    # TASK 7 - DECIDE DEPLOY
    # ============================================================

    decide_task = BranchPythonOperator(

        task_id="decide_deploy",

        python_callable=decide_deploy,

    )


    # ============================================================
    # TASK 8A - DEPLOY
    # ============================================================

    deploy_task = PythonOperator(

        task_id="deploy_model",

        python_callable=deploy_model,

    )


    # ============================================================
    # TASK 8B - SKIP
    # ============================================================

    skip_task = PythonOperator(

        task_id="skip_deploy",

        python_callable=skip_deploy,

    )


    # ============================================================
    # TASK 9 - LOG
    # ============================================================

    log_task = PythonOperator(

        task_id="log_result",

        python_callable=log_result,

        trigger_rule=(
            "none_failed_min_one_success"
        ),

    )


    # ============================================================
    # PIPELINE FLOW
    # ============================================================

    extract_task \
        >> prepare_task \
        >> train_task \
        >> evaluate_task \
        >> smoke_test_task \
        >> save_prediction_task \
        >> decide_task


    # ------------------------------------------------------------
    # PASS
    # ------------------------------------------------------------

    decide_task \
        >> deploy_task \
        >> log_task


    # ------------------------------------------------------------
    # FAIL
    # ------------------------------------------------------------

    decide_task \
        >> skip_task \
        >> log_task