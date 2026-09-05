from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import psycopg2


API_URL = "https://www.thaigoldtoday.com/api/gold-price"


def extract_gold_price():
    response = requests.get(API_URL, timeout=10)
    response.raise_for_status()

    data = response.json()

    print("ดึงข้อมูลราคาทองสำเร็จ")
    return data["current"]


def transform_gold_price(**context):
    data = context["ti"].xcom_pull(
        task_ids="extract_gold_price"
    )

    clean_data = {
        "buy_bar": data["buyBar"],
        "sell_bar": data["sellBar"],
        "buy_ornament": data["buyOrnament"],
        "sell_ornament": data["sellOrnament"],
        "change": data["change"],
        "change_percent": data["changePercent"],
        "updated_at": data["updatedAt"]
    }

    print("Clean ข้อมูลสำเร็จ")
    print(clean_data)

    return clean_data


def load_gold_price(**context):
    data = context["ti"].xcom_pull(
        task_ids="transform_gold_price"
    )

    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        database="airflow",
        user="airflow",
        password="airflow"
    )

    cursor = conn.cursor()

    # ทองคำแท่ง
    cursor.execute(
        """
        INSERT INTO gold_prices
        (price_date, gold_type, buy_price, sell_price,
         change_price, change_percent, updated_at)
        VALUES
        (CURRENT_DATE, %s, %s, %s, %s, %s, %s)
        """,
        (
            "ทองคำแท่ง",
            data["buy_bar"],
            data["sell_bar"],
            data["change"],
            data["change_percent"],
            data["updated_at"]
        )
    )

    # ทองรูปพรรณ
    cursor.execute(
        """
        INSERT INTO gold_prices
        (price_date, gold_type, buy_price, sell_price,
         change_price, change_percent, updated_at)
        VALUES
        (CURRENT_DATE, %s, %s, %s, %s, %s, %s)
        """,
        (
            "ทองรูปพรรณ",
            data["buy_ornament"],
            data["sell_ornament"],
            data["change"],
            data["change_percent"],
            data["updated_at"]
        )
    )

    conn.commit()

    cursor.close()
    conn.close()

    print("บันทึก PostgreSQL สำเร็จ")


with DAG(
    dag_id="gold_price_pipeline",
    start_date=datetime(2026, 9, 1),
    schedule="0 9 * * *",
    catchup=False,
    tags=["gold", "ETL"]
) as dag:

    extract = PythonOperator(
        task_id="extract_gold_price",
        python_callable=extract_gold_price
    )

    transform = PythonOperator(
        task_id="transform_gold_price",
        python_callable=transform_gold_price
    )

    load = PythonOperator(
        task_id="load_postgres",
        python_callable=load_gold_price
    )

    extract >> transform >> load