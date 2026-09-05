import requests

API_URL = "https://www.thaigoldtoday.com/api/gold-price"


def get_gold_price():
    response = requests.get(API_URL, timeout=10)
    response.raise_for_status()

    data = response.json()
    current = data["current"]

    return {
        "buy_bar": current["buyBar"],
        "sell_bar": current["sellBar"],
        "buy_ornament": current["buyOrnament"],
        "sell_ornament": current["sellOrnament"],
        "change": current["change"],
        "change_percent": current["changePercent"],
        "updated_at": current["updatedAt"]
    }


if __name__ == "__main__":
    print(get_gold_price())