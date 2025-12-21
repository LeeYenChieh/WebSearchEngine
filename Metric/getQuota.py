import requests
import os

api_key = os.environ.get('SERPAPI_KEY')
url = "https://serpapi.com/account.json"

params = {
    "api_key": api_key
}

resp = requests.get(url, params=params)
resp.raise_for_status()

data = resp.json()

print("本月已用搜尋次數:", data.get("this_month_usage"))
print("本月剩餘搜尋次數:", data.get("plan_searches_left"))
print("總剩餘搜尋次數:", data.get("total_searches_left"))
