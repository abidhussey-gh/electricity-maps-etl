import requests

key = "your_api_key"
headers = {"auth-token": "QPzESe4P5xQ84HkmbRHd"}

r1 = requests.get("https://api.electricitymaps.com/v4/electricity-mix/history?zone=FR", headers=headers)
print("Mix status:", r1.status_code)
print("Mix sample:", str(r1.json())[:500])

r2 = requests.get("https://api.electricitymaps.com/v4/electricity-flows/history?zone=FR", headers=headers)
print("Flows status:", r2.status_code)
print("Flows sample:", str(r2.json())[:500])