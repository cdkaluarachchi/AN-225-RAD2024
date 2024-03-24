import requests

API_KEY = "5e68e78603755969e4dbb47f458154bf"

url = f"https://api.openweathermap.org/data/2.5/weather?lat=6.9271&lon=79.8612&appid={API_KEY}"

data = requests.get(url)
parsed = data.json()
print(parsed)

