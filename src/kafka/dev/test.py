""" data = '{"coord": {"lon": 79.8612, "lat": 6.9271}, "weather": [{"id": 803, "main": "Clouds", "description": "broken clouds", "icon": "04n"}], "base": "stations", "main": {"temp": 299.43, "feels_like": 299.43, "temp_min": 299.43, "temp_max": 299.43, "pressure": 1013, "humidity": 77, "sea_level": 1013, "grnd_level": 1012}, "visibility": 10000, "wind": {"speed": 1.39, "deg": 172, "gust": 1.69}, "clouds": {"all": 59}, "dt": 1710440641, "sys": {"country": "LK", "sunrise": 1710377258, "sunset": 1710420717}, "timezone": 19800, "id": 1248991, "name": "Colombo", "cod": 200}'
datadict = eval(data)
print(datadict['coord']) """
import json
import time
import requests
import datetime
import pytz

API_KEY = "5e68e78603755969e4dbb47f458154bf"

""" def load_json(file_path):
        # Read JSON data from the file
    try:
        with open(file_path, 'r') as file:
            json_data = file.read()
        # Load JSON into a dictionary
        data_dict = json.loads(json_data)
        return data_dict
    except Exception as e:
        print(f"An error occurred: {e}") """

#data = load_json("../metadata/locationdata.json")

""" for location in data:
    print(location)
    print(f"{data[location][0]} {data[location][1]}")
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={data[location][0]}&lon={data[location][1]}&appid={API_KEY}"
    #time.sleep(10)
    data = requests.get(url)
    parsed = data.json() """

execute_timestamp = datetime.datetime.now(pytz.utc)
ist_timezone = pytz.timezone('Asia/Kolkata')
ist_execute_timestamp = execute_timestamp.astimezone(ist_timezone)
print(ist_execute_timestamp[:-13])
