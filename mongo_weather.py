import os 
import requests
from datetime import datetime
from pymongo import MongoClient

KEY = "test"
client = MongoClient(host="localhost", port=27017, username="ls", password="pw")

def make_data(city):
    """extracting weather and main only"""
    r = requests.get(
        url="https://api.openweathermap.org/data/2.5/weather?q={}&appid={}".format(
            city, KEY
        )
    )
    data = r.json()
    clean_data = {i: data[i] for i in ["weather", "main"]}
    clean_data["weather"] = clean_data["weather"][0]
    return clean_data


def add_key(data, city):
    """Adding current time and city"""
    current = datetime.now().strftime("%H:%M:%S")
    data["time"] = current
    data["city"] = city
    return data


def add_data(client, cities):
    col = client["sample"]["weather"]
    for city in cities:
        data = make_data(city)
        data = add_key(data, city)
        col.insert_one(data)
        
        
add_data(client, ["paris", "london", "rome","madrid"])

#  displaying insertion
c_weather = client["sample"]["weather"]
print(c_weather.find_one())
print()

#  finding cloudy cities
col=client["sample"]["weather"]
for i in list(col.find({"weather.main": "Clouds"}, {"_id": 0, "city": 1})):
    print(f"Cloudy city: {i}")
    
    
print()

#  cities with temp > 280 and < 291
print("cities with Kelvin temperature > 280 and < 291")
col=client["sample"]["weather"]
print(
    len(
        list(
            col.find(
                {
                    "$and": [
                        {"main.temp_min": {"$gte": 280}},
                        {"main.temp_max": {"$lte": 291}},
                    ]
                }
            )
        )
    )
)
print()

#  number of documents by weather type
print("number of documents by weather type")
col=client["sample"]["weather"]
for i in list(col.aggregate([{"$group": {"_id": "$weather.main", "nb": {"$sum": 1}}}])):
    print(i)
    
    
    
rm3kt7ifmpru9jok1t186r176c@group.calendar.google.com
