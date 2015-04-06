# sqlite_update.py

# calls Google's Reverse Geocoding API to add location data
# to an SQLite database containing latitude/longitude values

API_Key = "AIzaSyCN84vI7drlWNjVwPM-pNLZnV5HW0OAEVc"
# unnecessary for these calls, just used for monitoring
# 5 calls/second and 25,000 calls/day on this key

URL = "https://maps.googleapis.com/maps/api/geocode/json"

import requests
import sqlite3

def getLocationData(latitude, longitude):
    parameters = dict()
    parameters['latlng'] = str(latitude) + "," + str(longitude)
    parameters['key'] = API_Key
    r = requests.get(url=URL, params=parameters)
    return r.json()

def parseLocationData(jsonObject):
	result = dict()
	if jsonObject['status'] == "OK":
		callResult = jsonObject['results']
		for item in callResult:
			address = item['address_components']
			for element in address:
				name = element['short_name']
				locType = element['types'][0]
				# @TODO: rewrite next few lines
				if "sublocality" in locType: locType = "sublocality"
				elif (locType == "administrative_area_level_2"):
					locType = "county"
				elif (locType == "administrative_area_level_1"):
					locType = "city"
				# @TODO: rewrite previous few lines
				result[locType] = name
	return result

def update(database="locations.db"):
    con = sqlite3.connect(database)
    cursor = con.cursor()
    cursor.execute("select * from locations")
    fetch = cursor.fetchone()
    while fetch is not None:
    	if fetch[0]%10 == 0: print fetch[0]
    	latitude = fetch[3]
    	longitude = fetch[4]
    	jsonObject = getLocationData(latitude, longitude)
    	result = parseLocationData(jsonObject)
    	fetch = cursor.fetchone()

update()
