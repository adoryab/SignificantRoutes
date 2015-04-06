# sqlite_update.py

# calls Google's Reverse Geocoding API to add location data
# to an SQLite database containing latitude/longitude values

# @TODO:
# 1. speed up update function
# 2. improve output sanitation in parseLocationData
# 3. make code less specific to this particular database
# 4. set this up to work with command line
# 5. verify that path to database is valid (sqlite3 doesn't check)

API_Key = "AIzaSyCN84vI7drlWNjVwPM-pNLZnV5HW0OAEVc"
# unnecessary for these particular calls, just used for monitoring
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
                if ("sublocality" in locType): locType = "sublocality"
                elif (locType == "administrative_area_level_2"): locType = "county"
                elif (locType == "administrative_area_level_1"): locType = "state"
                elif (locType == "route"): locType = "street"
                elif (locType == "political"): locType = "city"
				# @TODO: rewrite previous few lines
                result[locType] = name
	return result

def update(database="locations.db", table="locations", 
           columns=["street", "locality", "neighborhood", "city", "county", "state",
                    "country", "postal_code"], id_col="_id"):
    con = sqlite3.connect(database)
    cursor = con.cursor()
    for col in columns:
        cursor.execute("ALTER TABLE {tn} ADD COLUMN '{cn}' TEXT"\
            .format(tn=table, cn=col))
    cursor.execute("SELECT * FROM {tn}".format(tn=table))
    fetch = cursor.fetchone()
    while fetch is not None:
        id_num = fetch[0]
        print "current ID", id_num
    	latitude = fetch[3] # @TODO: modify so that these lines aren't specific
    	longitude = fetch[4] # to just this database
    	jsonObject = getLocationData(latitude, longitude)
    	result = parseLocationData(jsonObject)
    	fetch = cursor.fetchone()
        for item in result:
            if item in columns:
                c.execute("UPDATE {tn} SET {cn}=('{value}') WHERE {idc}=({ID})".\
                          format(tn=table, cn=item, idc=id_col, ID=id_num))
    con.commit()
    con.close()

update()