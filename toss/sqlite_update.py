# sqlite_update.py

# calls Google's Reverse Geocoding API to add location data
# to an SQLite database containing latitude/longitude values

# @TODO:
# 1. speed up update function
# 2. improve output sanitation in parseLocationData
# 3. make code less specific to this particular database
# 4. set this up to work with command line
# 5. verify that path to database is valid (sqlite3 doesn't check)
# 6. time calls to avoid exceeding Google's query limit (5/sec)
# 7. style (eliminate magic numbers and such)
# 8. set up multithreading
# 9. memory mapping?
# 10. add named places and intersections

# API_Key = None
# API_Key = "AIzaSyCN84vI7drlWNjVwPM-pNLZnV5HW0OAEVc"
# API_Key = "AIzaSyAuAWiaukaws8JUwaLkiivaCz3P7X5e498"
API_Key = "AIzaSyB3ffCdODdeKP8zz1CvO40QRqpDB5UzHiA"
# unnecessary for these particular calls, just used for monitoring
# 5 calls/second and 25,000 calls/day on this key

URL = "https://maps.googleapis.com/maps/api/geocode/json"
GeoNames = 

import requests
import time
import sqlite3

def getLocationData(latitude, longitude):
    parameters = dict()
    parameters['latlng'] = str(latitude) + "," + str(longitude)
    # parameters['result_type'] = "bus_station"
    if API_Key != None: parameters['key'] = API_Key
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
                # @TODO: rewrite previous few lines (as a dictionary)
                result[locType] = name
    if jsonObject['status'] == "ZERO_RESULTS":
        result = None
    return result

def update(database="locations.db", table="locations", 
           columns=["street_number", "street", "locality", "neighborhood", "city", 
                    "county", "state", "country", "postal_code", "intersection",
                    "premise", "colloquial_area", "sublocality", "airport", "park",
                    "natural_feature", "point_of_interest", "bus_station"], id_col="_id",
                    startIndex = 0, endIndex = None):
    con = sqlite3.connect(database)
    with con:
        cursor = con.cursor()
        c = con.cursor() # secondary cursor
        c.execute("PRAGMA table_info({tn})".format(tn=table))
        existingCols = [item[1] for item in c.fetchall()]
        newColumns = [item for item in columns if item not in existingCols]
        for col in newColumns:
          cursor.execute("ALTER TABLE {tn} ADD COLUMN '{cn}' TEXT"\
             .format(tn=table, cn=col))
        cursor.execute("SELECT * FROM {tn} WHERE {idc} >= {min}"\
             .format(tn=table, idc = id_col, min = startIndex))
        fetch = cursor.fetchone()
        id_num = startIndex - 1
        while fetch is not None and (endIndex is None or id_num < endIndex):
            id_num = fetch[0]
            if id_num % 100 == 0: print "id:", id_num
            latitude = fetch[3] # @TODO: modify so that these lines aren't specific
            longitude = fetch[4] # to just this database
            jsonObject = getLocationData(latitude, longitude)
            result = parseLocationData(jsonObject)
            fetch = cursor.fetchone()
            if (result == None): 
                continue
            if (result == dict()): break
            for item in result:
                if item in columns:
                    with con:
                        cur = con.cursor()
                        cur.execute("UPDATE {tn} SET {cn}=('{value}') WHERE {idc}=({ID})"\
                            .format(tn=table, cn=item, value=result[item], 
                                               idc=id_col, ID=id_num))
        con.commit()
    con.close()

def check(index, database="locations.db", table="locations", id_col = "_id"):
    con = sqlite3.connect(database)
    with con:
        cursor = con.cursor()
        cursor.execute("SELECT * FROM {tn} WHERE {idc} = {i}"\
            .format(tn=table, idc=id_col, i=index))
        fetch = cursor.fetchone()
    return fetch

update()