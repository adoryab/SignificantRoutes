# update.py

# handles API calls and reverse geocoding
# currently set up to work with SQLite databases

import requests
import sqlite3
import sys

class StatusException(Exception):
    pass

def updateRow(data, **information):
    table = information.get('table', "locations")
    id_col = information.get('id_col', "_id")
    database = information.get('database', "locations.db")
    con = information.get('connection', sqlite3.connect(database))
    nameIndex = 1 # index of table info that contains column name
    cursor = con.cursor()
    cursor.execute("PRAGMA table_info({tn})".format(tn=table))
    existingCols = [item[nameIndex] for item in cursor.fetchall()]
    newColumns = [item for item in data if item not in existingCols]
    for col in newColumns:
        cursor.execute("ALTER TABLE {tn} ADD COLUMN '{cn}' TEXT"\
                       .format(tn=table, cn=col))
    ID = data[id_col]
    del data[id_col]
    for item in data:
        if (type(data[item]) == str and "'" in data[item]): 
        	data[item] = (data[item]).replace("'", "")
        cursor.execute("UPDATE {tn} SET {cn} = ('{value}') WHERE {idc} = ({ID})"\
                       .format(tn=table, cn=item, value=data[item], 
                                  idc=id_col, ID=ID))
    con.commit()
    if "connection" not in information: con.close()

def getJSON(URL, parameters):
    r = requests.get(url=URL, params=parameters)
    return r.json()

def getData(URL, item, key):
    parameters = dict()
    # Google Geocoding API
    if URL == "https://maps.googleapis.com/maps/api/geocode/json":
        translator = dict()
        translator['administrative_area_level_2'] = "county"
        translator['administrative_area_level_1'] = "state"
        translator['route'] = "street"
        translator['political'] = "city"
        parameters['latlng'] = str(item[3]) + "," + str(item[4])
        JSONObject = getJSON(URL, parameters)
        if JSONObject['status'] != "OK":
            raise StatusException(JSONObject['status'])
        data = dict()
        for item in JSONObject['results']:
            address = item['address_components']
            for item in address:
                locType = item['types'][0]
                if locType in translator: locType = translator[locType]
                if locType not in data:
                    name = item['short_name']
                    data['locType'] = name
    # GeoNames API (Places)
    elif URL == "http://api.geonames.org/findNearbyPlaceNameJSON?":
        parameters['lat'] = str(item[3])
        parameters['lng'] = str(item[4])
        parameters['username'] = key
        JSONObject = getJSON(URL, parameters)
        if 'geonames' not in JSONObject: raise StatusException("Uh oh! " 
            + str(JSONObject))
        data = dict()
        location = JSONObject['geonames'][0]
        for item in location:
            locType = item
            name = location[locType]
            data[locType] = name
    # GeoNames API (Intersection)
    elif URL == "http://api.geonames.org/findNearestIntersectionJSON?":
        parameters['lat'] = str(item[3])
        parameters['lng'] = str(item[4])
        parameters['username'] = key
        JSONObject = getJSON(URL, parameters)
        if 'intersection' not in JSONObject: raise StatusException("No data returned")
        data = dict()
        intersection = JSONObject['intersection']
        for item in intersection:
            locType = item
            name = intersection[locType]
            data[locType] = name
        street1, street2 = intersection['street1'], intersection['street2']
        if street1 > street2: street1, street2 = street2, street1
        streets = street1 + " at " + street2
        data["streets"] = streets
    # Google Nearby Places
    elif URL == "https://maps.googleapis.com/maps/api/place/nearbysearch/json?":
        parameters['key'] = key
        parameters['location'] = str(item[3]) + "," + str(item[4])
        parameters['radius'] = 500
        parameters['rankby'] = "prominence"
        JSONObject = getJSON(URL, parameters)
        if JSONObject['status'] != "OK": raise StatusException(status)
        data = dict()
        places = JSONObject['results']
        for i in xrange(1, 6):
            data["name_" + str(i)] = places[i]['name']
            data["type_" + str(i)] = places[i]['types'][0]
    return data

def updateTable(**kwargs):
    key = kwargs.get('key', None)
    database = kwargs.get('database', "locations.db")
    table = kwargs.get('table', "locations")
    con = kwargs.get('connection', sqlite3.connect(database))
    idc = kwargs.get('id_column', "_id")
    URL = kwargs.get("URL", "https://maps.googleapis.com/maps/api/geocode/json")
    minIndex, maxIndex = kwargs.get('minIndex', 0), kwargs.get('maxIndex', None)
    with con:
        cursor = con.cursor()
        if maxIndex != None:
            cursor.execute("SELECT * FROM {tn} WHERE {idc} >= {min} AND {idc} <= {max}"\
                              .format(tn=table, idc=idc, min=minIndex, max=maxIndex))
        else:
            cursor.execute("SELECT * FROM {tn} WHERE {idc} >= {min}"\
                              .format(tn=table, idc=idc, min=minIndex))
    for item in cursor.fetchall():
        data = getData(URL, item, key)
        ID = item[0]
        data[idc] = ID
        print "updating", ID, ("(%s)" % (URL))
        updateRow(data, table=table, id_col=idc, database=database, connection=con)
    con.commit()
    if "connection" not in kwargs: con.close()

def createTable(database, table, original):
    con = sqlite3.connect(database)
    c = con.cursor()
    c.execute("SELECT name from sqlite_master WHERE type='table' AND name='{tn}'"\
              .format(tn=table))
    fetch = c.fetchall()
    if (len(fetch) > 0 and table in fetch[0]): return
    with con:
        cursor = con.cursor()
        cursor.execute("SELECT sql FROM sqlite_master WHERE type='table' \
                        AND name='{ot}'".format(ot=original))
        statement = cursor.fetchone()[0]
        command = statement.replace(original, table, 1)
        cursor.execute(command)
        cursor.execute("INSERT INTO {tn} SELECT * FROM {ot}"\
                       .format(tn=table, ot=original))
    con.commit()
    con.close()

def update(type, database="locations.db", table="locations", 
           minIndex=0, maxIndex=None):
    if type == "Google Geocoding":
        newTable = "geocoding"
        key = "AIzaSyB3ffCdODdeKP8zz1CvO40QRqpDB5UzHiA"
        URL = "https://maps.googleapis.com/maps/api/geocode/json"
        createTable(database, newTable, table)
        updateTable(key=key, URL=URL, table=newTable,
                    minIndex=minIndex, maxIndex=maxIndex)
    elif type == "Google Places":
        key = "AIzaSyCN84vI7drlWNjVwPM-pNLZnV5HW0OAEVc"
        URL = "https://maps.googleapis.com/maps/api/place/nearbysearch/json?"
        newTable = "places"
        createTable(database, newTable, table)
        updateTable(key=key, URL=URL, table=newTable,
                    minIndex=minIndex, maxIndex=maxIndex)
    elif type == "GeoNames":
        key = "sqlite1"
        URL_intersections = "http://api.geonames.org/findNearestIntersectionJSON?"
        URL_places = "http://api.geonames.org/findNearbyPlaceNameJSON?"
        newTable = "GeoNames"
        createTable(database, newTable, table)
        updateTable(key=key, URL=URL_intersections, table=newTable,
                    minIndex=minIndex, maxIndex=maxIndex)
        updateTable(key=key, URL=URL_places, table=newTable,
                    minIndex=minIndex, maxIndex=maxIndex)

update("Google Geocoding", minIndex=0, maxIndex=None)
update("Google Places", minIndex=0, maxIndex=None)
update("GeoNames", minIndex=0, maxIndex=None)