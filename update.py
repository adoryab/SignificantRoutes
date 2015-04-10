# update.py

# handles API calls and reverse geocoding
# currently set up to work with SQLite databases

##### COMMAND LINE USAGE #####

# SYNTAX: "python update.py [service to use] 
#          [parameter value] ... [parameter value]"

# [service to use] = "Google Geocoding" or "GeoNames" or "Google Places"
# [parameter] = "minIndex" or "maxIndex" or "key"

# if just "python update.py" is called, all services will be run with
# default settings

import requests
import sqlite3
import sys

class StatusException(Exception): # Raised when results aren't returned
    pass

class InputException(Exception): # Raised for improper command line inputs
    pass

def updateRow(data, **information):
    """Updates a single row in a table using id_col to locate rows
    and data to provide columns and values; adds new columns where
    necessary"""
    table = information.get('table', "locations")
    id_col = information.get('id_col', "_id")
    database = information.get('database', "locations.db")
    con = information.get('connection', sqlite3.connect(database))
    nameIndex = 1 # index of table info that contains column name
    cursor = con.cursor()
    cursor.execute("PRAGMA table_info({tn})".format(tn=table))
    existingCols = [item[nameIndex] for item in cursor.fetchall()]
    newColumns = [item for item in data if item not in existingCols]
    for col in newColumns: # defaults to adding a TEXT column
        cursor.execute("ALTER TABLE {tn} ADD COLUMN '{cn}' TEXT"\
                       .format(tn=table, cn=col))
    ID = data[id_col]
    del data[id_col] # removes ID from data to avoid updating ID
    for item in data:
        if ((type(data[item]) == str or type(data[item]) == unicode) and "'" in data[item]): 
            data[item] = (data[item]).replace("'", "")
        cursor.execute("UPDATE {tn} SET {cn} = ('{value}') WHERE {idc} = ({ID})"\
                       .format(tn=table, cn=item, value=data[item], 
                                  idc=id_col, ID=ID))
    con.commit()
    if "connection" not in information: con.close()

def getJSON(URL, parameters):
    """Simply calls a HTTP get request and gets a JSON object"""
    r = requests.get(url=URL, params=parameters)
    return r.json()

def getData(URL, item, key):
    """Creates a dictionary using a JSON request for row updates"""
    parameters = dict()
    latIndex, lonIndex = 3, 4 # locations in item where lat/lon are
    # Google Geocoding API
    if URL == "https://maps.googleapis.com/maps/api/geocode/json":
        translator = dict() # converts Google's descriptions
        translator['administrative_area_level_2'] = "county"
        translator['administrative_area_level_1'] = "state"
        translator['route'] = "street"
        translator['political'] = "city"
        parameters['latlng'] = str(item[latIndex]) + "," + str(item[lonIndex])
        parameters['key'] = key
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
                    data[locType] = name
    # GeoNames API (Places)
    elif URL == "http://api.geonames.org/findNearbyPlaceNameJSON?":
        parameters['lat'] = str(item[latIndex])
        parameters['lng'] = str(item[lonIndex])
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
        parameters['lat'] = str(item[latIndex])
        parameters['lng'] = str(item[lonIndex])
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
        parameters['location'] = str(item[latIndex]) + "," + str(item[lonIndex])
        parameters['radius'] = 500
        parameters['rankby'] = "prominence"
        JSONObject = getJSON(URL, parameters)
        if JSONObject['status'] != "OK": raise StatusException(JSONObject['status'])
        data = dict()
        places = JSONObject['results']
        numPlaces = 5 # number of places to return
        for i in xrange(1, numPlaces + 1):
            data["name_" + str(i)] = places[i]['name']
            data["type_" + str(i)] = places[i]['types'][0]
    return data

def updateTable(**kwargs):
    """Updates an entire table using latitude/longitude data"""
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
        ID = item[0] # assumes ID is first column
        data[idc] = ID
        print "updating", ID, ("(%s)" % (URL)) # print statement for CL monitoring
        updateRow(data, table=table, id_col=idc, database=database, connection=con)
    con.commit()
    if "connection" not in kwargs: con.close()

def createTable(database, table, original):
    """Creates a new table based on a template 
    after checking that it doesn't already exist"""
    con = sqlite3.connect(database)
    c = con.cursor()
    c.execute("SELECT name from sqlite_master WHERE type='table' AND name='{tn}'"\
              .format(tn=table))
    fetch = c.fetchall() # checks to see if table already exists
    if (len(fetch) > 0 and table in fetch[0]): return
    with con: # otherwise creates table using statement to create original table
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

def update(service, database="locations.db", table="locations", 
           minIndex=0, maxIndex=None, key=None):
    """Given a service and database, etc., updates address/location data"""
    # contains default keys, calls createTable and updateTable
    if service == "Google Geocoding":
        newTable = "geocoding"
        if key == None:
            key = "AIzaSyB3ffCdODdeKP8zz1CvO40QRqpDB5UzHiA"
        URL = "https://maps.googleapis.com/maps/api/geocode/json"
        createTable(database, newTable, table)
        updateTable(key=key, URL=URL, table=newTable,
                    minIndex=minIndex, maxIndex=maxIndex)
    elif service == "Google Places":
        newTable = "places"
        if key == None:
            key = "AIzaSyCN84vI7drlWNjVwPM-pNLZnV5HW0OAEVc"
        URL = "https://maps.googleapis.com/maps/api/place/nearbysearch/json?"
        createTable(database, newTable, table)
        updateTable(key=key, URL=URL, table=newTable,
                    minIndex=minIndex, maxIndex=maxIndex)
    elif service == "GeoNames":
        newTable = "GeoNames"
        if key == None:
            key = "sqlite1"
        URL_intersections = "http://api.geonames.org/findNearestIntersectionJSON?"
        URL_places = "http://api.geonames.org/findNearbyPlaceNameJSON?"
        createTable(database, newTable, table)
        try:
            try:
                updateTable(key=key, URL=URL_intersections, table=newTable,
                            minIndex=minIndex, maxIndex=maxIndex)
                updateTable(key=key, URL=URL_places, table=newTable,
                            minIndex=minIndex, maxIndex=maxIndex)
            except StatusException:
                try:
                    updateTable(key=key, URL=URL_intersections, table=newTable,
                                minIndex=minIndex, maxIndex=maxIndex)
                except StatusException: 
                    updateTable(key=key, URL=URL_places, table=newTable,
                            minIndex=minIndex, maxIndex=maxIndex)
        except:
            pass

def hackyUpdate(service, database="locations.db", table="locations",
                minIndex=0, maxIndex=None, key=None):
    """Just returns true when an update is successful"""
    update(service=service, database=database, table=table, minIndex=minIndex,
           maxIndex=maxIndex, key=key)
    return True

def runUpdate(service, keySet, minIndex=0, maxIndex=8000, 
              chunkSize=10, attempts=10):
    # LIMITATION: maxIndex must be an int!
    """Runs an update in chunks; rotates keys to assure success"""
    keyIndex = 0
    key = keySet[keyIndex%(len(keySet))]
    for ind in xrange(minIndex, (maxIndex/chunkSize)+1):
        status = False
        minInd = ind * chunkSize
        maxInd = minInd + (chunkSize - 1)
        while (status != True):
            try:
                status = hackyUpdate(service=service, 
                                     key=key, minIndex=minInd, 
                                     maxIndex=maxInd)
            except:
                keyIndex += 1
                if keyIndex >= (attempts * len(keySet)): 
                    raise StatusException("All keys failed!")
                key = keySet[(keyIndex%(len(keySet)))]

googleKeys = ["AIzaSyAp4w8LLCVozx5X9SrJ3PiflwCng1ik1Y8",
              "AIzaSyB6gqWstIJN6WipZRAyzPVO5umSD3j4tWY",
              "AIzaSyC6oyobPYiUlnDNsnJrMTMZ-2kB8P_t6VA",
              "AIzaSyD52DsLAJFLRXFkIv_LRJtdOK7_ESjYKDM",
              "AIzaSyC6A-dlO1A-tobttQINQqBlLf7yA-fKhHU",
              "AIzaSyDWwG3t6J174Db1rNy1HMTNYK0I7qjphGM",
              "AIzaSyACCLSff26AZD3XCs2DFx3bse_ozmMScPo",
              "AIzaSyCODgpWYCzNUIkzID1TyPXIa91kNa8LU_I",
              "AIzaSyCF6oPByEx4NQyNF9lKZ-jpYj3SC5waOfo",
              "AIzaSyArhKRF62AP0Ggwhq0JRgJlJS5UwAeYolA",
              "AIzaSyDICubFC6OVgraLNVWjuQI_kn6wz1IdxjE"]

gNamesKeys = ["sqlite_updater", "sqlite1", "sqlite3", 
              "sqlite4", "sqlite5", "0sqlite", "sqlite6"]

args = sys.argv

if len(args) == 1:
    runUpdate(service="Google Geocoding", keySet=googleKeys)
    runUpdate(service="Google Places", keySet=googleKeys)
    runUpdate(service="GeoNames", keySet=gNamesKeys)

elif (len(args) == 2) or (len(args) == 3 and args[1] == "Google"):
    service = "".join([item for item in args[1:]])
    if service == "GeoNames": keySet = gNamesKeys
    else: keySet = googleKeys
    runUpdate(service=service, keySet=keySet)

else:

    # handles multi-word service names
    if len(args) > 1 and args[1] == "Google":
        args[1] = args[1] + " " + args[2]
        length = len(args)
        for i in xrange(2, (length-1)):
            args[i] = args[i+1]
        args = args[:-1]

    arg = dict()
    arg['service'] = args[1]
    arg['minIndex'] = 0
    arg['maxIndex'] = None
    arg['key'] = None

    if "minIndex" in args and args.index("minIndex") != (len(args) - 1):
        try:
            arg['minIndex'] = int(args[args.index("minIndex")+1])
        except: raise InputException("minIndex must be an int!")

    if "maxIndex" in args and args.index("maxIndex") != (len(args) - 1):
        try:
            arg['maxIndex'] = int(args[args.index("maxIndex")+1])
        except: raise InputException("maxIndex must be an int!")

    if "key" in args and args.index("key") != (len(args) - 1):
        try:
            arg['key'] = str(args[args.index("key")+1])
        except: raise InputException("key must be a string!")

    update(service=arg['service'], minIndex=arg['minIndex'],
           maxIndex=arg['maxIndex'], key=arg['key'])