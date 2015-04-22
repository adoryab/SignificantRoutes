# dbupdate.py

# script for database updating

# @TODO:
# ADD COMMAND LINE INTERFACE

# BREAK PIPELINE INTO FUNCTIONS

# ADD HANDLING OF NULL/NONE VALUES 
# WITHOUT CREATING NULL/NONE CONFUSION

import sys
from services import Service
from updater import Updater
from datetime import datetime
from math import radians, cos, sin, asin, sqrt

# FUNCTIONS

def haversine(items, lat1Key, lon1Key, lat2Key, lon2Key, resultKey):
    lat1, lon1, lat2, lon2 = items[lat1Key], items[lon1Key], items[lat2Key], items[lon2Key]
    lat1, lon1, lat2, lon2 = map(radians, [float(lat1), float(lon1),
                                           float(lat2), float(lon2)])
    deltaLat, deltaLon = lat2 - lat1, lon2 - lon1
    a = sin(deltaLat/2)**2 + cos(lat1) * cos(lat2) * sin(deltaLon/2)**2
    b = 2 * asin(sqrt(a))
    radius = 3956
    result = dict()
    result[resultKey] = (float(radius) * b)
    return result

def convertTimestamp(item):
    unixTime = item['timestamp']
    if len(str(int(unixTime))) > 10:
        unixTime = unixTime/1000.0
    realTime = datetime.fromtimestamp(unixTime)
    result = dict()
    result['year'] = realTime.year
    result['month'] = realTime.month
    result['hour'] = realTime.hour
    result['minute'] = realTime.minute
    result['second'] = realTime.second
    result['microsecond'] = realTime.microsecond
    return result

def getTransition(item, database, table, columnOfInterest, jump=1, maxJump=5):
    ID = item["_id"]
    updater = Updater(database)
    nextRow = (updater.getRows(table=table, column="_id", col_value=(int(ID)+jump)))
    while (columnOfInterest not in nextRow or nextRow[columnOfInterest] is None) and jump <= maxJump:
        jump += 1
        nextRow = (updater.getRows(table=table, column="_id", col_value=(int(ID)+jump)))
    if nextRow is not None and len(nextRow) > 0:
        nextRow = nextRow[0]
    else: nextRow = dict()
    result = dict()
    if columnOfInterest in nextRow:
        nextValue = nextRow[columnOfInterest]
        if nextValue is not None:
            result[str(columnOfInterest) + "_next"] = nextValue
    return result

def combineCols(item, newCol=None, reorder=False, *cols):
    if newCol is None:
        newCol = cols[0]
        for i in xrange(1, len(cols)):
            newCol += "/" + cols[i]
    result, newValue = dict(), []
    for col in cols:
        if col not in item or item[col] is None:
            return result
        newValue.append(str(item[col]))
    if reorder:
        newValue.sort()
    newValue = ",".join(newValue)
    if newValue == "": newValue = None
    result[newCol] = newValue
    return result

def getDataFromService(item, service):
    return service.getData(item)

def addPlaceInformation(table, uid, newTable, P1_name, distinct_places, updater, con=None):
    conCreated = False
    if (con is None):
        con, conCreated = updater.getConnection(), True
    for item in distinct_places:
        data = dict()
        data['uid'] = uid
        data['P1'] = item
        data['P1_label'] = updater.getAssociatedValue(table=table, col1=P1_name,
                                                     value=item, col2="label", con=con)
        data['frequency'] = updater.getFrequency(table=table, columns={P1_name:item}, con=con)
        for i in xrange(0, 24, 2):
            category = str(i) + '-' + str(i+1)
            data[category] = updater.getFrequency(table=table, columns={P1_name:item, 'hour':[i,i+1]}, con=con)
        updater.addRow(table=newTable, data=data, con=con)
    if conCreated: con.close()

def addTransitionInformation(table, uid, newTable, P1_name, P2_name, 
                             transition_name, distinct_transitions, updater, con=None):
    conCreated = False
    if (con is None):
        con, conCreated = updater.getConnection(), True
    for item in distinct_transitions:
        data = dict()
        data['uid'] = uid
        data['P1'] = updater.getAssociatedValue(table=table, col1=transition_name,
                                                value=item, col2=P1_name, con=con)
        data['P1_label'] = updater.getAssociatedValue(table=table, col1=P1_name,
                                                      value=(data['P1']), col2="label", con=con)
        data['P2'] = updater.getAssociatedValue(table=table, col1=transition_name,
                                                value=item, col2=P2_name, con=con)
        data['P2_label'] = updater.getAssociatedValue(table=table, col1=P2_name,
                                                      value=(data['P2']), col2="label", con=con)
        data['frequency'] = updater.getFrequency(table=table, columns={transition_name:item}, con=con)
        for i in xrange(0, 24, 2):
            category = str(i) + '-' + str(i+1)
            data[category] = updater.getFrequency(table=table, columns={transition_name:item, 'hour':[i,i+1]}, con=con)
        updater.addRow(table=newTable, data=data, con=con)
    if conCreated: con.close()

def addInformation(table, newTable, P1_name, P2_name, transition_name, uid_col, uid, updater, con=None):
    conCreated = False
    if (con is None):
        con, conCreated = updater.getConnection(), True
    print "newTable is", newTable
    print "Getting place category!"
    distinct_places = updater.getDistinctValues(table=table, column=P1_name, conditions={uid_col:uid})
    print "Getting transition category!"
    distinct_transitions = updater.getDistinctValues(table=table, column=transition_name, conditions={uid_col:uid})
    print "Adding place information!"
    addPlaceInformation(table=table, uid=uid, newTable=newTable, P1_name=P1_name, 
                        distinct_places=distinct_places, updater=updater, con=con)
    addTransitionInformation(table=table, uid=uid, newTable=newTable, P1_name=P1_name, P2_name=P2_name,
                             transition_name=transition_name, distinct_transitions=distinct_transitions,
                             updater=updater, con=con)
    if conCreated: con.close()

def geoNamesProcessor(inputs, error=False):
    if 'double_latitude' in inputs:
        inputs['lat'] = str(inputs['double_latitude'])
    elif error: raise IOError("Missing parameter!")
    if 'double_longitude' in inputs:
        inputs['lng'] = str(inputs['double_longitude'])
    elif error: raise IOError("Missing parameter!")
    return inputs

def placesProcessor(inputs, error=False):
    if 'double_latitude' in inputs and 'double_longitude' in inputs:
        inputs['location'] = str(inputs['double_latitude']) + "," + str(inputs['double_longitude'])
    elif error: raise IOError("Missing parameter!")
    inputs['radius'] = 500
    inputs['rankby'] = "prominence"
    return inputs

def geocodingProcessor(inputs, error=False):
    if 'double_latitude' in inputs and 'double_longitude' in inputs:
        inputs['latlng'] = str(inputs['double_latitude']) + "," + str(inputs['double_longitude'])
    elif error: raise IOError("Missing parameter!")
    return inputs

def geoNamesIntersectionInterpreter(call):
    return call['geonames'][0]

def geoNamesPlacesInterpreter(call):
    call = call['intersection']
    street1, street2 = call['street1'], call['street2']
    if street1 > street2:
        street1, street2 = street2, street1
    streets = street1 + "at" + street2
    call['streets'] = streets
    return call

def placesInterpreter(call, numPlaces=5):
    call = JSONObject['results']
    for i in xrange(1, numPlaces + 1):
        data["name_" + str(i)] = call[i]['name']
        data["type_" + str(i)] = call[i]['types'][0]
    return call

def geocodingInterpreter(call):
    data = dict()
    for item in call['results']:
        address = item['address_components']
        for loc in address:
            locType = loc['types'][0]
            if locType not in data:
                name = loc['short_name']
                data[locType] = name
    return data

# FUNCTIONS TO CREATE WRAPPER FUNCTIONS

def getTransitionGenerator(database, table, columnOfInterest):
    def transitionGetter(item):
        return getTransition(item=item, database=database, table=table, columnOfInterest=columnOfInterest)
    return transitionGetter

def combineColsGenerator(newCol, reorder=False, *cols):
    def colCombiner(item):
        return combineCols(item, newCol, reorder, *cols)
    return colCombiner

def serviceDataGenerator(service):
    def getServiceData(item):
        return getDataFromService(item=item, service=service)
    return getServiceData

def haversineGenerator(index):
    lat2Key, lon2Key = ("lat_" + str(index)), ("lat_" + str(index))
    resultKey = ("distance_" + str(index))
    def getHaversine(items):
        return haversine(items, lat1Key='double_latitude', lon1Key='double_longitude',
                         lat2Key=lat2Key, lon2Key=lon2Key, resultKey=resultKey)
    return getHaversine

# MAIN FUNCTION

def update(): # script to run here (FULL PIPELINE)
    updater = Updater("locations.db")
    con = updater.getConnection()

    """tables = ["locations", "GeoNames", "places", "geocoding"]
    for table in tables:
        updater.updateTable(table=table, id_col="_id", fun=convertTimestamp, monitor=5, con=con)

    updater.copyTable(newTable="GeoNames", originalTable="locations", con=con)
    updater.copyTable(newTable="places", originalTable="locations", con=con)
    updater.copyTable(newTable="geocoding", originalTable="locations", con=con)

    gNamesKeys = ["sqlite_updater", "sqlite1", "sqlite3", 
                  "sqlite4", "sqlite5", "0sqlite", "sqlite6"]

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
                  "AIzaSyDICubFC6OVgraLNVWjuQI_kn6wz1IdxjE",
                  "AIzaSyCpE7FF2O1PLrhNSGBb2iGdt6Zsji0iOOk",
                  "AIzaSyA0FvsQjfOd-yiDaE_QmngfAdZURxr2djQ",
                  "AIzaSyBkXo1vsBBLqbua26WOMKt4Hcjk4SgObrM",
                  "AIzaSyDRdVL-4M-2gIJem0ebiWXxCeShsBe4xC0",
                  "AIzaSyBs4gSTVsvYxL9yTITNnT0O_b9Sv95KEsU"]

    geocodingTranslator = dict()
    geocodingTranslator['administrative_area_level_2'] = "county"
    geocodingTranslator['administrative_area_level_1'] = "state"
    geocodingTranslator['route'] = "street"
    geocodingTranslator['political'] = "city"

    geoNamesIntersection = Service(name="GeoNames Intersection", 
                                   URL="http://api.geonames.org/findNearestIntersectionJSON?",
                                   required=['lat', 'lng'], keys=gNamesKeys,
                                   confirm=["intersection"], tries=10, skips=10,
                                   connects=10, interpreteter=geoNamesIntersectionInterpreter,
                                   processor=geoNamesProcessor, append="_intersection",
                                   key_id="username")

    geoNamesPlaces = Service(name="GeoNames Places",
                             URL="http://api.geonames.org/findNearbyPlaceNameJSON?",
                             required=['lat', 'lng'], keys=gNamesKeys,
                             confirm=["geonames"], tries=10, skips=10, connects=10,
                             interpreter=geoNamesPlacesInterpreter, processor=geoNamesProcessor,
                             append="_places", key_id="username")

    googlePlaces = Service(name="Google Places", 
                           URL="https://maps.googleapis.com/maps/api/place/nearbysearch/json?",
                           required=['location'], keys=googleKeys, confirm={'status':"OK"},
                           tries=10, skips=10, connects=10, intepreter=placesInterpreter,
                           processor=placesProcessor)

    googleGeocoding = Service(name="Google Geocoding",
                              URL="https://maps.googleapis.com/maps/api/geocode/json?",
                              required=['latlng'], keys=(googleKeys + [None]),
                              confirm={'status':"OK"}, tries=10, skips=10, connects=10,
                              interpreter=geocodingInterpreter, processor=geocodingProcessor)

    updater.UpdateTable(table="GeoNames", id_col="_id", fun=serviceDataGenerator(geoNamesIntersection), monitor=5, con=con)
    updater.UpdateTable(table="GeoNames", id_col="_id", fun=serviceDataGenerator(geoNamesPlaces), monitor=5, con=con)
    updater.UpdateTable(table="places", id_col="_id", fun=serviceDataGenerator(googlePlaces), monitor=5, con=con)
    updater.UpdateTable(table="geocoding", id_col="_id", fun=serviceDataGenerator(googleGeocoding), monitor=5, con=con)
    
    # ADD DISTANCE TO EACH PLACE IN "places"
    numPlaces = 5
    for i in xrange(1, numPlaces+1):
        updater.updateTable(table="places", id_col="_id", fun=haversineGenerator(i), monitor=5, con=con)"""

    updater.updateTable(table="places", id_col="_id", fun=combineColsGenerator("names_all", True, "name_1", "name_2", "name_3", 
                                                                               "name_4", "name_5"), monitor=5, con=con)

    updater.updateTable(table="GeoNames", id_col="_id", fun=getTransitionGenerator("locations.db", "GeoNames", "streets_intersections"), 
                        monitor=5, con=con)
    updater.updateTable(table="GeoNames", id_col="_id", fun=getTransitionGenerator("locations.db", "GeoNames", "toponymName_places"),
                        monitor=5, con=con)
    updater.updateTable(table="places", id_col="_id", fun=getTransitionGenerator("locations.db", "places", "name_1"),
                        monitor=5, con=con)
    updater.updateTable(table="places", id_col="_id", fun=getTransitionGenerator("locations.db", "places", "names_all"),
                        monitor=5, con=con)
    updater.updateTable(table="geocoding", id_col="_id", fun=getTransitionGenerator("locations.db", "geocoding", "street"), 
                        monitor=5, con=con)
    updater.updateTable(table="geocoding", id_col="_id", fun=getTransitionGenerator("locations.db", "geocoding", "establishment"), 
                        monitor=5, con=con)
    updater.updateTable(table="geocoding", id_col="_id", fun=getTransitionGenerator("locations.db", "geocoding", "bus_station"), 
                        monitor=5, con=con)
    updater.updateTable(table="geocoding", id_col="_id", fun=getTransitionGenerator("locations.db", "geocoding", "neighborhood"), 
                        monitor=5, con=con)
    
    updater.updateTable(table="GeoNames", id_col="_id", 
                        fun=combineColsGenerator("streets_transition", False, "streets_intersections", "streets_intersections_next"), 
                        monitor=5, con=con)
    updater.updateTable(table="GeoNames", id_col="_id",
                        fun=combineColsGenerator("toponym_transition", False, "toponymName_places", "toponymName_places_next"), 
                        monitor=5, con=con)
    updater.updateTable(table="places", id_col="_id",
                        fun=combineColsGenerator("name_1_transition", False, "name_1", "name_1_next"), 
                        monitor=5, con=con)
    updater.updateTable(table="places", id_col="_id",
                        fun=combineColsGenerator("names_all_transition", False, "names_all", "names_all_next"), 
                        monitor=5, con=con)
    updater.updateTable(table="geocoding", id_col="_id", 
                        fun=combineColsGenerator("street_transition", False, "street", "street_next"),
                        monitor=5, con=con)
    updater.updateTable(table="geocoding", id_col="_id", 
                        fun=combineColsGenerator("establishment_transition", False, "establishment", "establishment_next"), 
                        monitor=5, con=con)
    updater.updateTable(table="geocoding", id_col="_id", 
                        fun=combineColsGenerator("bus_station_transition", False, "bus_station", "bus_station_next"), 
                        monitor=5, con=con)
    updater.updateTable(table="geocoding", id_col="_id", 
                        fun=combineColsGenerator("neighborhood_transition", False, "neighborhood", "neighborhood_next"),
                        monitor=5, con=con)
    
    tables = ["locations_intersections", "locations_areas", "locations_places",  "locations_allplaces","locations_street",
              "locations_establishment", "locations_station", "locations_neighborhood"]
    for table in tables:
        updater.createTable(table=table, con=con)

    uid_col, coreTable = "device_id", "locations"
    user_IDs = updater.getDistinctValues(table=coreTable, column=uid_col)

    for uid in user_IDs:
        addInformation(table="GeoNames", newTable="locations_intersections", P1_name="streets_intersections", 
                       P2_name="streets_intersections_next", transition_name="streets_transition", uid_col=uid_col, 
                       uid=uid, updater=updater, con=con)
        addInformation(table="GeoNames", newTable="locations_areas", P1_name="toponymName_places", 
                       P2_name="toponymName_places_next", transition_name="toponym_transition", uid_col=uid_col, 
                       uid=uid, updater=updater, con=con)
        addInformation(table="places", newTable="locations_places", P1_name="name_1", 
                       P2_name="name_1_next", transition_name="name_1_transition", uid_col=uid_col, 
                       uid=uid, updater=updater, con=con)
        addInformation(table="places", newTable="locations_allplaces", P1_name="names_all", 
                       P2_name="names_all_next", transition_name="names_all_transition", uid_col=uid_col, 
                       uid=uid, updater=updater, con=con)
        addInformation(table="geocoding", newTable="locations_street", P1_name="street", 
                       P2_name="street_next", transition_name="street_transition", uid_col=uid_col, 
                       uid=uid, updater=updater, con=con)
        addInformation(table="geocoding", newTable="locations_establishment", P1_name="establishment", 
                       P2_name="establishment_next", transition_name="establishment_transition", uid_col=uid_col, 
                       uid=uid, updater=updater, con=con)
        addInformation(table="geocoding", newTable="locations_station", P1_name="bus_station", 
                       P2_name="bus_station_next", transition_name="bus_station_transition", uid_col=uid_col, 
                       uid=uid, updater=updater, con=con)
        addInformation(table="geocoding", newTable="locations_neighborhood", P1_name="neighborhood",
                       P2_name="neighborhood_next", transition_name="neighborhood_transition", uid_col=uid_col, 
                       uid=uid, updater=updater, con=con)
    con.close()

# COMMAND LINE INTERFACE

args = sys.argv[1:]

if len(args) == 0:
    update() # RUN FULL PIPELINE