# dbupdate.py

# script for second phase of database updating

from services import Service
from updater import Updater
from datetime import datetime

# FUNCTIONS

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

def getTransition(item, database, table, columnOfInterest, jump=1):
    ID = item["_id"]
    updater = Updater(database)
    nextRow = (updater.getRows(table=table, column="_id", col_value=(int(ID)+jump)))[0]
    result = dict()
    if columnOfInterest in nextRow:
        nextValue = nextRow[columnOfInterest]
        result[str(columnOfInterest) + "_next"] = nextValue
    return result

def combineCols(item, col1, col2, newCol=None):
    if newCol is None:
        newCol = col1 + "/" + col2
    result = dict()
    if (col1 not in item or col2 not in item):
        return result
    col1_val = item[col1]
    col2_val = item[col2]
    result[newCol] = str(col1_val + "," + col2_val)
    return result

def addPlaceInformation(table, uid, newTable, P1_name, place_category, updater, con=None):
    conCreated = False
    if (con is None):
        con, conCreated = updater.getConnection(), True
    for item in place_category:
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
                             transition_name, transition_category, updater, con=None):
    conCreated = False
    if (con is None):
        con, conCreated = updater.getConnection(), True
    for item in transition_category:
        data = dict()
        data['uid'] = uid
        data['P1'] = updater.getAssociatedValue(table=table, col1=transition_name,
                                               value=item, col2=P1_name, con=con)
        data['P1_label'] = updater.getAssociatedValue(table=table, col1=transition_name,
                                                     value=(data['P1']), col2="label", con=con)
        data['P2'] = updater.getAssociatedValue(table=table, col1=transition_name,
                                               value=item, col2=P2_name, con=con)
        data['P2_label'] = updater.getAssociatedValue(table=table, col1=transition_name,
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
    place_category = updater.getDistinctValues(table=table, column=P1_name, conditions={uid_col:uid})
    transition_category = updater.getDistinctValues(table=table, column=transition_name, conditions={uid_col:uid})
    addPlaceInformation(table=table, uid=uid, newTable=newTable, P1_name=P1_name, 
                        place_category=place_category, updater=updater, con=con)
    addTransitionInformation(table=table, uid=uid, newTable=newTable, P1_name=P1_name, P2_name=P2_name,
                             transition_name=transition_name, transition_category=transition_category,
                             updater=updater, con=con)
    if conCreated: con.close()

# FUNCTIONS TO WRITE WRAPPER FUNCTIONS

def getTransitionGenerator(database, table, columnOfInterest):
    def transitionGetter(item):
        return getTransition(item=item, database=database, table=table, columnOfInterest=columnOfInterest)
    return transitionGetter

def combineColsGenerator(col1, col, newCol):
    def colCombiner(item):
        return combineCols(item=item, col1=col1, col2=col2, newCol=newCol)
    return colCombiner

# MAIN FUNCTION

def update(): # script to run here
    updater = Updater("locations.db")
    con = updater.getConnection()

    tables = ["locations", "GeoNames", "places", "geocoding"]
    for table in tables:
        updater.updateTable(table=table, id_col="_id", fun=convertTimestamp, monitor=5, con=con)
    
    updater.updateTable(table="GeoNames", id_col="_id", fun=getTransitionGenerator("locations.db", "GeoNames", "streets_intersection"), 
                        monitor=5, con=con)
    updater.updateTable(table="GeoNames", id_col="_id", fun=getTransitionGenerator("locations.db", "GeoNames", "toponymName_places"),
                        monitor=5, con=con)
    updater.updateTable(table="places", id_col="_id", fun=getTransitionGenerator("locations.db", "places", "name_1"),
                        monitor=5, con=con)
    updater.updateTable(table="geocoding", id_col="_id", fun=getTransitionGenerator("locations.db", "geocoding", "street"), 
                        monitor=5, con=con)
    updater.updateTable(table="geocoding", id_col="_id", fun=getTransitionGenerator("locations.db", "geocoding", "establishment"), 
                        monitor=5, con=con)
    updater.updateTable(table="geocoding", id_col="_id", fun=getTransitionGenerator("locations.db", "geocoding", "bus_station"), 
                        monitor=5, con=con)
    
    updater.updateTable(table="GeoNames", id_col="_id", 
                        fun=combineColsGenerator("streets_intersection", "streets_intersection_next", "streets_transition"), 
                        monitor=5, con=con)
    updater.updateTable(table="GeoNames", id_col="_id",
                        fun=combineColsGenerator("toponymName_places", "toponymName_places_next", "toponym_transition"), 
                        monitor=5, con=con)
    updater.updateTable(table="places", id_col="_id",
                        fun=combineColsGenerator("name_1", "name_1_next", "name_1_transition"), 
                        monitor=5, con=con)
    updater.updateTable(table="geocoding", id_col="_id", 
                        fun=combineColsGenerator("street", "street_next", "street_transition"),
                        monitor=5, con=con)
    updater.updateTable(table="geocoding", id_col="_id", 
                        fun=combineColsGenerator("establishment", "establishment_next", "establishment_transition"), 
                        monitor=5, con=con)
    updater.updateTable(table="geocoding", id_col="_id", 
                        fun=combineColsGenerator("bus_station", "bus_station_transition", "bus_station_next"), 
                        monitor=5, con=con)
    
    tables = ["locations_intersections", "locations_areas", "locations_places", "locations_street",
              "locations_establishment", "locations_station"]
    for table in tables:
        updater.createTable(table=table, con=con)

    uid_col, coreTable = "device_id", "locations"
    user_IDs = updater.getDistinctValues(table=coreTable, column=uid_col)
    
    for uid in user_IDs:
        addInformation(table="GeoNames", newTable="locations_intersections", P1_name="streets_intersection", 
                       P2_name="streets_intersection_next", transition_name="streets_transition", uid_col=uid_col, 
                       uid=uid, updater=updater, con=col)
        addInformation(table="GeoNames", newTable="locations_areas", P1_name="toponymName_places", 
                       P2_name="toponymName_places_next", transition_name="toponym_transition", uid_col=uid_col, 
                       uid=uid, updater=updater, con=col)
        addInformation(table="places", newTable="locations_places", P1_name="name_1", 
                       P2_name="name_1_next", transition_name="name_1_transition", uid_col=uid_col, 
                       uid=uid, updater=updater, con=col)
        addInformation(table="geocoding", newTable="locations_street", P1_name="street", 
                       P2_name="street_next", transition_name="street_transition", uid_col=uid_col, 
                       uid=uid, updater=updater, con=col)
        addInformation(table="geocoding", newTable="locations_establishment", P1_name="establishment", 
                       P2_name="establishment_next", transition_name="establishment_transition", uid_col=uid_col, 
                       uid=uid, updater=updater, con=col)
        addInformation(table="geocoding", newTable="locations_station", P1_name="bus_station", 
                       P2_name="bus_station_next", transition_name="bus_station_transition", uid_col=uid_col, 
                       uid=uid, updater=updater, con=col)

        con.close()

update()