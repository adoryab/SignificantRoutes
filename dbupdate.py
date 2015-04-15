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

def getStreetsTransition(item):
    return getTransition(item, database="locations.db", table="GeoNames", columnOfInterest="streets_intersection")

def getAreaTransition(item):
    return getTransition(item, database="locations.db", table="GeoNames", columnOfInterest="toponymName_places")

def getPlacesTransition(item):
    return getTransition(item, database="locations.db", table="places", columnOfInterest="name_1")

def getStreetTransition(item):
    return getTransition(item, database="locations.db", table="geocoding", columnOfInterest="street")

def getEstablishmentTransition(item):
    return getTransition(item, database="locations.db", table="geocoding", columnOfInterest="establishment")

def getStationTransition(item):
    return getTransition(item, database="locations.db", table="geocoding", columnOfInterest="bus_station")

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

def combineStreetsCols(item):
    return combineCols(item=item, col1="streets_intersection", col2="streets_intersection_next", newCol="streets_transition")

def combineAreaCols(item):
    return combineCols(item=item, col1="toponymName_places", col2="toponymName_places_next", newCol="toponym_transition")

def combinePlacesCols(item):
    return combineCols(item=item, col1="name_1", col2="name_1_next", newCol="name_1_transition")

def combineStreetCols(item):
    return combineCols(item=item, col1="street", col2="street_next", newCol="street_transition")

def combineEstablishmentCols(item):
    return combineCols(item=item, col1="establishment", col2="establishment_next", newCol="establishment_transition")

def combineStationCols(item):
    return combineCols(item=item, col1="bus_station", col2="bus_station_next", newCol="bus_station_transition")

def update(): # script to run here
    updater = Updater("locations.db")
    updater.updateTable(table="locations", id_col="_id", fun=convertTimestamp, monitor=5)
    updater.updateTable(table="GeoNames", id_col="_id", fun=convertTimestamp, monitor=5)
    updater.updateTable(table="places", id_col="_id", fun=convertTimestamp, monitor=5)
    updater.updateTable(table="geocoding", id_col="_id", fun=convertTimestamp, monitor=5)
    updater.updateTable(table="GeoNames", id_col="_id", fun=getStreetsTransition, monitor=5)
    updater.updateTable(table="GeoNames", id_col="_id", fun=getAreaTransition, monitor=5)
    updater.updateTable(table="places", id_col="_id", fun=getPlacesTransition, monitor=5)
    updater.updateTable(table="geocoding", id_col="_id", fun=getStreetTransition, monitor=5)
    updater.updateTable(table="geocoding", id_col="_id", fun=getEstablishmentTransition, monitor=5)
    updater.updateTable(table="geocoding", id_col="_id", fun=getStationTransition, monitor=5)
    updater.updateTable(table="GeoNames", id_col="_id", fun=combineStreetsCols, monitor=5)
    updater.updateTable(table="GeoNames", id_col="_id", fun=combineAreaCols, monitor=5)
    updater.updateTable(table="places", id_col="_id", fun=combinePlacesCols, monitor=5)
    updater.updateTable(table="geocoding", id_col="_id", fun=combineStreetCols, monitor=5)
    updater.updateTable(table="geocoding", id_col="_id", fun=combineEstablishmentCols, monitor=5)
    updater.updateTable(table="geocoding", id_col="_id", fun=combineStationCols, monitor=5)
    updater.createTable(table="locations_intersections")
    intersections = updater.getDistinctValues(table="GeoNames", column="streets_intersection")
    streets_transitions = updater.getDistinctValues(table="GeoNames", column="streets_transition")
    uid_col = "device_id"
    table = "GeoNames"
    newTable = "locations_intersections"
    P1_name, P2_name = "streets_intersection", "streets_intersection_next"
    transition_name = "streets_transition"
    place_category, transition_category = intersections, streets_transitions
    for item in place_category:
        data = dict()
        data['uid'] = updater.getAssociatedValue(table=table, col1=P1_name,
                                                 value=item, col2=uid_col) # TODO: update to handle multiple UIDs
        data['P1'] = item
        data['P1_label'] = updater.getAssociatedValue(table=table, col1=P1_name,
                                                     value=item, col2="label")
        data['frequency'] = updater.getFrequency(table=table, columns={P1_name:item})
        for i in xrange(0, 24, 2):
            category = str(i) + '-' + str(i+1)
            data[category] = updater.getFrequency(table=table, columns={P1_name:item, 'hour':[i,i+1]})
        updater.addRow(table=newTable, data=data)
    for item in transition_category:
        data = dict()
        data['uid'] = updater.getAssociatedValue(table=table, col1=transition_name,
                                                 value=item, col2=uid_col) # TODO: update to handle multiple UIDs
        data['P1'] = updater.getAssociatedValue(table=table, col1=transition_name,
                                               value=item, col2=P1_name)
        data['P1_label'] = updater.getAssociatedValue(table=table, col1=transition_name,
                                                     value=(data['P1']), col2="label")
        data['P2'] = updater.getAssociatedValue(table=table, col1=transition_name,
                                               value=item, col2=P2_name)
        data['P2_label'] = updater.getAssociatedValue(table=table, col1=transition_name,
                                                     value=(data['P2']), col2="label")
        data['frequency'] = updater.getFrequency(table=table, columns={transition_name:item})
        for i in xrange(0, 24, 2):
            category = str(i) + '-' + str(i+1)
            data[category] = updater.getFrequency(table=table, columns={transition_name:item, 'hour':[i,i+1]})
        updater.addRow(table=newTable, data=data)
    updater.createTable(table="locations_areas")
    areas = updater.getDistinctValues(table="GeoNames", column="toponymName_places")
    areas_transitions = updater.getDistinctValues(table="GeoNames", column="toponym_transition")
    uid_col = "device_id"
    table = "GeoNames"
    newTable = "locations_areas"
    P1_name, P2_name = "toponymName_places", "toponymName_places_next"
    transition_name = "toponym_transition"
    place_category, transition_category = areas, areas_transitions
    for item in place_category:
        data = dict()
        data['uid'] = updater.getAssociatedValue(table=table, col1=P1_name,
                                                 value=item, col2=uid_col) # TODO: update to handle multiple UIDs
        data['P1'] = item
        data['P1_label'] = updater.getAssociatedValue(table=table, col1=P1_name,
                                                     value=item, col2="label")
        data['frequency'] = updater.getFrequency(table=table, columns={P1_name:item})
        for i in xrange(0, 24, 2):
            category = str(i) + '-' + str(i+1)
            data[category] = updater.getFrequency(table=table, columns={P1_name:item, 'hour':[i,i+1]})
        updater.addRow(table=newTable, data=data)
    for item in transition_category:
        data = dict()
        data['uid'] = updater.getAssociatedValue(table=table, col1=transition_name,
                                                 value=item, col2=uid_col) # TODO: update to handle multiple UIDs
        data['P1'] = updater.getAssociatedValue(table=table, col1=transition_name,
                                               value=item, col2=P1_name)
        data['P1_label'] = updater.getAssociatedValue(table=table, col1=transition_name,
                                                     value=(data['P1']), col2="label")
        data['P2'] = updater.getAssociatedValue(table=table, col1=transition_name,
                                               value=item, col2=P2_name)
        data['P2_label'] = updater.getAssociatedValue(table=table, col1=transition_name,
                                                     value=(data['P2']), col2="label")
        data['frequency'] = updater.getFrequency(table=table, columns={transition_name:item})
        for i in xrange(0, 24, 2):
            category = str(i) + '-' + str(i+1)
            data[category] = updater.getFrequency(table=table, columns={transition_name:item, 'hour':[i,i+1]})
        updater.addRow(table=newTable, data=data)
    updater.createTable(table="locations_places")
    places = updater.getDistinctValues(table="places", column="name_1")
    places_transitions = updater.getDistinctValues(table="places", column="name_1_transition")
    uid_col = "device_id"
    table = "places"
    newTable = "locations_places"
    P1_name, P2_name = "name_1", "name_1_next"
    transition_name = "name_1_transition"
    place_category, transition_category = places, places_transitions
    for item in place_category:
        data = dict()
        data['uid'] = updater.getAssociatedValue(table=table, col1=P1_name,
                                                 value=item, col2=uid_col) # TODO: update to handle multiple UIDs
        data['P1'] = item
        data['P1_label'] = updater.getAssociatedValue(table=table, col1=P1_name,
                                                     value=item, col2="label")
        data['frequency'] = updater.getFrequency(table=table, columns={P1_name:item})
        for i in xrange(0, 24, 2):
            category = str(i) + '-' + str(i+1)
            data[category] = updater.getFrequency(table=table, columns={P1_name:item, 'hour':[i,i+1]})
        updater.addRow(table=newTable, data=data)
    for item in transition_category:
        data = dict()
        data['uid'] = updater.getAssociatedValue(table=table, col1=transition_name,
                                                 value=item, col2=uid_col) # TODO: update to handle multiple UIDs
        data['P1'] = updater.getAssociatedValue(table=table, col1=transition_name,
                                               value=item, col2=P1_name)
        data['P1_label'] = updater.getAssociatedValue(table=table, col1=transition_name,
                                                     value=(data['P1']), col2="label")
        data['P2'] = updater.getAssociatedValue(table=table, col1=transition_name,
                                               value=item, col2=P2_name)
        data['P2_label'] = updater.getAssociatedValue(table=table, col1=transition_name,
                                                     value=(data['P2']), col2="label")
        data['frequency'] = updater.getFrequency(table=table, columns={transition_name:item})
        for i in xrange(0, 24, 2):
            category = str(i) + '-' + str(i+1)
            data[category] = updater.getFrequency(table=table, columns={transition_name:item, 'hour':[i,i+1]})
        updater.addRow(table=newTable, data=data)
    updater.createTable(table="locations_street")
    streets = updater.getDistinctValues(table="geocoding", column="street")
    street_transitions = updater.getDistinctValues(table="geocoding", column="street_transition")
    uid_col = "device_id"
    table = "geocoding"
    newTable = "locations_street"
    P1_name, P2_name = "street", "street_next"
    transition_name = "street_transition"
    place_category, transition_category = streets, street_transitions
    for item in place_category:
        data = dict()
        data['uid'] = updater.getAssociatedValue(table=table, col1=P1_name,
                                                 value=item, col2=uid_col) # TODO: update to handle multiple UIDs
        data['P1'] = item
        data['P1_label'] = updater.getAssociatedValue(table=table, col1=P1_name,
                                                     value=item, col2="label")
        data['frequency'] = updater.getFrequency(table=table, columns={P1_name:item})
        for i in xrange(0, 24, 2):
            category = str(i) + '-' + str(i+1)
            data[category] = updater.getFrequency(table=table, columns={P1_name:item, 'hour':[i,i+1]})
        updater.addRow(table=newTable, data=data)
    for item in transition_category:
        data = dict()
        data['uid'] = updater.getAssociatedValue(table=table, col1=transition_name,
                                                 value=item, col2=uid_col) # TODO: update to handle multiple UIDs
        data['P1'] = updater.getAssociatedValue(table=table, col1=transition_name,
                                               value=item, col2=P1_name)
        data['P1_label'] = updater.getAssociatedValue(table=table, col1=transition_name,
                                                     value=(data['P1']), col2="label")
        data['P2'] = updater.getAssociatedValue(table=table, col1=transition_name,
                                               value=item, col2=P2_name)
        data['P2_label'] = updater.getAssociatedValue(table=table, col1=transition_name,
                                                     value=(data['P2']), col2="label")
        data['frequency'] = updater.getFrequency(table=table, columns={transition_name:item})
        for i in xrange(0, 24, 2):
            category = str(i) + '-' + str(i+1)
            data[category] = updater.getFrequency(table=table, columns={transition_name:item, 'hour':[i,i+1]})
        updater.addRow(table=newTable, data=data)
    updater.createTable(table="locations_establishment")
    establishments = updater.getDistinctValues(table="geocoding", column="establishment")
    establishment_transitions = updater.getDistinctValues(table="geocoding", column="establishment_transition")
    uid_col = "device_id"
    table = "geocoding"
    newTable = "locations_establishment"
    P1_name, P2_name = "establishment", "establishment_next"
    transition_name = "establishment_transition"
    place_category, transition_category = establishments, establishment_transitions
    for item in place_category:
        data = dict()
        data['uid'] = updater.getAssociatedValue(table=table, col1=P1_name,
                                                 value=item, col2=uid_col) # TODO: update to handle multiple UIDs
        data['P1'] = item
        data['P1_label'] = updater.getAssociatedValue(table=table, col1=P1_name,
                                                     value=item, col2="label")
        data['frequency'] = updater.getFrequency(table=table, columns={P1_name:item})
        for i in xrange(0, 24, 2):
            category = str(i) + '-' + str(i+1)
            data[category] = updater.getFrequency(table=table, columns={P1_name:item, 'hour':[i,i+1]})
        updater.addRow(table=newTable, data=data)
    for item in transition_category:
        data = dict()
        data['uid'] = updater.getAssociatedValue(table=table, col1=transition_name,
                                                 value=item, col2=uid_col) # TODO: update to handle multiple UIDs
        data['P1'] = updater.getAssociatedValue(table=table, col1=transition_name,
                                               value=item, col2=P1_name)
        data['P1_label'] = updater.getAssociatedValue(table=table, col1=transition_name,
                                                     value=(data['P1']), col2="label")
        data['P2'] = updater.getAssociatedValue(table=table, col1=transition_name,
                                               value=item, col2=P2_name)
        data['P2_label'] = updater.getAssociatedValue(table=table, col1=transition_name,
                                                     value=(data['P2']), col2="label")
        data['frequency'] = updater.getFrequency(table=table, columns={transition_name:item})
        for i in xrange(0, 24, 2):
            category = str(i) + '-' + str(i+1)
            data[category] = updater.getFrequency(table=table, columns={transition_name:item, 'hour':[i,i+1]})
        updater.addRow(table=newTable, data=data)
    updater.createTable(table="locations_station")
    stations = updater.getDistinctValues(table="geocoding", column="bus_station")
    station_transitions = updater.getDistinctValues(table="geocoding", column="bus_station_transition")
    uid_col = "device_id"
    table = "geocoding"
    newTable = "locations_station"
    P1_name, P2_name = "bus_station", "bus_station_next"
    transition_name = "bus_station_transition"
    place_category, transition_category = stations, station_transitions
    for item in place_category:
        data = dict()
        data['uid'] = updater.getAssociatedValue(table=table, col1=P1_name,
                                                 value=item, col2=uid_col) # TODO: update to handle multiple UIDs
        data['P1'] = item
        data['P1_label'] = updater.getAssociatedValue(table=table, col1=P1_name,
                                                     value=item, col2="label")
        data['frequency'] = updater.getFrequency(table=table, columns={P1_name:item})
        for i in xrange(0, 24, 2):
            category = str(i) + '-' + str(i+1)
            data[category] = updater.getFrequency(table=table, columns={P1_name:item, 'hour':[i,i+1]})
        updater.addRow(table=newTable, data=data)
    for item in transition_category:
        data = dict()
        data['uid'] = updater.getAssociatedValue(table=table, col1=transition_name,
                                                 value=item, col2=uid_col) # TODO: update to handle multiple UIDs
        data['P1'] = updater.getAssociatedValue(table=table, col1=transition_name,
                                               value=item, col2=P1_name)
        data['P1_label'] = updater.getAssociatedValue(table=table, col1=transition_name,
                                                     value=(data['P1']), col2="label")
        data['P2'] = updater.getAssociatedValue(table=table, col1=transition_name,
                                               value=item, col2=P2_name)
        data['P2_label'] = updater.getAssociatedValue(table=table, col1=transition_name,
                                                     value=(data['P2']), col2="label")
        data['frequency'] = updater.getFrequency(table=table, columns={transition_name:item})
        for i in xrange(0, 24, 2):
            category = str(i) + '-' + str(i+1)
            data[category] = updater.getFrequency(table=table, columns={transition_name:item, 'hour':[i,i+1]})
        updater.addRow(table=newTable, data=data)

update()