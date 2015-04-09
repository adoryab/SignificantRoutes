# updater.py

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
# 10. add named places and intersection data (!!!)

import requests
import time
import sqlite3
import sys

class NoKeysException(Exception):
    pass

class Updater(object):

    def __init__(self, **information):
        self.database = information.get('database', "locations.db")
        self.checkPath()
        self.tables = information.get('tables', ["locations"])
        self.URL = information.get('URL', "https://maps.googleapis.com/maps/api/geocode/json")
        self.keys = information.get('keys', [None, "AIzaSyAuAWiaukaws8JUwaLkiivaCz3P7X5e498", 
                                             "AIzaSyB3ffCdODdeKP8zz1CvO40QRqpDB5UzHiA"])
        self.numKeys = len(self.keys)
        self.existingCols = []
        self.currentKey = self.keys[0]
        defaultTranslator = dict()
        defaultTranslator["administrative_area_level_1"] = "state"
        defaultTranslator["administrative_area_level_2"] = "county"
        defaultTranslator["route"] = "street"
        defaultTranslator["political"] = "city"
        self.translator = information.get('translator', defaultTranslator)
        self.data = None
        self.status = None

    def checkPath(self): # verify that the database exists
        pass

    def getLocationData(self, latitude, longitude):
        parameters = dict()
        parameters['latlng'] = str(latitude) + "," + str(longitude)
        if self.currentKey != None: parameters['key'] = self.currentKey
        r = requests.get(url=self.URL, params=parameters)
        self.data = r.json()
        self.status = self.data['status']
        print "status: ", self.status
        if self.status != "OK": self.updateKeys()

    def addColumns(self, table, **colInfo):
        connection = sqlite3.connect(self.database)
        columns = colInfo.get('columns', [])
        colType = colInfo.get('colType', "TEXT")
        for col in columns:
            cursor = connection.cursor()
            cursor.execute("ALTER TABLE {tn} ADD COLUMN '{cn}' {type}"\
                           .format(tn=table, cn=col, type=colType))
        connection.commit()
        connection.close()

    def updateKeys(self, attempts=1):
        latitude = "29.000" # an arbitrary set of coordinates
        longitude = "-95.000" # that shouldn't return an error
        parameters = dict()
        parameters['latlng'] = str(latitude) + "," + str(longitude)
        workingKeys = []
        for x in xrange(attempts):
            for key in self.keys:
                if key != None: parameters['key'] = key
                r = requests.get(url=self.URL, params=parameters)
                data = r.json()
                if data['status'] == "OK": workingKeys.append(key)
        if len(workingKeys) == 0: raise NoKeysException("All keys are failing!")
        self.currentKey = workingKeys[0]

    def getColumns(self, nameIndex = 1, table=None):
        if table == None: table = self.tables[0]
        con = sqlite3.connect(self.database)
        cursor = con.cursor()
        cursor.execute("PRAGMA table_info({tn})".format(tn=table))
        # column name in first index of results
        self.existingCols = [item[nameIndex] for item in cursor.fetchall()]
        con.close()

    def updateTable(self, **kwargs):
        columns = kwargs.get('columns', None)
        table = kwargs.get('table', self.tables[0])
        id_col = kwargs.get('id_col', "_id")
        minIndex = kwargs.get('minIndex', 0)
        maxIndex = kwargs.get('maxIndex', 2)
        self.getColumns()
        con = sqlite3.connect(self.database)
        cursor = con.cursor()
        cursor.execute("SELECT * FROM {tn} WHERE {idc} >= {min}"\
                       .format(tn=table, idc=id_col, min=minIndex))
        fetch = cursor.fetchone()
        id_num, id_col, type_index = minIndex, kwargs.get('id_col', 0), kwargs.get('type_index', 0)
        lat_col, lon_col = kwargs.get('lat_col', 3), kwargs.get('lon_col', 4)
        while fetch is not None and (maxIndex is None or id_num < maxIndex):
            id_num = fetch[id_col]
            print "current index: ", id_num
            latitude, longitude = fetch[lat_col], fetch[lon_col]
            self.getLocationData(latitude, longitude)
            if self.status != "OK": 
                self.updateTable(columns=columns, table=table, id_col=id_col, minIndex=id_num,
                                 maxIndex=maxIndex)
                break
            print self.data['results']
            for item in self.data['results']:
                address = item['address_components']
                for element in address:
                    name = element['short_name']
                    locType = element['types'][type_index]
                    if locType in self.translator:
                        locType = self.translator[locType]
                    print locType, self.existingCols, locType in self.existingCols
                    if locType not in self.existingCols: 
                        self.addColumns(table=table, colType="TEXT", columns=[locType])
                        print "goes here for ", locType
                    with con:
                        c = con.cursor()
                        c.execute("UPDATE {tn} SET {cn}=('{value}') WHERE {idc} = ({ID})"\
                                  .format(tn=table, cn=locType, value=name, 
                                          idc = id_col, ID = id_num))
            fetch = cursor.fetchone()
        con.close()

    def updateTables(self, **kwargs):
        tables = kwargs.get('tables', self.tables)
        for table in tables:
            self.updateTable(table=table)

if len(sys.argv) == 1:
    update = Updater()

else:
    database = sys.argv[1]
    tables = sys.argv[2:]
    update = Updater(database=database, tables=tables)

update.updateTable()