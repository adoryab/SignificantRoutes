# updater.py

# module to handle/update SQLite database

import sqlite3
import os
from datetime import datetime

# CLASSLESS FUNCTIONS

def pathExists(path):
    """Determines whether a file exists at a certain path"""

    return os.path.exists(path)

# CLASSES

class Updater(object):
    def __init__(self, database):
        """Constructor for Updater class

        PARAMETERS
        database: database to connect to"""

        self.database = database
        if (not pathExists(self.database)): raise IOError("Invalid path")

    def getConnection(self):
        """Produces a connection to the database"""
        con = sqlite3.connect(self.database)
        return con

    def getTables(self, con=None):
        """Lists all tables that are members of a datbase
        
        OPTIONAL
        con: existing sqlite3 connection
        (recommended for situations in which the connection
        needs to be used after this function runs)"""

        conCreated = False
        if con is None:
            con, conCreated = self.getConnection(), True
        cur, tables = con.cursor(), []
        cur.execute("SELECT name from sqlite_master WHERE type='table'")
        if conCreated: con.close() # avoids wasting processing power
        return [item[0] for item in cur.fetchall()]

    def tableExists(self, table):
        """Checks whether a table exists in a database"""

        return (table in self.getTables())

    def getColumns(self, table, error=False, con=None):
        """Lists all columns within a table

        PARAMETERS
        table: table to check

        OPTIONAL
        error: set to True to return IOError if table doesn't exist
        con: provide an existing connection
        (recommended for situations where connection will be
        used after this function call terminates)"""

        if self.tableExists(table):
            conCreated = False
            if con is None:
                con, conCreated = self.getConnection(), True
            cur, columns, nameIndex = con.cursor(), [], 1
            # nameIndex = index with column names
            cursor.execute("PRAGMA table_info({tn})".format(tn=table))
            if conCreated: con.close()
            return [item[nameIndex] for item in cursor.fetchall()]
        elif error:
            raise IOError("Table does not exist")
        else: return []

    def columnExists(self, table, column):
        """Checks whether a column exists in a table"""
        return (column in self.getColumns(table))

    def getIndex(self, table, column, error=True):
        """Returns index of column in a table"""
        if self.columnExists(table, column):
            return self.getColumns().index(column)
        elif error:
            raise IOError("Column does not exist!")

    def createColumn(self, table, column, 
                     colType="TEXT", error=False,
                     con=None):
        """Creates a new column in a table
        PARAMETERS
        table, column

        OPTIONAL: 
        colType: type of value in column (default is TEXT)
        error (if set to True, raises error if column exists)
        con: existing connection (defaults to None)"""
        if (not self.columnExists(table, column)):
            conCreated = False
            if con is None: 
                con, conCreated = self.getConnection(), True
            with con:
                cur = con.cursor()
                cur.execute("ALTER TABLE {tn} ADD COLUMN '{cn}' {ct}"\
                            .format(tn=table, cn=col, ct=colType))
            if conCreated: con.close()
        elif error:
            raise IOError("Column already exists!")

    def createTable(self, table, primaryKey=True, error=False,
                    con=None):
        """Creates a new table in the database
        PARAMETERS
        table: name of table to be created

        OPTIONAL:
        key: whether or not to generate a primary key
        (defaults to True)
        error: if set to True, raises error if table exists
        con: pre-existing sqlite connection"""
        if (not self.tableExists(table)):
            conCreated = False
            if con is None:
                con, conCreated = self.getConnection(), True
            with con:
                cur = con.cursor()
                statement = "CREATE TABLE {tn}".format(tn=table)
                if primaryKey: 
                    statement +=  " (_id integer primary key autoincrement)"
                cur.execute(statement)
            if conCreated: con.close()
        elif error:
            raise IOError("Table already exists!")

    def updateRow(self, data, table, id_col, ID, con=None):
        """Updates a single row in a table 
        based on a dictionary of columns and their values

        PARAMETERS
        data: dictionary of columns with values
        table: table to be modified
        id_col: column through which rows can be identified
        ID: ID of row to be updated

        OPTIONAL
        con: existing sqlite connection"""

        conCreated = False
        if (con is None): 
            con, conCreated = self.getConnection(), True
        for item in data:
            self.createColumn(table, item, con=con)
            with con:
                cur = con.cursor()
                value = str(data[item])
                if "'" in value: value = value.replace("'", "")
                cur.execute("UPDATE {tn} SET {cn} = ('{value}') WHERE {idc} = ({ID})"\
                            .format(tn=table, cn=item, value=value, idc=id_col, ID=ID))
        if conCreated: con.close()

    def rowAsDict(self, rowData, colData):
        result = dict()
        for i in xrange(len(colData)):
            col = colData[i]
            result[col] = item[i]
        return result

    def updateTable(self, table, id_col, fun, minIndex=None, maxIndex=None, con=None):
        """Updates an entire table or a range over the table

        PARAMETERS
        table: table to update
        id_col: id column within table
        fun: function to get a dictionary from a row in the table (to update the row)

        OPTIONAL
        minIndex: minimum index to update (inclusive)
        maxIndex: maximum index to update (inclusive)
        con: pre-existing connection"""

        if (not self.tableExists(table)): raise IOError("Invalid table!")
        conCreated = False
        id_index = self.getIndex(table, id_col)
        if (con is None):
            con, conCreated = self.getConnection(), True
        with con:
            cur = con.cursor()
            if minIndex != None:
                if maxIndex != None:
                    cur.execute("SELECT * FROM {tn} WHERE {idc} >= {min} AND {idc} <= {max}"\
                                   .format(tn=table, idc=id_col, min=minIndex, max=maxIndex))
                else:
                    cur.execute("SELECT * FROM {tn} WHERE {idc} >= {min}"\
                                   .format(tn=table, idc=id_col, min=minIndex))
            else:
                cur.execute("SELECT * FROM {tn}".format(tn=table))
            for item in cur.fetchall():
                itemDict = self.rowAsDict(item, self.getColumns(table))
                data = fun(itemDict)
                ID = item[id_index]
                self.updateRow(data, table, id_col, ID, con=con)
        if conCreated: con.close()

    def getRows(self, table, column, col_value, con=None):
        """Gets rows matching certain criteria

        PARAMETERS
        table: table to check
        column: column to check for values
        col_value: values to check for within column

        OPTIONAL
        con: pre-existing sqlite connection"""

        conCreated = False
        if (con is None):
            con, conCreated = sqlite3.connect(self.database), True
        with con:
            cur = con.cursor()
            cur.execute("SELECT * FROM {tn} WHERE {cn} = {cv}"\
                        .format(tn=table, cn=column, cv=col_value))
        columns = self.getColumns(table)
        if conCreated: con.close()
        return [self.rowAsDict(item, columns) for item in cur.fetchall()]