package com.aware.plugin.data_collection;

import java.util.HashMap;

import android.content.ContentProvider;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.UriMatcher;
import android.database.Cursor;
import android.database.SQLException;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.Environment;
import android.provider.BaseColumns;
import android.util.Log;

import com.aware.Aware;
import com.aware.utils.DatabaseHelper;

/**
 * ContentProvider for the DataCollection
 */

public class DataCollection_Provider extends ContentProvider {

        public static final String AUTHORITY = "com.aware.provider.plugin.data_collection";
        
        private static final int DATABASE_VERSION = 1;
        
        private static final int DATA_COLLECTION = 1;
        private static final int DATA_COLLECTION_ID = 2;
        
        private static UriMatcher uriMatcher = null;
        private static HashMap<String, String> dataCollectionMap = null;        
        private static DatabaseHelper databaseHelper = null;
        private static SQLiteDatabase database = null;        
        
        public static final class DataCollection implements BaseColumns {
        	private DataCollection() {};
        	
        	//this needs to match the table name
            public static final Uri CONTENT_URI = Uri.parse("content://"+AUTHORITY+"/plugin_data_collection"); 
            public static final String CONTENT_TYPE = "vnd.android.cursor.dir/vnd.aware.plugin.data_collection";
            public static final String CONTENT_ITEM_TYPE = "vnd.android.cursor.item/vnd.aware.plugin.data_collection";
            
            public static final String _ID = "_id";
            public static final String TIMESTAMP = "timestamp";
            public static final String DEVICE_ID = "device_id";
            
            public static final String ACTIVITY_TYPE = "activity_type";
            public static final String BATTERY_STATUS = "battery_status";
            public static final String SCREEN_STATUS = "screen_status";
            public static final String BAROMETER_STATUS = "barometer_status";
            public static final String TEMPERATURE_STATUS = "temperature_status";
            public static final String LOCATION = "location";
            public static final String TOWER = "celltower_id";
        }
        
        public static String DATABASE_NAME = Environment.getExternalStorageDirectory() + "/AWARE/plugin_data_collection.db";
        
        public static final String[] DATABASE_TABLES = {
                "plugin_data_collection"                
        };
        
        public static final String[] TABLES_FIELDS = {
        		DataCollection._ID + " integer primary key autoincrement," +
        		DataCollection.TIMESTAMP + " real default 0," + 
        		DataCollection.DEVICE_ID + " text default ''," +
        		DataCollection.ACTIVITY_TYPE + " text default ''," +
        		DataCollection.BATTERY_STATUS + " int default -1," +
        		DataCollection.SCREEN_STATUS + " int default -1," +
        		DataCollection.BAROMETER_STATUS + " int default -1," +
        		DataCollection.TEMPERATURE_STATUS + " int default 100," +
        		DataCollection.LOCATION + " text default ''," +
        		DataCollection.TOWER + " text default '-1'," +
                "UNIQUE ("+DataCollection.TIMESTAMP+","+DataCollection.DEVICE_ID+")"
        };
        
        static {
            uriMatcher = new UriMatcher(UriMatcher.NO_MATCH);
            uriMatcher.addURI(AUTHORITY, DATABASE_TABLES[0], DATA_COLLECTION);
            uriMatcher.addURI(AUTHORITY, DATABASE_TABLES[0]+"/#", DATA_COLLECTION_ID);
            
            dataCollectionMap = new HashMap<String, String>();
            dataCollectionMap.put(DataCollection._ID, DataCollection._ID);
            dataCollectionMap.put(DataCollection.TIMESTAMP, DataCollection.TIMESTAMP);
            dataCollectionMap.put(DataCollection.DEVICE_ID, DataCollection.DEVICE_ID);
            dataCollectionMap.put(DataCollection.ACTIVITY_TYPE, DataCollection.ACTIVITY_TYPE);
            dataCollectionMap.put(DataCollection.BATTERY_STATUS, DataCollection.BATTERY_STATUS);
            dataCollectionMap.put(DataCollection.SCREEN_STATUS, DataCollection.SCREEN_STATUS);
            dataCollectionMap.put(DataCollection.TEMPERATURE_STATUS, DataCollection.TEMPERATURE_STATUS);
            dataCollectionMap.put(DataCollection.BAROMETER_STATUS, DataCollection.BAROMETER_STATUS);
            dataCollectionMap.put(DataCollection.LOCATION, DataCollection.LOCATION);
            dataCollectionMap.put(DataCollection.TOWER, DataCollection.TOWER);
        }
        
        @Override
        public int delete(Uri uri, String selection, String[] selectionArgs) {
            if( database == null || ! database.isOpen()) database = databaseHelper.getWritableDatabase();
        
	        int count = 0;
	        switch (uriMatcher.match(uri)) {
	            case DATA_COLLECTION:
	                count = database.delete(DATABASE_TABLES[0], selection, selectionArgs);
	                break;            
	            default:
	                throw new IllegalArgumentException("Unknown URI " + uri);
	        }
	        getContext().getContentResolver().notifyChange(uri, null);
	        return count;
        }

        @Override
        public String getType(Uri uri) {
                switch (uriMatcher.match(uri)) {
                case DATA_COLLECTION:
                    return DataCollection.CONTENT_TYPE;
                case DATA_COLLECTION_ID:
                    return DataCollection.CONTENT_ITEM_TYPE;                
                default:
                    throw new IllegalArgumentException("Unknown URI " + uri);
            }
        }

        @Override
        public Uri insert(Uri uri, ContentValues initialValues) {
            if( database == null || ! database.isOpen()) database = databaseHelper.getWritableDatabase();
        
	        ContentValues values = (initialValues != null) ? new ContentValues(initialValues) : new ContentValues();
	        
	        switch(uriMatcher.match(uri)) {
	            case DATA_COLLECTION:
	                long _id = database.insert(DATABASE_TABLES[0], DataCollection.TIMESTAMP, values);
	                if (_id > 0) {
	                    Uri dataUri = ContentUris.withAppendedId(DataCollection.CONTENT_URI, _id);
	                    getContext().getContentResolver().notifyChange(dataUri, null);
	                    return dataUri;
	                }
	                throw new SQLException("Failed to insert row into " + uri);            
	            default:
	                throw new IllegalArgumentException("Unknown URI " + uri);
	        }
        }

        @Override
        public boolean onCreate() {
            if( databaseHelper == null ) 
            	databaseHelper = new DatabaseHelper(getContext(), DATABASE_NAME, null, DATABASE_VERSION, DATABASE_TABLES, TABLES_FIELDS);
	        database = databaseHelper.getWritableDatabase();
	        return (databaseHelper != null);
        }

        @Override
        public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {
                
	        if( database == null || ! database.isOpen()) database = databaseHelper.getWritableDatabase();
	        
	        SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
	        switch (uriMatcher.match(uri)) {
	            case DATA_COLLECTION:
	                qb.setTables(DATABASE_TABLES[0]);
	                qb.setProjectionMap(dataCollectionMap);
	                break;            
	            default:
	                throw new IllegalArgumentException("Unknown URI " + uri);
	        }
	        try {
	            Cursor c = qb.query(database, projection, selection, selectionArgs, null, null, sortOrder);
	            c.setNotificationUri(getContext().getContentResolver(), uri);
	            return c;
	        }catch ( IllegalStateException e ) {
	            if ( Aware.DEBUG ) Log.e(Aware.TAG,e.getMessage());
	            return null;
	        }
        }

        @Override
        public int update(Uri uri, ContentValues values, String selection,
                        String[] selectionArgs) {
                
	                if( database == null || ! database.isOpen()) database = databaseHelper.getWritableDatabase();
	        
	                int count = 0;
	        switch (uriMatcher.match(uri)) {
	            case DATA_COLLECTION:
	                count = database.update(DATABASE_TABLES[0], values, selection, selectionArgs);
	                break;            
	            default:
	                database.close();
	                throw new IllegalArgumentException("Unknown URI " + uri);
	        }
	        getContext().getContentResolver().notifyChange(uri, null);
	        return count;
        }
}