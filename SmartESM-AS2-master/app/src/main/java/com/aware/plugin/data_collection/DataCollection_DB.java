package com.aware.plugin.data_collection;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.content.pm.FeatureInfo;
import android.content.pm.PackageManager;
import android.os.Environment;
import android.util.Log;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * Database for the DataCollection
 */

public class DataCollection_DB extends SQLiteOpenHelper {

	private String TAG = "DataCollection::DB Activity";
	private static final int DATABASE_VERSION = 1;
    //private static final String DATABASE_NAME = "DataCollection_DB";
    private static final String DATABASE_NAME = Environment.getExternalStorageDirectory() + "/AWARE/plugin_data_collection";
    
    // three tables of the database
    private static final String TABLE_ANSWERS = "answers";
    private static final String TABLE_APPS = "apps";
    private static final String TABLE_CONTEXT_DATA = "context_data";
 
    // auto columns that are needed by all tables
    private static final String KEY_ID = "id";
    private static final String KEY_TSys = "system_time";
    private static final String KEY_TS = "timestamp";
    
    private static final String KEY_CELL_ID = "cell_id";
    private static final String KEY_LABEL_ID = "label_id";
 
    // columns of answers table
    private static final String KEY_LABEL = "label";
    private static final String KEY_ANS = "answer";
    private static final String KEY_TYPE = "trigg";
 
    // column of apps table
    private static final String KEY_APP_NAME = "app_name";
 
    // columns of context data - many
    private static final String KEY_WIFI = "wifi_strength";
    private static final String KEY_WIFI_NAME = "wifi_name";
    private static final String KEY_ACTIVITY = "activity";
    private static final String KEY_BATTERY = "battery_status";
    private static final String KEY_BAROMETER = "barometer";
    private static final String KEY_TEMPERATURE = "temperature";
    private static final String KEY_LOCATION = "location";
    private static final String KEY_ACCURACY = "accuracy";
    private static final String KEY_TOWER = "celltower_id";
 
    // Default table create Statements
    private static final String CREATE_TABLE_ANSWERS = "CREATE TABLE "
            + TABLE_ANSWERS + "(" + KEY_ID + " INTEGER PRIMARY KEY," 
            + KEY_CELL_ID + " TEXT," + KEY_LABEL_ID + " INTEGER," 
            + KEY_TYPE + " TEXT," + KEY_LABEL + " TEXT," + KEY_ANS 
            + " TEXT," + KEY_TSys + " DOUBLE," + KEY_TS + " DATETIME" + ")";
 
    private static final String CREATE_TABLE_APPS = "CREATE TABLE " 
    		+ TABLE_APPS + "(" + KEY_ID + " INTEGER PRIMARY KEY," 
    		+ KEY_CELL_ID + " TEXT," + KEY_LABEL_ID + " INTEGER," 
    		+ KEY_APP_NAME + " TEXT," + KEY_TSys + " DOUBLE," 
    		+ KEY_TS + " DATETIME" + ")";
 
    private static final String CREATE_TABLE_CONTEXT = "CREATE TABLE "
            + TABLE_CONTEXT_DATA + "(" + KEY_ID + " INTEGER PRIMARY KEY,"
            + KEY_CELL_ID + " TEXT," + KEY_LABEL_ID + " INTEGER," 
            + KEY_WIFI + " INTEGER," + KEY_WIFI_NAME + " TEXT," 
            + KEY_ACTIVITY + " TEXT," + KEY_BATTERY + " INTEGER," 
            + KEY_BAROMETER + " INTEGER," + KEY_TEMPERATURE + " INTEGER," 
            + KEY_LOCATION + " TEXT," + KEY_ACCURACY + " TEXT," 
            + KEY_TOWER + " TEXT," + KEY_TSys + " DOUBLE," 
            + KEY_TS + " DATETIME" + ")";

	public DataCollection_DB(Context context) {
		super(context, DATABASE_NAME, null, DATABASE_VERSION);
	}
	
	// before creating the tables, check for availability of sensors
	public final static boolean checkSensor(Context context, String sensor) {
        
		final PackageManager packageManager = context.getPackageManager();
        final FeatureInfo[] featuresList = packageManager.getSystemAvailableFeatures();
        
        // iterates through the list of features on given phone
        for (FeatureInfo feature : featuresList) {
            if (feature.name != null && feature.name.equals(sensor)) {
                 return true;
            }
        }
       return false;
    }

	@Override
	public void onCreate(SQLiteDatabase db) {
		// app currently needs three tables for holding information
		Log.v(TAG, "creating the tables");
        db.execSQL(CREATE_TABLE_ANSWERS);
        db.execSQL(CREATE_TABLE_APPS);
        db.execSQL(CREATE_TABLE_CONTEXT);
	}

	@Override
	public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
		Log.v(TAG, "upgrading the tables");
		db.execSQL("DROP TABLE IF EXISTS " + TABLE_ANSWERS);
        db.execSQL("DROP TABLE IF EXISTS " + TABLE_APPS);
        db.execSQL("DROP TABLE IF EXISTS " + TABLE_CONTEXT_DATA);
 
        onCreate(db);		
	}
	
	// individual methods for each of the tables
	
	// Context Data table
	public long createCDRecord(ContextData entry) {
	    SQLiteDatabase db = this.getWritableDatabase();
	 
	    ContentValues values = new ContentValues();
	    
	    values.put(KEY_CELL_ID, entry.getCellId());
	    values.put(KEY_LABEL_ID, entry.getLabelId());
	    values.put(KEY_WIFI, entry.getWifiStrength());
	    values.put(KEY_WIFI_NAME, entry.getWifiName());
	    values.put(KEY_ACTIVITY, entry.getActivity());
	    values.put(KEY_BATTERY, entry.getBatteryStatus());
	    values.put(KEY_BAROMETER, entry.getBarometer());
	    values.put(KEY_TEMPERATURE, entry.getTemperature());
	    values.put(KEY_LOCATION, entry.getLocation());
	    values.put(KEY_ACCURACY, entry.getAccuracy());
	    values.put(KEY_TOWER, entry.getTower());
	    values.put(KEY_TSys, System.currentTimeMillis());
	    values.put(KEY_TS, getDateTime());
	 
	    // row is inserted
	    long entryId = db.insert(TABLE_CONTEXT_DATA, null, values);
	    return entryId;
	}
	
	public ContextData getContextData (long entryId) {
        SQLiteDatabase db = this.getReadableDatabase();
 
        String selectQuery = "SELECT  * FROM " + TABLE_CONTEXT_DATA + " WHERE "
                + KEY_ID + " = " + entryId;
 
        Log.e(TAG, selectQuery);
 
        Cursor c = db.rawQuery(selectQuery, null);
 
        if (c != null)
            c.moveToFirst();
 
        ContextData cd = new ContextData();
        
        cd.setRowId((c.getInt(c.getColumnIndex(KEY_ID))));
        cd.setCellId(c.getString(c.getColumnIndex(KEY_CELL_ID)));
        cd.setLabelId(c.getInt(c.getColumnIndex(KEY_LABEL_ID)));
        cd.setWifiStrength(c.getInt(c.getColumnIndex(KEY_WIFI)));
        cd.setWifiName(c.getString(c.getColumnIndex(KEY_WIFI_NAME)));
        cd.setBatteryStatus(c.getInt(c.getColumnIndex(KEY_BATTERY)));
        cd.setBarometer(c.getInt(c.getColumnIndex(KEY_BAROMETER)));
        cd.setTemperature(c.getInt(c.getColumnIndex(KEY_TEMPERATURE)));
        cd.setLocation(c.getString(c.getColumnIndex(KEY_LOCATION)));
        cd.setAccuracy(c.getString(c.getColumnIndex(KEY_ACCURACY)));
        cd.setTower(c.getString(c.getColumnIndex(KEY_TOWER)));
        //System.out.println (c.getString(c.getColumnIndex(KEY_TS)));
 
        return cd;
    }
	
	public ArrayList<HashMap<String, String>> getContext(long time){
		SQLiteDatabase db = this.getReadableDatabase();
		ArrayList<HashMap<String, String>> res;
		res = new ArrayList<HashMap<String, String>>();
		 
        String selectQuery = "SELECT * FROM " + TABLE_CONTEXT_DATA + " WHERE "
                + KEY_TSys + " > " + time;
        
        Log.e(TAG, selectQuery);
        
        Cursor c = db.rawQuery(selectQuery, null);
 
        if (c != null)
            c.moveToFirst();

        Log.i(TAG,"context count: "+c.getCount());
        
        for(int i=0;i<c.getCount();i++){
        	HashMap<String, String> map = new HashMap<String, String>();
        	map.put(KEY_CELL_ID,c.getString(c.getColumnIndex(KEY_CELL_ID)));
        	map.put(KEY_LABEL_ID,Integer.toString(c.getInt(c.getColumnIndex(KEY_LABEL_ID))));
        	map.put(KEY_WIFI,c.getString(c.getColumnIndex(KEY_WIFI)));
        	map.put(KEY_WIFI_NAME,c.getString(c.getColumnIndex(KEY_WIFI_NAME)));
        	map.put(KEY_ACTIVITY,c.getString(c.getColumnIndex(KEY_ACTIVITY)));
        	map.put(KEY_BATTERY,c.getString(c.getColumnIndex(KEY_BATTERY)));
        	map.put(KEY_BAROMETER,c.getString(c.getColumnIndex(KEY_BAROMETER)));
        	map.put(KEY_TEMPERATURE,c.getString(c.getColumnIndex(KEY_TEMPERATURE)));
        	map.put(KEY_LOCATION,c.getString(c.getColumnIndex(KEY_LOCATION)));
        	map.put(KEY_ACCURACY,c.getString(c.getColumnIndex(KEY_ACCURACY)));
        	map.put(KEY_TOWER,c.getString(c.getColumnIndex(KEY_TOWER)));
        	map.put(KEY_TS,c.getString(c.getColumnIndex(KEY_TS)));
        	map.put(KEY_TSys,c.getString(c.getColumnIndex(KEY_TSys)));
        	res.add(map);
        	c.moveToNext();
        }
        
        db.close();
        return res;
	}
	
	public String composeJSONfromSQLiteContext(long time){
		SQLiteDatabase db = this.getReadableDatabase();
		ArrayList<HashMap<String, String>> res;
		res = new ArrayList<HashMap<String, String>>();
		 
        String selectQuery = "SELECT * FROM " + TABLE_CONTEXT_DATA + " WHERE "
                + KEY_TSys + " > " + time;
        
        Log.e(TAG, selectQuery);
        
        Cursor c = db.rawQuery(selectQuery, null);
 
        if (c != null)
            c.moveToFirst();

        Log.i(TAG,"context count: "+c.getCount());
        
        for(int i=0;i<c.getCount();i++){
        	HashMap<String, String> map = new HashMap<String, String>();
        	map.put(KEY_CELL_ID,c.getString(c.getColumnIndex(KEY_CELL_ID)));
        	map.put(KEY_LABEL_ID,Integer.toString(c.getInt(c.getColumnIndex(KEY_LABEL_ID))));
        	map.put(KEY_WIFI,c.getString(c.getColumnIndex(KEY_WIFI)));
        	map.put(KEY_WIFI_NAME,c.getString(c.getColumnIndex(KEY_WIFI_NAME)));
        	map.put(KEY_ACTIVITY,c.getString(c.getColumnIndex(KEY_ACTIVITY)));
        	map.put(KEY_BATTERY,c.getString(c.getColumnIndex(KEY_BATTERY)));
        	map.put(KEY_BAROMETER,c.getString(c.getColumnIndex(KEY_BAROMETER)));
        	map.put(KEY_TEMPERATURE,c.getString(c.getColumnIndex(KEY_TEMPERATURE)));
        	map.put(KEY_LOCATION,c.getString(c.getColumnIndex(KEY_LOCATION)));
        	map.put(KEY_ACCURACY,c.getString(c.getColumnIndex(KEY_ACCURACY)));
        	map.put(KEY_TOWER,c.getString(c.getColumnIndex(KEY_TOWER)));
        	map.put(KEY_TS,c.getString(c.getColumnIndex(KEY_TS)));
        	map.put(KEY_TSys,Double.toString(c.getDouble(c.getColumnIndex(KEY_TSys))));
        	res.add(map);
        	c.moveToNext();
        }
        
        db.close();
        Gson gson = new GsonBuilder().create();
      //Use GSON to serialize Array List to JSON
        return gson.toJson(res);
	}
	
    public int getCDCount() {        
        SQLiteDatabase db = this.getReadableDatabase();
        
        String countQuery = "SELECT  * FROM " + TABLE_CONTEXT_DATA;
        Cursor cursor = db.rawQuery(countQuery, null);
 
        int count = cursor.getCount();
        cursor.close();
 
        return count;
    }
 
    public int updateCDRecord(ContextData entry) {
    	
        SQLiteDatabase db = this.getWritableDatabase();
        ContentValues values = new ContentValues();
        
	    values.put(KEY_CELL_ID, entry.getCellId());
	    values.put(KEY_LABEL_ID, entry.getLabelId());
	    values.put(KEY_WIFI, entry.getWifiStrength());
	    values.put(KEY_WIFI_NAME, entry.getWifiName());
	    values.put(KEY_ACTIVITY, entry.getActivity());
	    values.put(KEY_BATTERY, entry.getBatteryStatus());
	    values.put(KEY_BAROMETER, entry.getBarometer());
	    values.put(KEY_TEMPERATURE, entry.getTemperature());
	    values.put(KEY_LOCATION, entry.getLocation());
	    values.put(KEY_ACCURACY, entry.getAccuracy());
	    values.put(KEY_TOWER, entry.getTower());
	    values.put(KEY_TSys, System.currentTimeMillis());
	    values.put(KEY_TS, getDateTime());
 
        return db.update(TABLE_CONTEXT_DATA, values, KEY_ID + " = ?",
                new String[] { String.valueOf(entry.getRowId()) });
    }
 
    public void deleteCDRecord(long id) {
        SQLiteDatabase db = this.getWritableDatabase();
        
        db.delete(TABLE_CONTEXT_DATA, KEY_ID + " = ?",
                new String[] { String.valueOf(id) });
    }
    
    // Answers - Labels table
	public long createLabelsRecord(Labels entry) {
	    SQLiteDatabase db = this.getWritableDatabase();
	 
	    ContentValues values = new ContentValues();
	    
	    values.put(KEY_CELL_ID, entry.getCellId());
	    values.put(KEY_LABEL_ID, entry.getLabelId());
	    values.put(KEY_TYPE, entry.getTrigger());
	    values.put(KEY_LABEL, entry.getLabel());
	    values.put(KEY_ANS, entry.getAnswer());
	    values.put(KEY_TSys, System.currentTimeMillis());
	    values.put(KEY_TS, getDateTime());
	 
	    // row is inserted
	    long entryId = db.insert(TABLE_ANSWERS, null, values);	    
	    return entryId;
	}
	
	public ArrayList<HashMap<String, String>> getAnswers(long time){
		SQLiteDatabase db = this.getReadableDatabase();
		ArrayList<HashMap<String, String>> answers;
		answers = new ArrayList<HashMap<String, String>>();
		 
        String selectQuery = "SELECT * FROM " + TABLE_ANSWERS + " WHERE "
                + KEY_TSys + " > " + time;
        
        Log.e(TAG, selectQuery);
        
        Cursor c = db.rawQuery(selectQuery, null);
 
        if (c != null)
            c.moveToFirst();

        Log.i(TAG,"count: "+c.getCount());
        
        for(int i=0;i<c.getCount();i++){
        	HashMap<String, String> map = new HashMap<String, String>();
        	map.put(KEY_CELL_ID,c.getString(c.getColumnIndex(KEY_CELL_ID)));
        	map.put(KEY_LABEL_ID,Integer.toString(c.getInt(c.getColumnIndex(KEY_LABEL_ID))));
        	map.put(KEY_TYPE,c.getString(c.getColumnIndex(KEY_TYPE)));
        	map.put(KEY_LABEL,c.getString(c.getColumnIndex(KEY_LABEL)));
        	map.put(KEY_ANS,c.getString(c.getColumnIndex(KEY_ANS)));
        	map.put(KEY_TS,c.getString(c.getColumnIndex(KEY_TS)));
        	map.put(KEY_TSys,c.getString(c.getColumnIndex(KEY_TSys)));
        	answers.add(map);
        	c.moveToNext();
        }
        
        db.close();
        return answers;
	}
	
	public String composeJSONfromSQLite(long time){
		SQLiteDatabase db = this.getReadableDatabase();
		ArrayList<HashMap<String, String>> answers;
		answers = new ArrayList<HashMap<String, String>>();
		 
        String selectQuery = "SELECT * FROM " + TABLE_ANSWERS + " WHERE "
                + KEY_TSys + " > " + time;
        
        Log.e(TAG, selectQuery);
        
        Cursor c = db.rawQuery(selectQuery, null);
 
        if (c != null)
            c.moveToFirst();

        Log.i(TAG,"count: "+c.getCount());
        
        for(int i=0;i<c.getCount();i++){
        	HashMap<String, String> map = new HashMap<String, String>();
        	map.put(KEY_CELL_ID,c.getString(c.getColumnIndex(KEY_CELL_ID)));
        	map.put(KEY_LABEL_ID,Integer.toString(c.getInt(c.getColumnIndex(KEY_LABEL_ID))));
        	map.put(KEY_TYPE,c.getString(c.getColumnIndex(KEY_TYPE)));
        	map.put(KEY_LABEL,c.getString(c.getColumnIndex(KEY_LABEL)));
        	map.put(KEY_ANS,c.getString(c.getColumnIndex(KEY_ANS)));
        	map.put(KEY_TS,c.getString(c.getColumnIndex(KEY_TS)));
        	map.put(KEY_TSys,Double.toString(c.getDouble(c.getColumnIndex(KEY_TSys))));
        	answers.add(map);
        	c.moveToNext();
        }
        
        db.close();
        Gson gson = new GsonBuilder().create();
      //Use GSON to serialize Array List to JSON
        return gson.toJson(answers);
	}
	
	public Labels getLabelsData (long entryId) {
        SQLiteDatabase db = this.getReadableDatabase();
 
        String selectQuery = "SELECT  * FROM " + TABLE_ANSWERS + " WHERE "
                + KEY_ID + " = " + entryId;
 
        Log.e(TAG, selectQuery);
 
        Cursor c = db.rawQuery(selectQuery, null);
 
        if (c != null)
            c.moveToFirst();
 
        Labels label = new Labels();
        
        label.setCellId((c.getString(c.getColumnIndex(KEY_CELL_ID))));
        label.setLabelId((c.getInt(c.getColumnIndex(KEY_LABEL_ID))));
        label.setRowId((c.getInt(c.getColumnIndex(KEY_ID))));
        label.setTrigger(c.getString(c.getColumnIndex(KEY_TYPE)));
        label.setLabel(c.getString(c.getColumnIndex(KEY_LABEL)));
        label.setAnswer(c.getString(c.getColumnIndex(KEY_ANS)));
        
        //System.out.println (c.getString(c.getColumnIndex(KEY_TS)));
 
        return label;
    }
	
    public int getLabelsCount() {        
        SQLiteDatabase db = this.getReadableDatabase();
        
        String countQuery = "SELECT  * FROM " + TABLE_ANSWERS;
        Cursor cursor = db.rawQuery(countQuery, null);
 
        int count = cursor.getCount();
        cursor.close();
 
        return count;
    }

    public void deleteLabelsRecord(long id) {
        SQLiteDatabase db = this.getWritableDatabase();
        
        db.delete(TABLE_ANSWERS, KEY_ID + " = ?",
                new String[] { String.valueOf(id) });
    }
    
    // Application info table
	public long createAppLogRecord(String appName, String cellId, int labelId) {
	    SQLiteDatabase db = this.getWritableDatabase();
	 
	    ContentValues values = new ContentValues();
	    
	    values.put(KEY_CELL_ID, cellId);
	    values.put(KEY_LABEL_ID, labelId);
	    values.put(KEY_APP_NAME, appName);
	    values.put(KEY_TSys, System.currentTimeMillis());
	    values.put(KEY_TS, getDateTime());
	 
	    // row is inserted
	    long entryId = db.insert(TABLE_APPS, null, values);	    
	    return entryId;
	}
	
	public String getAppLogData (long entryId) {
        SQLiteDatabase db = this.getReadableDatabase();
 
        String selectQuery = "SELECT  * FROM " + TABLE_APPS + " WHERE "
                + KEY_ID + " = " + entryId;
 
        Log.e(TAG, selectQuery);
        Cursor c = db.rawQuery(selectQuery, null);
 
        if (c != null)
            c.moveToFirst();
        
        String value = c.getString(c.getColumnIndex(KEY_APP_NAME));
        //System.out.println (c.getString(c.getColumnIndex(KEY_TS)));
 
        return value;
    }
	
	public ArrayList<HashMap<String, String>> getApps(long time){
		SQLiteDatabase db = this.getReadableDatabase();
		ArrayList<HashMap<String, String>> apps;
		apps = new ArrayList<HashMap<String, String>>();
		 
        String selectQuery = "SELECT * FROM " + TABLE_APPS + " WHERE "
                + KEY_TSys + " > " + time;
        
        Log.e(TAG, selectQuery);
        
        Cursor c = db.rawQuery(selectQuery, null);
 
        if (c != null)
            c.moveToFirst();

        Log.i(TAG,"apps count: "+c.getCount());
        
        for(int i=0;i<c.getCount();i++){
        	HashMap<String, String> map = new HashMap<String, String>();
        	map.put(KEY_CELL_ID,c.getString(c.getColumnIndex(KEY_CELL_ID)));
        	map.put(KEY_LABEL_ID,Integer.toString(c.getInt(c.getColumnIndex(KEY_LABEL_ID))));
        	map.put(KEY_APP_NAME,c.getString(c.getColumnIndex(KEY_APP_NAME)));
        	map.put(KEY_TS,c.getString(c.getColumnIndex(KEY_TS)));
        	map.put(KEY_TSys,c.getString(c.getColumnIndex(KEY_TSys)));
        	apps.add(map);
        	c.moveToNext();
        }
        
        db.close();
        return apps;
	}
	
	public String composeJSONfromSQLiteApps(long time){
		SQLiteDatabase db = this.getReadableDatabase();
		ArrayList<HashMap<String, String>> apps;
		apps = new ArrayList<HashMap<String, String>>();
		 
        String selectQuery = "SELECT * FROM " + TABLE_APPS + " WHERE "
                + KEY_TSys + " > " + time;
        
        Log.e(TAG, selectQuery);
        
        Cursor c = db.rawQuery(selectQuery, null);
 
        if (c != null)
            c.moveToFirst();

        Log.i(TAG,"count: "+c.getCount());
        
        for(int i=0;i<c.getCount();i++){
        	HashMap<String, String> map = new HashMap<String, String>();
        	map.put(KEY_CELL_ID,c.getString(c.getColumnIndex(KEY_CELL_ID)));
        	map.put(KEY_LABEL_ID,Integer.toString(c.getInt(c.getColumnIndex(KEY_LABEL_ID))));
        	map.put(KEY_APP_NAME,c.getString(c.getColumnIndex(KEY_APP_NAME)));
        	map.put(KEY_TS,c.getString(c.getColumnIndex(KEY_TS)));
        	map.put(KEY_TSys,Double.toString(c.getDouble(c.getColumnIndex(KEY_TSys))));
        	apps.add(map);
        	c.moveToNext();
        }
        
        db.close();
        Gson gson = new GsonBuilder().create();
      //Use GSON to serialize Array List to JSON
        return gson.toJson(apps);
	}
	
    public int getAppLogCount() {        
        SQLiteDatabase db = this.getReadableDatabase();
        
        String countQuery = "SELECT  * FROM " + TABLE_APPS;
        Cursor cursor = db.rawQuery(countQuery, null);
 
        int count = cursor.getCount();
        cursor.close();
 
        return count;
    }
    
    public void deleteAppLogRecord(long id) {
        SQLiteDatabase db = this.getWritableDatabase();
        
        db.delete(TABLE_ANSWERS, KEY_ID + " = ?",
                new String[] { String.valueOf(id) });
    }

    
	// helper classes to insert data
	
    private String getDateTime() {
        SimpleDateFormat dateFormat = new SimpleDateFormat(
                "yyyy-MM-dd HH:mm:ss", Locale.getDefault());
        Date date = new Date();
        return dateFormat.format(date);
    }
}