package com.aware.plugin.data_collection;

import java.util.List;
import java.util.Random;

import android.content.BroadcastReceiver;
import android.content.ContentValues;
import android.content.Context;
import android.content.Intent;
import android.content.IntentFilter;
import android.content.SharedPreferences;
import android.content.pm.ServiceInfo;
import android.database.Cursor;
import android.net.wifi.WifiManager;
import android.os.Bundle;
import android.os.Handler;
import android.os.Vibrator;
import android.telephony.PhoneStateListener;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.view.accessibility.AccessibilityManager;

import com.aware.Aware;
import com.aware.Aware_Preferences;
import com.aware.plugin.data_collection.DataCollection_Provider.DataCollection;
import com.aware.providers.Barometer_Provider.Barometer_Data;
import com.aware.providers.Battery_Provider.Battery_Data;
import com.aware.providers.Applications_Provider.Applications_Foreground;
import com.aware.providers.Screen_Provider.Screen_Data;
import com.aware.providers.Locations_Provider.Locations_Data;
import com.aware.providers.Telephony_Provider.CDMA_Data;
import com.aware.providers.Telephony_Provider.GSM_Data;
import com.aware.utils.Aware_Plugin;

/**
 * AWARE Plugin for DataCollection
 */

public class Plugin extends Aware_Plugin{

	public static final String ACTION_AWARE_DATACOLLECTION = "ACTION_AWARE_DATACOLLECTION";
	private DataCollection_DB db;
	private String data;
	private String type;
	private long lastTimestamp = 0;
	private boolean randomFlag = true;
	private boolean busyFlag = false;
	private boolean inCall = false;
	private boolean esmOn = false;
	private boolean devFlag = false;
	private boolean appFlag = false;
	private boolean screenFlag = true;
	private boolean gsmFlag = false;
	private boolean cdmaFlag = false;
	private long timer;
	private String deviceId = "0";
	private int labelId = -1;
	private String lastActivity = "";
	private long lastTime = 0;
	private long lastSync = 0;
	private long tempSync = 0;
	private int timeout = 15;
	SharedPreferences preferences;
	SharedPreferences.Editor edit;
	Context context;
    private Intent framework;

	@Override
	public void onCreate() {
		super.onCreate();
		context = this;

        framework = new Intent(this, Aware.class);
        startService(framework);

		//activate the sensors
        Aware.setSetting(getApplicationContext(), Aware_Preferences.STATUS_APPLICATIONS, true);
		Aware.setSetting(getApplicationContext(), Aware_Preferences.STATUS_BAROMETER, true);
		Aware.setSetting(getApplicationContext(), Aware_Preferences.FREQUENCY_BAROMETER,300000);

		Aware.setSetting(getApplicationContext(), Aware_Preferences.STATUS_BATTERY, true);

		Aware.setSetting(getApplicationContext(), Aware_Preferences.STATUS_COMMUNICATION_EVENTS, true);

		Aware.setSetting(getApplicationContext(), Aware_Preferences.STATUS_LOCATION_GPS, true);
		Aware.setSetting(getApplicationContext(), Aware_Preferences.STATUS_LOCATION_NETWORK, true);
		Aware.setSetting(getApplicationContext(), Aware_Preferences.FREQUENCY_LOCATION_GPS, 180);
		Aware.setSetting(getApplicationContext(), Aware_Preferences.FREQUENCY_LOCATION_NETWORK, 300);

		Aware.setSetting(getApplicationContext(), Aware_Preferences.STATUS_SCREEN, true);

		Aware.setSetting(getApplicationContext(), Aware_Preferences.STATUS_TELEPHONY, true);

		//apply settings of sensors
		Intent applySettings = new Intent(Aware.ACTION_AWARE_REFRESH);
		sendBroadcast(applySettings);

		//activate plugin if consent received
		preferences = getSharedPreferences("consent", MODE_PRIVATE);
		boolean consent = preferences.getBoolean("consent", false);
		int id = preferences.getInt("labelId", -1);
		if(consent){
			Aware.setSetting(getApplicationContext(),Settings.ACTION_AWARE_DATA_COLLECTION_STATUS,true);
		}
		if(id<0){
			edit = preferences.edit();
			edit.putInt("labelId",1);
			edit.commit();
			labelId = 1;
		}
		else{
			labelId = id;
		}

		TAG = "DataCollection::Plugin";
		//Share the context back to the framework and other applications
		CONTEXT_PRODUCER = new Aware_Plugin.ContextProducer() {
			@Override
			public void onContext() {
				Intent notification = new Intent(ACTION_AWARE_DATACOLLECTION);
				sendBroadcast(notification);
			}
		};

		if(Aware.DEBUG){
			Log.e(TAG,"Plugin started");
		}

		//setup & start activity recognition
		Intent intent = new Intent();
		intent.setClassName("com.aware.plugin.data_collection","com.aware.plugin.data_collection.ActivityRecognitionSetup");
		intent.setFlags(Intent.FLAG_ACTIVITY_NEW_TASK);
		startActivity(intent);

		/*
		 * Broadcast Registering
		 */

		IntentFilter cdmaTowerFilter = new IntentFilter();
		cdmaTowerFilter.addAction("ACTION_AWARE_CDMA_TOWER");
		registerReceiver(cdmaReceiver,cdmaTowerFilter);

		IntentFilter gsmTowerFilter = new IntentFilter();
		gsmTowerFilter.addAction("ACTION_AWARE_GSM_TOWER");
		registerReceiver(gsmReceiver,gsmTowerFilter);

		IntentFilter batteryChargingFilter = new IntentFilter();
		batteryChargingFilter.addAction("ACTION_AWARE_BATTERY_CHARGING");
		registerReceiver(batteryReceiver,batteryChargingFilter);

		IntentFilter batteryDischargingFilter = new IntentFilter();
		batteryDischargingFilter.addAction("ACTION_AWARE_BATTERY_DISCHARGING");
		registerReceiver(batteryReceiver,batteryDischargingFilter);

		IntentFilter screenOffFilter = new IntentFilter();
		// ACTION_AWARE_SCREEN_OFF - just screen off
		screenOffFilter.addAction("ACTION_AWARE_SCREEN_OFF");
		registerReceiver(screenReceiver,screenOffFilter);

		IntentFilter screenOnFilter = new IntentFilter();
		// ACTION_AWARE_SCREEN_ON - just screen on
		screenOnFilter.addAction("ACTION_AWARE_SCREEN_ON");
		registerReceiver(screenOnReceiver,screenOnFilter);

		IntentFilter applicationLogFilter = new IntentFilter();
		applicationLogFilter.addAction("ACTION_AWARE_APPLICATIONS_FOREGROUND");
		registerReceiver(applicationLogReceiver,applicationLogFilter);

		IntentFilter answerNowfilter = new IntentFilter();
		answerNowfilter.addAction("answerNow");
		registerReceiver(answerNowReceiver, answerNowfilter);

		IntentFilter answerLaterfilter = new IntentFilter();
		answerLaterfilter.addAction("answerLater");
		registerReceiver(answerLaterReceiver, answerLaterfilter);

		IntentFilter answerCancelfilter = new IntentFilter();
		answerCancelfilter.addAction("answerCancel");
		registerReceiver(answerCancelReceiver, answerCancelfilter);

		IntentFilter answerBusyfilter = new IntentFilter();
		answerBusyfilter.addAction("answerBusy");
		registerReceiver(answerBusyReceiver, answerBusyfilter);

		IntentFilter timeoutfilter = new IntentFilter();
		timeoutfilter.addAction("timeout");
		registerReceiver(timeoutReceiver, timeoutfilter);

		IntentFilter noIssuefilter = new IntentFilter();
		timeoutfilter.addAction("noIssue");
		registerReceiver(noIssueReceiver, noIssuefilter);

		IntentFilter taskChosenfilter = new IntentFilter();
		taskChosenfilter.addAction("taskChosen");
		registerReceiver(taskChosenReceiver, taskChosenfilter);

		IntentFilter userInputfilter = new IntentFilter();
		userInputfilter.addAction("inputReceived");
		registerReceiver(userInputReceiver, userInputfilter);

		IntentFilter activityfilter = new IntentFilter();
		activityfilter.addAction("activityChanged");
		registerReceiver(activityReceiver, activityfilter);

		IntentFilter devfilter = new IntentFilter();
		devfilter.addAction("developer");
		registerReceiver(devReceiver, devfilter);

		IntentFilter syncfilter = new IntentFilter();
		syncfilter.addAction("syncSuccess");
		registerReceiver(syncReceiver, syncfilter);

		// to listen to the phone calls state
		setUpCallHandler();
	}

    public void setUpCallHandler(){

		TelephonyManager telephonyManager =
				(TelephonyManager)getSystemService(Context.TELEPHONY_SERVICE);

		deviceId = telephonyManager.getDeviceId();

		SharedPreferences sp = getSharedPreferences("deviceID", MODE_PRIVATE);
		SharedPreferences.Editor edit = sp.edit();
		edit.putString("deviceId",deviceId);
		edit.commit();

		PhoneStateListener callStateListener = new PhoneStateListener() {
			public void onCallStateChanged(int state, String incomingNumber) {
				// get the phone application name on the phone
				String appName = "";
				if(state==TelephonyManager.CALL_STATE_RINGING){
					appName = appName + "Phone Call Ringing";
					inCall = true;
				}
				if(state==TelephonyManager.CALL_STATE_OFFHOOK){
					appName = appName + "Phone Call OffHook";
					inCall = true;
				}
				if(state==TelephonyManager.CALL_STATE_IDLE){
					appName = appName + "Phone Call Done";
					inCall = false;
				}
				// place data in application info table
				Log.v(TAG, "Call app is logged "+appName);
				db = new DataCollection_DB (getApplicationContext());
				long entryVal = db.createAppLogRecord(appName,deviceId,labelId);
				Log.v(TAG, "Entries: " + Long.valueOf(entryVal));
				db.close();
			}
		};
		telephonyManager.listen(callStateListener,PhoneStateListener.LISTEN_CALL_STATE);
	}

	BroadcastReceiver gsmReceiver = new BroadcastReceiver() {

		@Override
		public void onReceive(Context context, Intent intent) {
			gsmFlag = true;
			cdmaFlag = false;
		}

	};
	BroadcastReceiver cdmaReceiver = new BroadcastReceiver() {

		@Override
		public void onReceive(Context context, Intent intent) {
			cdmaFlag = true;
			gsmFlag = false;
		}

	};

	BroadcastReceiver applicationLogReceiver = new BroadcastReceiver() {

		@Override
		public void onReceive(Context context, Intent intent) {
			appFlag = true;

			String appName = "";

			Cursor appCursor = context.getContentResolver().query(Applications_Foreground.CONTENT_URI, null, null, null, "timestamp DESC LIMIT 1");
			if( appCursor != null && appCursor.moveToFirst()) {
				do {
					appName = appCursor.getString(appCursor.getColumnIndex(Applications_Foreground.APPLICATION_NAME));
				}while(appCursor.moveToNext());
			}
			appCursor.close();

			try{
				Log.v(TAG, "Application logged is " + appName);
				// place data in application info table
				db = new DataCollection_DB (context);
				long entryVal = db.createAppLogRecord(appName,deviceId,labelId);
				Log.v(TAG, "Entries: " + Long.valueOf(entryVal));
				db.close();
			}catch(Exception e){
				Log.e(TAG, e.getMessage());
			}
		}
	};
	BroadcastReceiver batteryReceiver = new BroadcastReceiver() {
		@Override
		public void onReceive(Context context, Intent intent) {

			String batteryInfo = intent.toString();
			String data = null;
			if (batteryInfo.contains("ACTION_AWARE_BATTERY_CHARGING")){
				data = "Plugged in";
				if((System.currentTimeMillis()-lastSync) > (12*60*60000)){//12hours
					syncNow();
				}
			}
			else if (batteryInfo.contains("ACTION_AWARE_BATTERY_DISCHARGING")){
				data = "Plugged out";
			}
			Log.e(TAG, batteryInfo);

			if(!lastActivity.equals("in_vehicle") &&
					checkLastESM(System.currentTimeMillis(),timeout)){
				Vibrator v = (Vibrator) getSystemService(Context.VIBRATOR_SERVICE);
				long[] pattern = {150,700,150,700,300,700,300,700,450,700,450,700};
				v.vibrate(pattern,-1);
				displayPreScreen("battery", data);
			}
			else{
				Intent intent1 = new Intent();
				intent1.putExtra("type","battery "+data);
				intent1.setAction("noIssue");
				sendBroadcast(intent1);
			}
		}
	};

	BroadcastReceiver screenOnReceiver = new BroadcastReceiver() {
		@Override
		public void onReceive(Context context, Intent intent) {
			Log.i(TAG,"screen on");
			appFlag = false;
			screenFlag = true;
		}
	};

	BroadcastReceiver screenReceiver = new BroadcastReceiver() {
		@Override
		public void onReceive(Context context, Intent intent) {

			screenFlag = false;
			Log.v(TAG, intent.toString());
			try{
				if(!appFlag){
					Log.e(TAG,"no app used");
					return;
				}
				if(checkLastESM(System.currentTimeMillis(),timeout)) {
					/*
					 * post the ESM immediately after the user locks the screen
					 * once data is collected lock the screen
					 */
					appFlag = false;
					Log.v(TAG, "Calling the screen handler");
					Vibrator v = (Vibrator) getSystemService(Context.VIBRATOR_SERVICE);
					long[] pattern = {150,700,150,700,300,700,300,700,450,700,450,700};
					v.vibrate(pattern,-1);
					displayPreScreen("screen", null);
				}
				else{
					Intent intent1 = new Intent();
					intent1.putExtra("type","screen");
					intent1.setAction("noIssue");
					sendBroadcast(intent1);
				}
			}catch(Exception e){
				Log.e(TAG, e.getMessage());
			}
		}
	};

	BroadcastReceiver activityReceiver = new BroadcastReceiver() {
		@Override
		public void onReceive(Context context, Intent intent) {
			String activity = intent.getExtras().getString("activityName");
			lastActivity = activity;
			lastTime = System.currentTimeMillis();
			Log.i(TAG,"received broadcast "+activity);
			if(screenFlag){
				saveInput("activity "+activity,"screen on","screen on");
			}
			else if(checkLastESM(System.currentTimeMillis(),timeout)){
				Vibrator v = (Vibrator) getSystemService(Context.VIBRATOR_SERVICE);
				long[] pattern = {150,700,150,700,300,700,300,700,450,700,450,700};
				v.vibrate(pattern,-1);
				displayPreScreen("activity", activity);
			}
			else{
				Intent intent1 = new Intent();
				intent1.putExtra("type",activity);
				intent1.setAction("noIssue");
				sendBroadcast(intent1);
			}
		}
	};

	BroadcastReceiver answerNowReceiver = new BroadcastReceiver() {
		@Override
		public void onReceive(Context context, Intent intent) {
			Bundle bundle = intent.getExtras();
			String type = bundle.getString("type");
			String data = bundle.getString("data");
			startESM(type, data);
		}
	};
	BroadcastReceiver answerLaterReceiver = new BroadcastReceiver(){
		@Override
		public void onReceive(Context context, Intent intent) {
			Bundle bundle = intent.getExtras();
			type = bundle.getString("type");
			data = bundle.getString("data");
			int timeout = bundle.getInt("timeout");
			Log.v(TAG, "launch the ESM in " + Integer.toString(timeout)+" minutes");

			if (timeout == 0){
				timeout = 5;
			}
			else{
				timeout = 10;
			}

			lastTimestamp = System.currentTimeMillis();
			esmOn = false;

			Handler mHandler = new Handler();
			mHandler.postDelayed(new Runnable(){
				public void run(){
					esmOn = true;
					Log.v(TAG, "going to start the ESM");
					Vibrator v = (Vibrator) getSystemService(Context.VIBRATOR_SERVICE);
					long[] pattern = {150,700,150,700,300,700,300,700,450,700,450,700};
					v.vibrate(pattern,-1);
					startESM(type, data);
				}
			}, timeout * 60000); // ----- this is for timeout minutes
		}
	};
	BroadcastReceiver answerBusyReceiver = new BroadcastReceiver() {
		@Override
		public void onReceive(Context context, Intent intent) {
			Log.e(TAG,"in busy receiver");
			Bundle bundle = intent.getExtras();
			String type = bundle.getString("type");
			timer = System.currentTimeMillis();
			busyFlag = true;
			esmOn = false;
			if(lastTimestamp==0){
				lastTimestamp = timer;
			}
			saveInput(type,"busy","busy");
		}
	};

	BroadcastReceiver answerCancelReceiver = new BroadcastReceiver() {
		@Override
		public void onReceive(Context context, Intent intent) {
			Log.v(TAG,"in cancel Receiver");
			Bundle bundle = intent.getExtras();
			String type = bundle.getString("type");
			lastTimestamp = System.currentTimeMillis();
			esmOn = false;
			saveInput(type,"cancelled","cancelled");
		}
	};

	BroadcastReceiver taskChosenReceiver = new BroadcastReceiver() {
		@Override
		public void onReceive(Context context, Intent intent) {
			String type = intent.getExtras().getString("type");
			String task = intent.getExtras().getString("task");
			String activity = intent.getExtras().getString("activity");
			lastTimestamp = intent.getExtras().getLong("timestamp");
			esmOn = false;
			saveInput(type,task,activity);
		}
	};
	BroadcastReceiver userInputReceiver = new BroadcastReceiver() {
		@Override
		public void onReceive(Context context, Intent intent) {
			Log.i(TAG,"received user input");
			lastTimestamp = intent.getExtras().getLong("timestamp");
			String input = intent.getExtras().getString("input");
			String trigger = intent.getExtras().getString("type");
			esmOn = false;
			saveInput(trigger,"",input);
		}
	};

	BroadcastReceiver noIssueReceiver = new BroadcastReceiver(){
		@Override
		public void onReceive(Context context, Intent intent) {
			Log.v(TAG,"in noIssue Receiver");
			Bundle bundle = intent.getExtras();
			String type = bundle.getString("type");
			String task = "not issued";
			String activity = "not issued";
			esmOn = false;
			saveInput(type,task,activity);
		}
	};

	BroadcastReceiver timeoutReceiver = new BroadcastReceiver(){
		@Override
		public void onReceive(Context context, Intent intent) {
			Log.v(TAG,"in timeout Receiver");
			Bundle bundle = intent.getExtras();
			String type = bundle.getString("type");
			String task = bundle.getString("task","timeout");
			String activity = bundle.getString("activity","timeout");
			esmOn = false;
			saveInput(type,task,activity);
		}
	};

	BroadcastReceiver devReceiver = new BroadcastReceiver(){
		@Override
		public void onReceive(Context context, Intent intent) {
			if(devFlag){
				devFlag = false;
			}
			else{
				devFlag = true;
				syncNow();
				entropy();
			}
		}
	};
	BroadcastReceiver syncReceiver = new BroadcastReceiver(){
		@Override
		public void onReceive(Context context, Intent intent) {
			lastSync = tempSync;
		}
	};

	private void syncNow(){
		Intent i = new Intent();
		i.setClass(this,ServerSync.class);
		i.setFlags(Intent.FLAG_ACTIVITY_NEW_TASK);
		i.putExtra("time",lastSync);
		tempSync = System.currentTimeMillis();
		startActivity(i);
	}

	private void entropy(){
		Intent i = new Intent();
		i.setClass(this,EntropyUpdater.class);
		i.setFlags(Intent.FLAG_ACTIVITY_NEW_TASK);
		startActivity(i);
	}

	private boolean randomCheck(){
		Random r= new Random();
		int num = r.nextInt(101);
		if(num<50){
			Log.e(TAG,"random check TRUE");
			return true;
		}
		else{
			Log.e(TAG,"random check FALSE");
			return false;
		}
	}

	/*
	 * Checks if the last ESM query was initiated within the threshold
	 * This would be in minutes.
	 * Also, 1. Dice would be included here
	 * 2. Entropy calculation would be added here
	 */
	public boolean checkLastESM(long timestamp,double threshold) {
		if(devFlag){
			Log.e(TAG,"waiting for developer flag");
			return false;
		}
		if(inCall){
			Log.e(TAG,"waiting for inCall flag");
			return false;
		}

		if(esmOn){
			Log.e(TAG,"waiting for esmOn flag");
			return false;
		}

		if(busyFlag){
			if((timestamp-timer) > (60*60000)){
				Log.e(TAG,"Resetting busy flag");
				busyFlag = false;
			}
			else{
				Log.e(TAG,"waiting for busy timer");
				return false;
			}
		}

		if((timestamp-lastTimestamp) > threshold*60000) {
			if(randomFlag && !randomCheck()){
				Log.e(TAG,"Timestamp TRUE but Random FALSE");
				return false;
			}
			Log.e(TAG,"ESM check TRUE Timestamp: " + timestamp + " LastTimestamp: " + lastTimestamp + " Threshold: " + threshold + " Difference: " + (timestamp-lastTimestamp));
			return true;
		}
		else {
			Log.e(TAG,"ESM check FALSE Timestamp: " + timestamp + " LastTimestamp: " + lastTimestamp + " Threshold: " + threshold + " Difference: " + (timestamp-lastTimestamp));
			return false;
		}
	}

	public boolean displayPreScreen(String type, String data){

		esmOn = true;
		//give the user choices to log the information later
		Intent preScreenIntent = new Intent();
		Bundle bundle = new Bundle();
		bundle.putString("type", type);
		bundle.putString("data", data);

		preScreenIntent.setClassName("com.aware.plugin.data_collection","com.aware.plugin.data_collection.PreScreen");
		preScreenIntent.setFlags(Intent.FLAG_ACTIVITY_NEW_TASK);
		preScreenIntent.putExtras(bundle);

		// as we cannot call startActivityForResult from a service
		// the information keeps bouncing across the activities using bundle
		startActivity(preScreenIntent);

		return true;
	}

	public void startESM(String trigger, String data) {

		Log.v(TAG, "Create ESM and display. Trigger is "+trigger);
		//Aware.setSetting(getContentResolver(), Aware_Preferences.STATUS_ESM, true);

		if (trigger.equals("screen")){
			Intent i = new Intent();
			i.setClass(this, TaskChoice.class);
			i.setFlags(Intent.FLAG_ACTIVITY_NEW_TASK);
			i.putExtra("type",trigger);
			startActivity(i);
			return;
		}
		else{
			Intent i = new Intent();
			i.setClass(this, UserInput.class);
			i.setFlags(Intent.FLAG_ACTIVITY_NEW_TASK);
			i.putExtra("type",trigger);
			startActivity(i);
			return;
		}
	}

	public void saveInput(String type,String task,String activity) {
		Log.v(TAG,"saving activity info");

		saveUsageData();

		db = new DataCollection_DB (this);
		Labels labels = new Labels();

		labels.setCellId(deviceId);
		labels.setLabelId(labelId);
		labels.setTrigger(type);
		labels.setLabel(task);
		labels.setAnswer(activity);

		db.createLabelsRecord(labels);
		db.close();

		labelId++;
		edit = preferences.edit();
		edit.putInt("labelId",labelId);
		edit.commit();
	}

	public void saveUsageData() {
		// places in to the respective content providers
		ContentValues rowData = new ContentValues();
		rowData.put(DataCollection.DEVICE_ID, Aware.getSetting(getApplicationContext(), Aware_Preferences.DEVICE_ID));
		rowData.put(DataCollection.TIMESTAMP, System.currentTimeMillis());

		if((System.currentTimeMillis() - lastTime) < 300000)
			rowData.put(DataCollection.ACTIVITY_TYPE,lastActivity);

		Cursor batteryCursor = this.getContentResolver().query(Battery_Data.CONTENT_URI, null, null, null, "timestamp DESC LIMIT 1");

		long last_Timestamp = 0;
		int batteryStatus = -1;
		int temperature = -1;

		if( batteryCursor != null && batteryCursor.moveToFirst()) {
			do {
				last_Timestamp = batteryCursor.getLong(batteryCursor.getColumnIndex(Battery_Data.TIMESTAMP));
				batteryStatus = batteryCursor.getInt(batteryCursor.getColumnIndex(Battery_Data.STATUS));
				temperature = batteryCursor.getInt(batteryCursor.getColumnIndex(Battery_Data.TEMPERATURE));
			}while(batteryCursor.moveToNext());
		}

		batteryCursor.close();

		if((System.currentTimeMillis() - last_Timestamp) < 300000){
			rowData.put(DataCollection.BATTERY_STATUS, batteryStatus);
			rowData.put(DataCollection.TEMPERATURE_STATUS, temperature);
		}

		Cursor screenCursor = this.getContentResolver().query(Screen_Data.CONTENT_URI, null, null, null, "timestamp DESC LIMIT 1");

		last_Timestamp = 0;
		int screenStatus = -1;

		if(screenCursor != null && screenCursor.moveToFirst()) {
			do {
				last_Timestamp = screenCursor.getLong(screenCursor.getColumnIndex(Screen_Data.TIMESTAMP));
				screenStatus = screenCursor.getInt(screenCursor.getColumnIndex(Screen_Data.SCREEN_STATUS));
			}while(screenCursor.moveToNext());
		}

		screenCursor.close();

		if((System.currentTimeMillis() - last_Timestamp) < 300000)
			rowData.put(DataCollection.SCREEN_STATUS, screenStatus);

		Cursor locationCursor = this.getContentResolver().query(Locations_Data.CONTENT_URI, null, null, null, "timestamp DESC LIMIT 1");

		last_Timestamp = 0;
		String location = "";
		String accuracy = "";

		if( locationCursor != null && locationCursor.moveToFirst()) {
			do {
				last_Timestamp = locationCursor.getLong(locationCursor.getColumnIndex(Locations_Data.TIMESTAMP));
				location = locationCursor.getString(locationCursor.getColumnIndex(Locations_Data.LATITUDE));
				location += ";";
				location += locationCursor.getString(locationCursor.getColumnIndex(Locations_Data.LONGITUDE));
				accuracy = locationCursor.getString(locationCursor.getColumnIndex(Locations_Data.ACCURACY));
			}while(locationCursor.moveToNext());
		}

		if((System.currentTimeMillis() - last_Timestamp) < 300000){
			rowData.put(DataCollection.LOCATION, location);
		}

		locationCursor.close();

		if(rowData.size() > 0)
			getContentResolver().insert(DataCollection.CONTENT_URI, rowData);

		// WiFi information
		WifiManager wifiManager = (WifiManager)getApplicationContext().getSystemService(Context.WIFI_SERVICE);
		int wifiStrength = wifiManager.getConnectionInfo().getRssi();
		String wifiName = wifiManager.getConnectionInfo().getSSID();

		Cursor barometerCursor = this.getContentResolver().query(Barometer_Data.CONTENT_URI, null, null, null, "timestamp DESC LIMIT 1");

		last_Timestamp = 0;
		int barometer = 0;

		if(barometerCursor != null && barometerCursor.moveToFirst()) {
			do {
				last_Timestamp = barometerCursor.getLong(barometerCursor.getColumnIndex(Barometer_Data.TIMESTAMP));
				barometer = barometerCursor.getInt(barometerCursor.getColumnIndex(Barometer_Data.AMBIENT_PRESSURE));
			}while(barometerCursor.moveToNext());
		}

		barometerCursor.close();

		if((System.currentTimeMillis() - last_Timestamp) < 300000)
			rowData.put(DataCollection.BAROMETER_STATUS, barometer);

		String tower = "-1";
		last_Timestamp = 0;
		if(cdmaFlag){
			Cursor cdmaCursor = this.getContentResolver().query(CDMA_Data.CONTENT_URI, null, null, null, "timestamp DESC LIMIT 1");
			if(cdmaCursor != null && cdmaCursor.moveToFirst()) {
				do {
					last_Timestamp = cdmaCursor.getLong(cdmaCursor.getColumnIndex(CDMA_Data.TIMESTAMP));
					tower = cdmaCursor.getString(cdmaCursor.getColumnIndex(CDMA_Data.BASE_STATION_ID));
				}while(cdmaCursor.moveToNext());
			}

			cdmaCursor.close();

			if((System.currentTimeMillis() - last_Timestamp) < 300000)
				rowData.put(DataCollection.TOWER, tower);
		}
		else if(gsmFlag){
			Cursor gsmCursor = this.getContentResolver().query(GSM_Data.CONTENT_URI, null, null, null, "timestamp DESC LIMIT 1");
			if(gsmCursor != null && gsmCursor.moveToFirst()) {
				do {
					last_Timestamp = gsmCursor.getLong(gsmCursor.getColumnIndex(GSM_Data.TIMESTAMP));
					tower = gsmCursor.getString(gsmCursor.getColumnIndex(GSM_Data.CID));
				}while(gsmCursor.moveToNext());
			}

			gsmCursor.close();

			if((System.currentTimeMillis() - last_Timestamp) < 300000)
				rowData.put(DataCollection.TOWER, tower);
		}

		// place data in to the context data table
		db = new DataCollection_DB(this);
		ContextData cd = new ContextData(deviceId,labelId,wifiStrength,wifiName,
				lastActivity,batteryStatus,barometer,temperature,location,accuracy,tower);

		db.createCDRecord(cd);
		db.close();
	}

	@Override
	public void onDestroy() {
		super.onDestroy();

		//turn off sensors
        Aware.setSetting(getApplicationContext(), Aware_Preferences.STATUS_APPLICATIONS, false);
		Aware.setSetting(getApplicationContext(), Aware_Preferences.STATUS_BAROMETER, false);
		Aware.setSetting(getApplicationContext(), Aware_Preferences.STATUS_BATTERY, false);
		Aware.setSetting(getApplicationContext(), Aware_Preferences.STATUS_COMMUNICATION_EVENTS, false);
		Aware.setSetting(getApplicationContext(), Aware_Preferences.STATUS_LOCATION_GPS, false);
		Aware.setSetting(getApplicationContext(), Aware_Preferences.STATUS_LOCATION_NETWORK, false);
		Aware.setSetting(getApplicationContext(), Aware_Preferences.STATUS_SCREEN, false);
		Aware.setSetting(getApplicationContext(), Aware_Preferences.STATUS_TELEPHONY, false);
		//apply settings
		Intent applySettings = new Intent(Aware.ACTION_AWARE_REFRESH);
		sendBroadcast(applySettings);
		//unregister listeners
		unregisterReceiver(gsmReceiver);
		unregisterReceiver(cdmaReceiver);
		unregisterReceiver(batteryReceiver);
		unregisterReceiver(screenReceiver);
		unregisterReceiver(applicationLogReceiver);
		unregisterReceiver(timeoutReceiver);
		unregisterReceiver(noIssueReceiver);
		unregisterReceiver(answerNowReceiver);
		unregisterReceiver(answerLaterReceiver);
		unregisterReceiver(answerCancelReceiver);
		unregisterReceiver(answerBusyReceiver);
		unregisterReceiver(taskChosenReceiver);
		unregisterReceiver(userInputReceiver);
		unregisterReceiver(activityReceiver);
		unregisterReceiver(devReceiver);
		unregisterReceiver(syncReceiver);
		unregisterReceiver(screenOnReceiver);

        stopService(framework);
	}	
}