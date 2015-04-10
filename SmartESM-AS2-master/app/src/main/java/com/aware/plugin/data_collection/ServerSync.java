package com.aware.plugin.data_collection;

import java.util.ArrayList;
import java.util.HashMap;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.loopj.android.http.AsyncHttpClient;
import com.loopj.android.http.AsyncHttpResponseHandler;
import com.loopj.android.http.RequestParams;

import android.app.Activity;
import android.content.Intent;
import android.os.Bundle;
import android.util.Log;

public class ServerSync extends Activity {
	
	private String TAG = "ServerSync";
	private DataCollection_DB db;
	private String server = "http://smartesm.cmu-tbank.com/insert_data.php";
	private String lastTime = "";
	private long time = 0;
	
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		
		Log.v(TAG,"updating server");
		Intent intent = getIntent();
		time = intent.getLongExtra("time",0);
		db = new DataCollection_DB(this);
		syncSQLiteMySQLDB();
		
		exitActivity();
	}
	
	private void exitActivity(){
		db.close();
		ServerSync.this.finish();
	}
	
	private void sendSuccess(){
		Intent i = new Intent();
		i.setAction("syncSuccess");
		sendBroadcast(i);
	}
	
	public void syncSQLiteMySQLDB(){
		AsyncHttpClient client = new AsyncHttpClient();
		RequestParams params = new RequestParams();
		ArrayList<HashMap<String, String>> data = db.getAnswers(time);
		ArrayList<HashMap<String, String>> data2 = db.getApps(time);
		ArrayList<HashMap<String, String>> data3 = db.getContext(time);
		Log.i(TAG,"syncing now");
		if(data.size()>0){
			params.put("answersJSON",db.composeJSONfromSQLite(time));
			if(data2.size()>0){
				params.put("appsJSON",db.composeJSONfromSQLiteApps(time));
			}
			if(data3.size()>0){
				params.put("contextJSON",db.composeJSONfromSQLiteContext(time));
			}
			client.post(server,params,new AsyncHttpResponseHandler() {
				@Override
				public void onSuccess(String response) {
					Log.i(TAG,"res: "+response);
					try {
						JSONArray arr = new JSONArray(response);
						for(int i=0; i<arr.length();i++){
							JSONObject obj = (JSONObject)arr.get(i);
							lastTime = (String) obj.get("timestamp");
						}
						Log.e(TAG, "DB Sync completed!");
						Log.i(TAG,"lastTime: "+lastTime);
						sendSuccess();
					} catch (JSONException e) {
						Log.e(TAG, "Error Occured [Server's JSON response might be invalid]!");
						e.printStackTrace();
					}
				}
				@Override
				public void onFailure(int statusCode, Throwable error,String content) {
					if(statusCode == 404){
						Log.e(TAG,"Requested resource not found");
					}else if(statusCode == 500){
						Log.e(TAG,"Something went wrong at server end");
					}else{
						Log.e(TAG,"Unexpected Error occcured! [Most common Error: Device might not be connected to Internet]");
					}
				}
			});
		}
		else{
			Log.i(TAG,"SQLite and Remote MySQL DBs are in Sync!");
		}
	}

}
