package com.aware.plugin.data_collection;

import java.util.LinkedList;
import java.util.ListIterator;

import android.app.Service;
import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;
import android.content.IntentFilter;
import android.os.Binder;
import android.os.IBinder;
import android.util.Log;

public class ActivityRecognitionDecision extends Service {
	
	private String TAG = "ActivityRecognitionDecision";
	private String prevActivity = "";
	private String lastActivity = "";
	private String currActivity = "";
	private String activityName = "";
	private int count = 0;
	private boolean uflag = false;
	private LinkedList<String> ll = new LinkedList<String>();
	private String[] types = {"still","on_foot","on_bicycle","in_vehicle","unknown"};
	
	private final IBinder mBinder = new MyBinder();
	
	@Override
	public void onCreate(){
		super.onCreate();
		Log.v(TAG,"created service");
		
		IntentFilter activityRecognizedFilter = new IntentFilter();
        activityRecognizedFilter.addAction("activityRecognized");
        registerReceiver(activityRecognizedReceiver,activityRecognizedFilter);
	}
	
	BroadcastReceiver activityRecognizedReceiver = new BroadcastReceiver() {

		@Override
		public void onReceive(Context context, Intent intent) {
			activityName = intent.getExtras().getString("activityName");
			Log.i(TAG,"name: "+activityName);
			if(count<10){
				ll.add(activityName);
				count++;
			}
			else{
				ll.remove();
				ll.add(activityName);
			}
			if(count>5){
				decisionMaker();
			}
		}
		
	};
	
	private void decisionMaker(){
		currActivity = getActivity();
		if(approved(currActivity)){
			broadcast();
			prevActivity = lastActivity;
    		lastActivity = currActivity;
		}
		else if(currActivity.equals("unknown")){
			uflag = true;
		}
	}
	
	private void broadcast(){
		Log.v(TAG,"sending activity broadcast");
    	Intent intent1 = new Intent();
		intent1.setAction("activityChanged");
		intent1.putExtra("activityName",currActivity);
		sendBroadcast(intent1);
	}
	
	private String getActivity(){
		String activity = "";
		ListIterator<String> it = ll.listIterator();
		int[] arr = {0,0,0,0,0};
		while(it.hasNext()){
			activity = it.next();
			if(activity.equals(types[0])){
				arr[0]++;
			}
			else if(activity.equals(types[1])){
				arr[1]++;
			}
			else if(activity.equals(types[2])){
				arr[2]++;
			}
			else if(activity.equals(types[3])){
				arr[3]++;
			}
			else if(activity.equals(types[4])){
				arr[4]++;
			}
		}
		int max = 0;
		int index = 4;
		for(int i=0;i<5;i++){
			if(arr[i]>max){
				max = arr[i];
				index = i;
			}
		}
		if(max>5){
			return types[index];
		}
		return "tilting";
	}
	
	private boolean approved(String activity){
		if(activity.equals("unknown") || activity.equals("tilting")){
			uflag = false;
			return false;
		}
		if(lastActivity.equals(activity)){
			uflag = false;
			return false;
		}
		if(uflag){
			uflag = false;
			return true;
		}
		if(lastActivity.equals("") || lastActivity.equals("on_foot")){
			return true;
		}
		if(lastActivity.equals("still")){
			if(activity.equals("on_foot")){
				return true;
			}
			if(activity.equals("in_vehicle") && prevActivity.equals("on_foot")){
				return true;
			}
		}
		if(lastActivity.equals("on_bicyle") && activity.equals("on_foot")){
			return true;
		}
		if(lastActivity.equals("in_vehicle") && activity.equals("on_foot")){
			return true;
		}
		return false;
	}
	
	@Override
	public IBinder onBind(Intent intent) {
		return mBinder;
	}
	
	public class MyBinder extends Binder {
	    ActivityRecognitionDecision getService() {
	      return ActivityRecognitionDecision.this;
	    }
	  }


}
