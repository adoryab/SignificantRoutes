package com.aware.plugin.data_collection;

import android.app.PendingIntent;
import android.content.Context;
import android.content.Intent;
import android.os.Bundle;
import android.util.Log;

import com.google.android.gms.common.ConnectionResult;
import com.google.android.gms.common.GooglePlayServicesClient.ConnectionCallbacks;
import com.google.android.gms.common.GooglePlayServicesClient.OnConnectionFailedListener;
import com.google.android.gms.location.ActivityRecognitionClient;

public class ActivityRecognition implements ConnectionCallbacks, OnConnectionFailedListener{
	
    private Context mcontext;
    // Stores the PendingIntent used to send activity recognition events back to the app
    private PendingIntent mActivityRecognitionPendingIntent;
    // Stores the current instantiation of the activity recognition client
    private ActivityRecognitionClient mActivityRecognitionClient;
    private String TAG = "ActivityRecognition";
    private long interval = 0;//10*1000; //seconds
    
    public ActivityRecognition(Context context) {
    	Log.v(TAG,"in activityrecognition");
    	this.mcontext = context;
    	// Initialize the globals to null
        mActivityRecognitionPendingIntent = null;
        mActivityRecognitionClient = null;
    }
    
    /**
    * Call this to start a scan - don't forget to stop the scan once it's done.
    * Note the scan will not start immediately, because it needs to establish a connection with Google's servers - you'll be notified of this at onConnected
    */
    public void startActivityRecognitionScan(){
    	mActivityRecognitionClient = new ActivityRecognitionClient(mcontext, this, this);
    	mActivityRecognitionClient.connect();
    	Log.v(TAG,"startActivityRecognitionScan");
    }
    
    public void stopActivityRecognitionScan(){
    	try{
    		mActivityRecognitionClient.removeActivityUpdates(mActivityRecognitionPendingIntent);
    		Log.v(TAG,"stopActivityRecognitionScan");
    	} catch (IllegalStateException e){
    	// probably the scan was not set up, we'll ignore
    	}
    }

	@Override
	public void onConnectionFailed(ConnectionResult arg0) {
		Log.v(TAG,"onConnectionFailed");
	}

	@Override
	public void onConnected(Bundle arg0) {
		Log.v(TAG,"onConnected");
		Intent intent = new Intent(mcontext, ActivityRecognitionService.class);
		if (mActivityRecognitionClient == null) {
            mActivityRecognitionClient = new ActivityRecognitionClient(mcontext, this, this);
        }
		mActivityRecognitionPendingIntent = PendingIntent.getService(mcontext, 0, intent,
				PendingIntent.FLAG_UPDATE_CURRENT);
		// 0 sets it to update as fast as possible
		mActivityRecognitionClient.requestActivityUpdates(interval,mActivityRecognitionPendingIntent);
	}

	@Override
	public void onDisconnected() {
		Log.v(TAG,"onDisconnected");
	}

}
