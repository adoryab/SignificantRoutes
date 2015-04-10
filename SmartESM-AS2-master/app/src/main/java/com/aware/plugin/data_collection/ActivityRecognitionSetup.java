package com.aware.plugin.data_collection;

import com.google.android.gms.common.ConnectionResult;
import com.google.android.gms.common.GooglePlayServicesUtil;

import android.app.Activity;
import android.content.ComponentName;
import android.content.Intent;
import android.content.ServiceConnection;
import android.os.Bundle;
import android.os.IBinder;
import android.util.Log;

public class ActivityRecognitionSetup extends Activity {
	
	private String TAG = "ActivityRecognitionSetup";
	
	private ActivityRecognition activityrecognition;
	@SuppressWarnings("unused")
	private ActivityRecognitionDecision activitydecision;
	private ServiceConnect mConnection;
	
	/*
     * Define a request code to send to Google Play services
     * This code is returned in Activity.onActivityResult
     */
    public final static int CONNECTION_FAILURE_RESOLUTION_REQUEST = 9000;
    // Intent actions and extras for sending information from the IntentService to the Activity
    public static final String ACTION_CONNECTION_ERROR =
            "com.aware.plugin.data_collection.activityrecognition.ACTION_CONNECTION_ERROR";
    public static final String ACTION_REFRESH_STATUS_LIST =
    		"com.aware.plugin.data_collection.activityrecognition.ACTION_REFRESH_STATUS_LIST";
    public static final String CATEGORY_LOCATION_SERVICES =
            "com.aware.plugin.data_collection.activityrecognition.CATEGORY_LOCATION_SERVICES";
    public static final String EXTRA_CONNECTION_ERROR_CODE =
            "com.aware.plugin.data_collection.activityrecognition.EXTRA_CONNECTION_ERROR_CODE";
    public static final String EXTRA_CONNECTION_ERROR_MESSAGE =
            "com.aware.plugin.data_collection.activityrecognition.EXTRA_CONNECTION_ERROR_MESSAGE";
    
    protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		
		activityrecognition = new ActivityRecognition(this);
		
		// Check for Google Play services
        if (!servicesConnected()) {
        	Log.v(TAG,"couldn't connect to google play services");
        }
        else{
        	//start service that decides if activity is updated
        	Intent decision = new Intent(this,ActivityRecognitionDecision.class);
        	startService(decision);
        	mConnection = new ServiceConnect();
        	boolean result = bindService(
        			new Intent(this, ActivityRecognitionDecision.class),
        			mConnection, BIND_AUTO_CREATE);
        	if (!result) {
        		Log.e(TAG,"Unable to bind with service.");
        	}
        	// Pass the update request to the requester object
        	activityrecognition.startActivityRecognitionScan();
        }
        
        exitActivity();
    }
	
    @Override
	protected void onActivityResult(int requestCode, int resultCode, Intent intent) {
		// Choose what to do based on the request code
	    switch (requestCode) {
	    // If the request code matches the code sent in onConnectionFailed
	    case CONNECTION_FAILURE_RESOLUTION_REQUEST :
	    	switch (resultCode) {
	    	// If Google Play services resolved the problem
	    	case Activity.RESULT_OK:
	        	// If the request was to start activity recognition updates
	            // Restart the process of requesting activity recognition updates
	            activityrecognition.startActivityRecognitionScan();
	            break;
	            // If any other result was returned by Google Play services
	            default:
	            	// Report that Google Play services was unable to resolve the problem.
	                Log.v(TAG,"error with google play");
	        }
	    	// If any other request code was received
	    	default:
	    		// Report that this Activity received an unknown requestCode
	            Log.v(TAG,"unknown request code");
	            break;
	    }
	}
	
	private void exitActivity(){
		if(mConnection != null){
			unbindService(mConnection);
			mConnection = null;
		}
		ActivityRecognitionSetup.this.finish();
	}
	
	/**
     * Verify that Google Play services is available before making a request.
     *
     * @return true if Google Play services is available, otherwise false
     */
    private boolean servicesConnected() {
        // Check that Google Play services is available
        int resultCode = GooglePlayServicesUtil.isGooglePlayServicesAvailable(this);
        // If Google Play services is available
        if (ConnectionResult.SUCCESS == resultCode) {
        	// In debug mode, log the status
            Log.v(TAG,"google play available");
            // Continue
            return true;
        } // Google Play services was not available for some reason
        else {
            // Display an error dialog
            GooglePlayServicesUtil.getErrorDialog(resultCode, this, 0).show();
            return false;
        }
    }
    
    protected class ServiceConnect implements ServiceConnection {

	    @Override
		public void onServiceConnected(ComponentName className, IBinder binder) {
	      ActivityRecognitionDecision.MyBinder b = (ActivityRecognitionDecision.MyBinder)binder;
	      activitydecision = b.getService();
	      Log.v(TAG,"connected to decisionService");
	    }

	    @Override
	    public void onServiceDisconnected(ComponentName className) {
	      activitydecision = null;
	    }
	}
    
}
