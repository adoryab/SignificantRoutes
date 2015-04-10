package com.aware.plugin.data_collection;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import android.app.IntentService;
import android.content.Intent;
import android.os.Environment;
import android.util.Log;

import com.google.android.gms.location.ActivityRecognitionResult;
import com.google.android.gms.location.DetectedActivity;

public class ActivityRecognitionService extends IntentService {
	
	private String TAG = "ActivityRecognitionService";
	
	public ActivityRecognitionService() {
		super("ActivityRecognitionService");
		//Log.v(TAG,"in ActivityRecognitionService");
	}

	@Override
	protected void onHandleIntent(Intent intent) {
		// If the intent contains an update
        if (ActivityRecognitionResult.hasResult(intent)) {
        	Log.v(TAG,"recognizing activity");
            // Get the update
            ActivityRecognitionResult result = ActivityRecognitionResult.extractResult(intent);
            // Get the most probable activity from the list of activities in the update
            DetectedActivity mostProbableActivity = result.getMostProbableActivity();
            // Get the confidence percentage for the most probable activity
            //int confidence = mostProbableActivity.getConfidence();
            // Get the type of activity
            int activityType = mostProbableActivity.getType();
            String activityName = getNameFromType(activityType);
            writeData(activityName);
            Intent intent1 = new Intent();
    		intent1.setAction("activityRecognized");
    		intent1.putExtra("activityName",activityName);
    		sendBroadcast(intent1);
        }
	}
	
	private void writeData(String activity){
		String path = Environment.getExternalStorageDirectory() + "/AWARE/activityLog.txt";
		final File file = new File(path);
		//String log = String.valueOf(System.currentTimeMillis())+", "+activity;
		String log = getDateTime()+", "+activity+" ";
		if(!(file.exists())){
			try {
				file.createNewFile();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
		try {
			BufferedWriter writer = new BufferedWriter(new FileWriter(file,true));
			writer.append(log);
			writer.newLine();
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private String getDateTime() {
        SimpleDateFormat dateFormat = new SimpleDateFormat(
                "yyyy-MM-dd HH:mm:ss", Locale.getDefault());
        Date date = new Date();
        return dateFormat.format(date);
    }
	
	private String getNameFromType(int activityType) {
        switch(activityType) {
            case DetectedActivity.IN_VEHICLE:
                return "in_vehicle";
            case DetectedActivity.ON_BICYCLE:
                return "on_bicycle";
            case DetectedActivity.ON_FOOT:
                return "on_foot";
            case DetectedActivity.RUNNING:
                return "on_foot";
            case DetectedActivity.WALKING:
                return "on_foot";
            case DetectedActivity.STILL:
                return "still";
            case DetectedActivity.UNKNOWN:
                return "unknown";
            case DetectedActivity.TILTING:
                return "tilting";
        }
        return "unknown";
    }

}
