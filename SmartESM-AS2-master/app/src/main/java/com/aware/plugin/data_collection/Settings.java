package com.aware.plugin.data_collection;

import com.aware.Aware;
//import com.aware.ui.Plugins_Manager;


import android.app.Activity;
import android.app.AlertDialog;
import android.content.DialogInterface;
import android.content.Intent;
import android.content.SharedPreferences;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;

@SuppressWarnings("unused")
public class Settings extends Activity {
	public static String ACTION_AWARE_DATA_COLLECTION_STATUS = "com.aware.plugin.data_collection.STATUS";
	//private String TAG = "DataCollection::Settings";
	private String key = "";
	private SharedPreferences preferences;
	
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		
		//check shared preferences for consent to decide layout
		preferences = getSharedPreferences("consent", MODE_PRIVATE);
		boolean consent = preferences.getBoolean("consent", false);
		if(!consent){
			displayConsent();
		}
		else{
			displaySettings();
		}
		
//		if(!Aware.getSetting(getContentResolver(), ACTION_AWARE_DATA_COLLECTION_STATUS).equals("true")) {
//			Intent launchDataCollectionPlugin = new Intent(Plugins_Manager.ACTION_AWARE_ACTIVATE_PLUGIN);
//			launchDataCollectionPlugin.putExtra(Plugins_Manager.EXTRA_PACKAGE_NAME, "com.aware.plugin.data_collection");
//			sendBroadcast(launchDataCollectionPlugin);
//			launchDataCollectionPlugin.setClass(this, com.aware.plugin.data_collection.Plugin.class);
//			startService(launchDataCollectionPlugin);
//		}
	}
	
	private void displaySettings(){
		this.setContentView(R.layout.home);
		
		Button devButton = (Button)this.findViewById(R.id.devButton);
		
		//button for developer to stop/start asking questions
		devButton.setOnClickListener(new View.OnClickListener() {
			@SuppressWarnings("deprecation")
			public void onClick (View v) {
				AlertDialog.Builder popupBuilder = new AlertDialog.Builder(Settings.this);
				final AlertDialog popup = popupBuilder.create();
				popup.setTitle("Please enter password:");
				final EditText input = new EditText(Settings.this);
				popup.setView(input);
				popup.setCancelable(false);
				popup.setButton("OK", new DialogInterface.OnClickListener(){
					@Override
					public void onClick(DialogInterface dialog, int which) {
						key = input.getText().toString();
						if(key.equals("BreakNeeded")){
							Intent intent=new Intent();
							intent.setAction("developer");
							sendBroadcast(intent);
							Settings.this.finish();
						}
						popup.dismiss();
					}			
				});
				popup.show();
			}
		});
	}
	
	private void displayConsent(){
		this.setContentView(R.layout.consent);
		
		Button acceptB = (Button)this.findViewById(R.id.acceptButton);
		Button declineB = (Button)this.findViewById(R.id.declineButton);
		
		acceptB.setOnClickListener(new View.OnClickListener() {
			@Override
			public void onClick(View v) {
				SharedPreferences.Editor edit = preferences.edit();
				edit.putBoolean("consent",true);
				edit.commit();
				displaySettings();
			}
		});
		
		declineB.setOnClickListener(new View.OnClickListener() {
			@Override
			public void onClick(View v) {
				SharedPreferences.Editor edit = preferences.edit();
				edit.putBoolean("consent",false);
				edit.commit();
				Settings.this.finish();
			}
		});
	}
	
}