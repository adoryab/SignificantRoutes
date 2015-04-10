package com.aware.plugin.data_collection;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import android.app.Activity;
import android.app.AlertDialog;
import android.content.Context;
import android.content.DialogInterface;
import android.content.Intent;
import android.os.Bundle;
import android.os.CountDownTimer;
import android.os.Environment;
import android.util.Log;
import android.view.View;
import android.widget.AdapterView;
import android.widget.ArrayAdapter;
import android.widget.Button;
import android.widget.EditText;
import android.widget.Spinner;
import android.widget.TextView;

public class ActivityChoice extends Activity {
	
	private String TAG = "DataCollection::ActivityChoice";
	private long timestamp;
	private CountDownTimer timer;
	String path = Environment.getExternalStorageDirectory() + "/AWARE/activity-choices.txt";
	private final File file = new File(path);
	private String type, task, activity = "";
	private boolean oflag = false, cflag = false;
	Context context;

	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		
		Log.v(TAG, "preparing to display activity dialog");
		
		setContentView(R.layout.esm_activity);
		
		this.setFinishOnTouchOutside(false);
		
		context = this;
		Intent intent = getIntent();
		task = intent.getExtras().getString("task");
		type = intent.getExtras().getString("type");
		
		timestamp = intent.getExtras().getLong("timestamp");
		timer = new TimeOut(120000,1000).start();
		
		final Button submit = (Button) findViewById(R.id.spinner_submit);
		final Button cancel = (Button) findViewById(R.id.spinner_cancel);
		
		submit.setVisibility(View.GONE);
		
		String title = "What is your current activity?";
		TextView tv = (TextView) findViewById(R.id.user_title);
		tv.setText(title);
		
		final Spinner s = (Spinner) findViewById(R.id.spinner);
		
		List<String> choices = new ArrayList<String>();
		choices.add("Choose an activity...");
		choices.add("Getting ready for work/school");
		choices.add("Attending class");
		choices.add("Working");
		choices.add("Studying");
		choices.add("Relaxing");
		choices.add("Going to sleep");
		choices.add("Going for lunch/dinner");
		choices.add("Eating");
		choices.add("Cooking");
		choices.add("Cleaning");
		choices.add("Laundry");
		choices.add("Shopping");
		choices.add("Playing sports");
		choices.add("Driving");
		choices.add("Going to work/school");
		choices.add("Going home");
		choices.add("Other");
		choices.add("------------");
		if(file.exists()){
			BufferedReader reader;
			try {
				reader = new BufferedReader(new FileReader(file));
				String line = reader.readLine();
				while(line != null){
					choices.add(line);
					line = reader.readLine();
				}
				reader.close();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		ArrayAdapter<String> adapter = new ArrayAdapter<String>(context,android.R.layout.simple_spinner_item,choices);
		s.setAdapter(adapter);
		s.setOnItemSelectedListener(new AdapterView.OnItemSelectedListener(){
			@SuppressWarnings("deprecation")
			@Override
			public void onItemSelected(AdapterView<?> parent, View view, int position, long id) {
				String str = parent.getSelectedItem().toString();
				submit.setVisibility(View.VISIBLE);
				cflag = true;
				if(str.equals("Other")){
					AlertDialog.Builder popupBuilder = new AlertDialog.Builder(context);
					final AlertDialog popup = popupBuilder.create();
					popup.setTitle("Please be more specific");
					final EditText input = new EditText(context);
					popup.setView(input);
					popup.setCancelable(false);
					popup.setButton("OK", new DialogInterface.OnClickListener(){
						@Override
						public void onClick(DialogInterface dialog, int which) {
							activity = input.getText().toString();
							if(!activity.equals("")){
								oflag = true;
							}
							popup.dismiss();
						}			
					});
					popup.show();
				}
				else{
					activity = str;
				}
			}

			@Override
			public void onNothingSelected(AdapterView<?> parent) {
				//doing nothing here
			}
		});
		
		submit.setOnClickListener(new View.OnClickListener() {
			@Override
			public void onClick(View v) {
				if(activity.equals("")){
					if(cflag){
						activity = s.getSelectedItem().toString();
					}
				}
				if(oflag){
					if(!(file.exists())){
						try {
							file.createNewFile();
						} catch (IOException e1) {
							e1.printStackTrace();
						}
					}
					try {
						BufferedWriter writer = new BufferedWriter(new FileWriter(file,true));
						writer.append(activity);
						writer.newLine();
						writer.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				timer.cancel();
				gotEverything();
			}			
		});
		
		cancel.setOnClickListener(new View.OnClickListener() {	
			@Override
			public void onClick(View v) {
				Log.v(TAG, "Cancel pressed");
				activity = "cancelled";
				timer.cancel();
				gotEverything();
			}
		});
	}
	
	public void gotEverything(){
		Intent intent = new Intent();
		intent.setAction("taskChosen");
		intent.putExtra("type",type);
		intent.putExtra("task",task);
		intent.putExtra("activity",activity);
		intent.putExtra("timestamp",timestamp);
		sendBroadcast(intent);
        ActivityChoice.this.finish();
	}
	
	public class TimeOut extends CountDownTimer{

		public TimeOut(long millisInFuture, long countDownInterval) {
			super(millisInFuture, countDownInterval);
		}

		@Override
		public void onTick(long millisUntilFinished) {
			//doing nothing here
		}

		@Override
		public void onFinish() {
			//same behavior as cancel
			Log.v(TAG, "ActivityChosen timeout");
			Intent intent = new Intent();
			intent.setAction("timeout");
			intent.putExtra("type",type);
			intent.putExtra("task",task);
			sendBroadcast(intent);
			ActivityChoice.this.finish();
		}
		
	}
}
