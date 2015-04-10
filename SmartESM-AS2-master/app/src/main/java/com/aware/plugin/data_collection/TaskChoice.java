package com.aware.plugin.data_collection;

import android.app.Activity;
import android.app.AlertDialog;
import android.content.Context;
import android.content.DialogInterface;
import android.content.Intent;
import android.os.Bundle;
import android.os.CountDownTimer;
import android.util.Log;
import android.view.View;
import android.widget.Button;
import android.widget.RadioButton;
import android.widget.RadioGroup;
import android.widget.RadioGroup.OnCheckedChangeListener;

public class TaskChoice extends Activity {
	
	private String TAG = "DataCollection::TaskChoice";
	private long timestamp;
	private CountDownTimer timer;
	String task,type;
	Context context;

	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		
		Log.v(TAG, "preparing to display task dialog");
		
		setContentView(R.layout.task_esm);
		context = this;
		
		this.setFinishOnTouchOutside(false);
		
		type  = this.getIntent().getExtras().getString("type");
		timestamp = System.currentTimeMillis();
		timer = new TimeOut(120000,1000).start();
		
		//get buttons and set onClickListeners
		final Button consumeH = (Button) findViewById(R.id.consumeHelp);
		final Button createH = (Button) findViewById(R.id.createHelp);
		final Button planningH = (Button) findViewById(R.id.planningHelp);
		final Button mixtureH = (Button) findViewById(R.id.mixtureHelp);
		final Button nothingH = (Button) findViewById(R.id.nothingHelp);
		final Button cancelB = (Button) findViewById(R.id.cancelButton);
		final Button submitB = (Button) findViewById(R.id.submitButton);
				
		submitB.setVisibility(View.GONE);
		
		//set task choices as radio options
		RadioGroup taskOptions = (RadioGroup) findViewById(R.id.taskRadio);
		String[] choices = {"Consume Information","Create Information",
				"Planning","Mixture","Nothing Special"};
		for(int i=0;i<5;i++){
			final RadioButton radioOption = new RadioButton(TaskChoice.this);
            radioOption.setId(i);
            radioOption.setText(choices[i]);
            taskOptions.addView(radioOption);
		}
		taskOptions.setOnCheckedChangeListener(new OnCheckedChangeListener(){
			@Override
			public void onCheckedChanged(RadioGroup group, int checkedId) {
				submitB.setVisibility(View.VISIBLE);
			}
		});
		
		cancelB.setOnClickListener(new View.OnClickListener() {	
			@Override
			public void onClick(View v) {
				Log.v(TAG, "Cancel pressed");
		    	task = "cancelled";
		    	timer.cancel();
		    	broadcast();
			}
		});
		
		submitB.setOnClickListener(new View.OnClickListener() {	
			@Override
			public void onClick(View v) {
				Log.v(TAG, "Task Submitted");
				getChoice();
			}
		});
		
		consumeH.setOnClickListener(new View.OnClickListener() {
			@SuppressWarnings("deprecation")
			@Override
			public void onClick(View v) {
			    AlertDialog.Builder popupBuilder = new AlertDialog.Builder(context);
				final AlertDialog popup = popupBuilder.create();
				popup.setTitle("Task Help");
				popup.setMessage("Consuming Information \n Getting Information, Reading" +
								"\n Ex. Reading, checking email/sms, listening to music, " +
								"receiving a phone call etc.");
				popup.setCancelable(false);
				popup.setButton("OK", new DialogInterface.OnClickListener(){
					@Override
					public void onClick(DialogInterface dialog, int which) {
						popup.dismiss();
					}			
				});
				popup.show();
			}			
		});
		
		createH.setOnClickListener(new View.OnClickListener() {
			@SuppressWarnings("deprecation")
			@Override
			public void onClick(View v) {
			    AlertDialog.Builder popupBuilder = new AlertDialog.Builder(context);
				final AlertDialog popup = popupBuilder.create();
				popup.setTitle("Task Help");
				popup.setMessage("Creating Information \n Writing, Input data \n " +
								"Ex. Writing an email/sms, making a phone call, " +
								"taking a picture etc.");
				popup.setCancelable(false);
				popup.setButton("OK", new DialogInterface.OnClickListener(){
					@Override
					public void onClick(DialogInterface dialog, int which) {
						popup.dismiss();
					}			
				});
				popup.show();
			}				
		});
		
		planningH.setOnClickListener(new View.OnClickListener() {
			@SuppressWarnings("deprecation")
			@Override
			public void onClick(View v) {
			    AlertDialog.Builder popupBuilder = new AlertDialog.Builder(context);
				final AlertDialog popup = popupBuilder.create();
				popup.setTitle("Task Help");
				popup.setMessage("Planning \n Scheduling, Getting directions \n " +
								"Ex. Checking the calendar, checking bus schedule, checking " +
								"site's reviews, making a reservation on the phone, " +
								"coordinating a meeting with people through a call/sms etc.");
				popup.setCancelable(false);
				popup.setButton("OK", new DialogInterface.OnClickListener(){
					@Override
					public void onClick(DialogInterface dialog, int which) {
						popup.dismiss();
					}			
				});
				popup.show();
			}			
		});
		
		mixtureH.setOnClickListener(new View.OnClickListener() {
			@SuppressWarnings("deprecation")
			@Override
			public void onClick(View v) {
			    AlertDialog.Builder popupBuilder = new AlertDialog.Builder(context);
				final AlertDialog popup = popupBuilder.create();
				popup.setTitle("Task Help");
				popup.setMessage("Mixture \n More than one of above \n " +
								"Ex. Scheduling using a to-do list etc.");
				popup.setCancelable(false);
				popup.setButton("OK", new DialogInterface.OnClickListener(){
					@Override
					public void onClick(DialogInterface dialog, int which) {
						popup.dismiss();
					}			
				});
				popup.show();
			}		
		});
		
		nothingH.setOnClickListener(new View.OnClickListener() {
			@SuppressWarnings("deprecation")
			@Override
			public void onClick(View v) {
			    AlertDialog.Builder popupBuilder = new AlertDialog.Builder(context);
				final AlertDialog popup = popupBuilder.create();
				popup.setTitle("Task Help");
				popup.setMessage("Nothing Special \n None of the above");
				popup.setCancelable(false);
				popup.setButton("OK", new DialogInterface.OnClickListener(){
					@Override
					public void onClick(DialogInterface dialog, int which) {
						popup.dismiss();
					}			
				});
				popup.show();
			}
		});
	}
	
	private void broadcast(){
		Intent intent = new Intent();
		intent.setAction("taskChosen");
		intent.putExtra("type",type);
		intent.putExtra("task",task);
		intent.putExtra("activity","");
		intent.putExtra("timestamp",timestamp);
		sendBroadcast(intent);
		TaskChoice.this.finish();
	}
	
	//gets chosen option and broadcasts for plugin
	private void getChoice(){
		RadioGroup taskOptions = (RadioGroup) findViewById(R.id.taskRadio);
		int chosenID = taskOptions.getCheckedRadioButtonId();
		RadioButton chosen = (RadioButton) findViewById(chosenID);
		task = (String) chosen.getText();
		//ask next question & get activity details
		Intent activity = new Intent();
		activity.setClassName("com.aware.plugin.data_collection", "com.aware.plugin.data_collection.ActivityChoice");
		activity.setFlags(Intent.FLAG_ACTIVITY_NEW_TASK);
		activity.putExtra("type",type);
		activity.putExtra("task",task);;
		activity.putExtra("timestamp",timestamp);
		startActivity(activity);
		timer.cancel();
		TaskChoice.this.finish();
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
			Log.v(TAG, "TaskChoice timeout");
	    	//broadcast();
			Intent intent = new Intent();
			intent.setAction("timeout");
			intent.putExtra("type",type);
			sendBroadcast(intent);
			Log.v(TAG,"timeout broadcast sent");
		}
		
	}
}
