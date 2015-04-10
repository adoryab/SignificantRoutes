package com.aware.plugin.data_collection;

import android.app.Activity;
import android.app.AlertDialog;
import android.content.DialogInterface;
import android.content.Intent;
import android.os.Bundle;
import android.os.CountDownTimer;
import android.util.Log;
import android.view.View;
import android.widget.Button;

public class PreScreen extends Activity {
	private String TAG = "DataCollection::PreScreen";
	private int index = 0;
	private String type,data,trigger;
	private CountDownTimer timer;

	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);

		Log.v(TAG, "yet to display the alert dialog");

		setContentView(R.layout.prescreen);
		
		this.setFinishOnTouchOutside(false);

		type = getIntent().getExtras().getString("type");
		data = getIntent().getExtras().getString("data");
		
		if(data != null){
			trigger = type+" "+data;
		}
		else{
			trigger = type;
		}

		Button now = (Button) findViewById(R.id.button_now);
		Button later = (Button) findViewById(R.id.button_later);
		Button cancel = (Button) findViewById(R.id.button_cancel);
		Button busy = (Button) findViewById(R.id.button_busy);

		timer = new TimeOut(120000,1000).start();

		cancel.setOnClickListener(new View.OnClickListener() {	
			@Override
			public void onClick(View v) {
				Log.v(TAG, "Cancel pressed");
				Intent intent = new Intent();
				intent.setAction("answerCancel");
				intent.putExtra("type",trigger);
				sendBroadcast(intent);
				Log.v(TAG,"cancel broadcast sent");
				timer.cancel();
				PreScreen.this.finish();
			}
		});

		busy.setOnClickListener(new View.OnClickListener() {
			@Override
			public void onClick(View v) {
				Log.v(TAG, "Busy pressed");
				Intent intent = new Intent();
				intent.setAction("answerBusy");
				intent.putExtra("type",trigger);
				sendBroadcast(intent);
				Log.v(TAG,"busy broadcast sent");
				timer.cancel();
				PreScreen.this.finish();
			}
		});

		later.setOnClickListener(new View.OnClickListener() {	
			@Override
			public void onClick(View v) {
				Log.v(TAG, "Answer later");
				// display a dialog to answer later and save the information in database
				setOptionsDialog();
			}
		});

		now.setOnClickListener(new View.OnClickListener() {	
			@Override
			public void onClick(View v) {
				Log.v(TAG, "Going to answer now");
				// this is the normal scenario
				Intent intent = new Intent();
				intent.setAction("answerNow");
				intent.putExtra("type",trigger);
				Log.v(TAG, "trigger is "+trigger);
				sendBroadcast(intent);
				timer.cancel();
				PreScreen.this.finish();
			}
		});

	}

	public void help(View v){
		Intent helpScreen = new Intent();
		helpScreen.setClassName("com.aware.plugin.data_collection", "com.aware.plugin.data_collection.HelpScreen");
		helpScreen.setFlags(Intent.FLAG_ACTIVITY_NEW_TASK);
		startActivity(helpScreen);
	}

	public void setOptionsDialog(){
		final CharSequence[] items = {"Five minutes", "Ten minutes"};

		AlertDialog.Builder builder = new AlertDialog.Builder(this);
		builder.setTitle("When do you want to be reminded again?");
		builder.setSingleChoiceItems(items, -1,
				new DialogInterface.OnClickListener() {
			@Override
			public void onClick(DialogInterface dialog, int indexSelected){
				if (indexSelected == 0){
					index = 0;
					Log.v(TAG, "User has selected five minutes");
				}
				else if (indexSelected == 1){
					index = 1;
					Log.v(TAG, "User has selected ten minutes");
				}
			}
		})
		// what if the dialog disappears?
		.setOnCancelListener(
				new DialogInterface.OnCancelListener() {
					@Override
					public void onCancel(DialogInterface dialog) {
						finish();
					}
				})
				// OK button action
				.setPositiveButton("OK", new DialogInterface.OnClickListener() {
					@Override
					public void onClick(DialogInterface dialog, int id) {
						Log.v(TAG, "clicked OK! nothing happens yet");

						Intent intent=new Intent();
						intent.setAction("answerLater");
						intent.putExtra("timeout",index);
						intent.putExtra("type",trigger);
						sendBroadcast(intent);
						Log.v(TAG, "later broadcast sent");
						dialog.dismiss();
						timer.cancel();
						PreScreen.this.finish();
					}
				});
		builder.show();
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
			Log.v(TAG, "PreScreen timeout");
			Intent intent = new Intent();
			intent.setAction("timeout");
			intent.putExtra("type",trigger);
			sendBroadcast(intent);
			Log.v(TAG,"timeout broadcast sent");
			PreScreen.this.finish();
		}

	}

	@Override
	protected void onDestroy() {
		super.onDestroy();
	}
}