package com.aware.plugin.data_collection;

import android.app.Activity;
import android.os.Bundle;
import android.util.Log;
import android.view.View;
import android.widget.Button;

public class HelpScreen extends Activity {
	private String TAG = "DataCollection::HelpScreen";

	@Override
	protected void onCreate(Bundle savedInstanceState) {
		
		super.onCreate(savedInstanceState);
		Log.v(TAG, "help screen");
		
		this.setContentView(R.layout.helpscreen);
		
		Button helpButton = (Button)this.findViewById(R.id.button1);
		
		// button click to proceed forward
		helpButton.setOnClickListener(new View.OnClickListener() {

			public void onClick (View v){
				HelpScreen.this.finish();
			}
		});
	}
	
	protected void onPause(){
		super.onPause();
		finish();
	}

	@Override
	protected void onDestroy() {
		// TODO Auto-generated method stub
		super.onDestroy();
	}
	
}
