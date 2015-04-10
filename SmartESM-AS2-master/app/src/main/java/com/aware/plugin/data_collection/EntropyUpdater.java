package com.aware.plugin.data_collection;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import android.app.Activity;
import android.content.SharedPreferences;
import android.os.AsyncTask;
import android.os.Bundle;
import android.os.Environment;
import android.util.Log;
import com.loopj.android.http.AsyncHttpClient;
import com.loopj.android.http.AsyncHttpResponseHandler;
import com.loopj.android.http.RequestParams;

public class EntropyUpdater extends Activity {

	private String TAG = "EntropyUpdater";
	private String deviceId = "";
	private String server = "http://epiwork.hcii.cs.cmu.edu/smartesm/mlEngine.php?userID=";
	private int OUTPUT_BUFF = 4096;
	private String path = Environment.getExternalStorageDirectory() + "/AWARE/entropy.mod";
	private String file = "http://epiwork.hcii.cs.cmu.edu/smartesm/";
	private ScheduledExecutorService exec;

	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);

		Log.v(TAG,"updating server");

		SharedPreferences sp = getSharedPreferences("deviceID", MODE_PRIVATE);
		deviceId = sp.getString("deviceId","unknown");
		Log.e(TAG,"deviceID: "+deviceId);
		server+=deviceId;

		syncSQLiteMySQLDB();

		exitActivity();
	}

	private void exitActivity(){
		EntropyUpdater.this.finish();
	}

	private void getFile(){
		exec = Executors.newScheduledThreadPool(1);
		exec.scheduleWithFixedDelay(new Runnable(){
			@Override
			public void run() {
				final DownloadTask downloadTask = new DownloadTask();
				downloadTask.execute();
			}
		},0,2000,TimeUnit.MILLISECONDS);
	}

	public void syncSQLiteMySQLDB(){
		AsyncHttpClient client = new AsyncHttpClient();
		RequestParams params = new RequestParams();
		Log.i(TAG,"updating now");
		params = null;
		client.post(server,params,new AsyncHttpResponseHandler() {
			@Override
			public void onSuccess(String response) {
				Log.i(TAG,"res: "+response);
				getFile();
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

	public void updateEntropyData(){
		exec.shutdownNow();
		try {
			File filepath = new File(path);
			InputStream is = new FileInputStream(filepath);
			InputStreamReader iread = new InputStreamReader(is);
			BufferedReader buffin = new BufferedReader(iread);
			StringBuilder sb = new StringBuilder();
			String line = null;
			while ((line = buffin.readLine()) != null) {
				sb.append(line);
			}
			buffin.close();
			String str = sb.toString();
			JSONObject obj = new JSONObject(str);
			JSONArray stds = obj.getJSONArray("stds");
			JSONArray centroids = obj.getJSONArray("centroids");
			float[] stdsArr = new float[stds.length()];
			for(int i=0;i<stds.length();i++){
				stdsArr[i] = stds.getInt(i);
			}
			Log.i(TAG,"len: "+stds.length());
			Log.i(TAG,"arr: "+stdsArr.toString());
			int rows = centroids.length();
			int cols = centroids.getJSONArray(0).length();
			float[][] centroidsArr = new float[rows][cols];
			for(int i=0;i<rows;i++){
				for(int j=0;j<cols;j++){
					centroidsArr[i][j] = centroids.getJSONArray(i).getInt(j);
				}
			}
			Log.i(TAG,"rows: "+rows+" cols: "+cols);
			Log.i(TAG,"arr: "+centroidsArr.toString());
			//TODO: do something with arrays
		} catch (FileNotFoundException e) {
			Log.e(TAG,"no entropy file");
			e.printStackTrace();
		} catch (IOException e) {
			Log.e(TAG,"erro reading file");
			e.printStackTrace();
		} catch (JSONException e) {
			Log.e(TAG,"JSON exception");
			e.printStackTrace();
		}
	}

	private class DownloadTask extends AsyncTask<String, Integer, String> {

		public DownloadTask() {
			file+=deviceId;
			file+=".mod";
			Log.i(TAG,file);
		}

		@Override
		protected String doInBackground(String... params) {
			InputStream input = null;
			OutputStream output = null;
			HttpURLConnection conn = null;
			try {
				URL url = new URL(file);
				conn = (HttpURLConnection) url.openConnection();
				conn.connect();
				int fileSize = conn.getContentLength();
				Log.i(TAG,"fileSize: "+fileSize);
				// expect HTTP 200 OK, so we don't mistakenly save error report
				// instead of the file
				if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
					Log.e(TAG,"Server returned HTTP " + conn.getResponseCode()
							+ " " + conn.getResponseMessage());
					return "error";
				}
				Log.e(TAG,"Non-error: Server returned HTTP " + conn.getResponseCode()
						+ " " + conn.getResponseMessage());
				// download the file
				input = conn.getInputStream();
				output = new FileOutputStream(path);
				byte[] data = new byte[OUTPUT_BUFF];
				long total = 0;
				int count;
				while ((count = input.read(data)) != -1) {
					total += count;
					output.write(data, 0, count);
				}
				Log.i(TAG,"total: "+total);
				updateEntropyData();
			} catch (Exception e) {
				Log.e(TAG,e.toString());
				return e.toString();
			} finally {
				try {
					if (output != null)
						output.close();
					if (input != null)
						input.close();
				} catch (IOException ignored) {
				}

				if (conn != null)
					conn.disconnect();
			}
			return null;
		}

	}

}
