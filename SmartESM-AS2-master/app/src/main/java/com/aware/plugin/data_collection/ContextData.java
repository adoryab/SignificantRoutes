package com.aware.plugin.data_collection;

/**
 * Context data to be stored for DataCollection
 */

public class ContextData {
	
    private String cellId;
    private int labelId;
    private int wifiStrength;
    private String wifiName;
    private String activity;
    private int batteryStatus;
    private int barometer;
    private int temperature;
    private String location;
    private String accuracy;
    private String tower;
    private int rowId;
    
    private static int count = 1;
    
    public ContextData(){
    }
    
    public ContextData(String cellId, int labelId, int wifiStrength, String wifiName,
    		String activity, int batteryStatus, int barometer, int temperature, 
    		String location, String accuracy, String tower){
    	this.cellId = cellId;
    	this.labelId = labelId;
    	this.wifiStrength = wifiStrength;
    	this.wifiName = wifiName;
    	this.activity = activity;
    	this.batteryStatus = batteryStatus;
    	// the following two can be NULL - most devices don't support these sensors
    	this.barometer = barometer;
    	this.temperature = temperature;
    	this.location = location;
    	this.accuracy = accuracy;
    	this.tower = tower;
    	this.rowId = count++;
    }
    
	public String getCellId() {
		return cellId;
	}
	public void setCellId(String cellId) {
		this.cellId = cellId;
	}
	public int getLabelId() {
		return labelId;
	}
	public void setLabelId(int labelId) {
		this.labelId = labelId;
	}
	public int getWifiStrength() {
		return wifiStrength;
	}
	public void setWifiStrength(int wifiStrength) {
		this.wifiStrength = wifiStrength;
	}
	public String getWifiName() {
		return wifiName;
	}
	public void setWifiName(String wifiName) {
		this.wifiName = wifiName;
	}
	public String getActivity() {
		return activity;
	}
	public void setActivity(String activity) {
		this.activity = activity;
	}
	public int getBatteryStatus() {
		return batteryStatus;
	}
	public void setBatteryStatus(int batteryStatus) {
		this.batteryStatus = batteryStatus;
	}
	public int getBarometer() {
		return barometer;
	}
	public void setBarometer(int barometer) {
		this.barometer = barometer;
	}
	public int getTemperature() {
		return temperature;
	}
	public void setTemperature(int temperature) {
		this.temperature = temperature;
	}
	public String getLocation() {
		return location;
	}
	public void setLocation(String location) {
		this.location = location;
	}
	public String getAccuracy() {
		return accuracy;
	}
	public void setAccuracy(String accuracy) {
		this.accuracy = accuracy;
	}
	public String getTower() {
		return tower;
	}
	public void setTower(String tower) {
		this.tower = tower;
	}
	public int getRowId() {
		return rowId;
	}
	public void setRowId(int rowId) {
		this.rowId = rowId;
	}
	
	public String toString(){ 
		System.out.println("Context Data in string format ");
		
		String value = "Cell: "+this.cellId+" Wifi: "+Integer.toString(this.wifiStrength)+
				" WifiName: "+this.wifiName+" Activity: "+this.activity+" Battery: "
				+Integer.toString(this.batteryStatus)+" Barometer: "
				+Integer.toString(this.barometer)+" Temperature: "
				+Integer.toString(this.temperature)+" Location: "+this.location
				+" Accuracy: "+this.accuracy+" Tower: "+this.tower;
		
		return value;
		
	}
}
