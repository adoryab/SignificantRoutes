package com.aware.plugin.data_collection;

/**
 * Labels and answers to be stored for DataCollection
 */

public class Labels {
	
	private String trigger;
	private String label;
	private String answer;
	private String cellId;
	private int labelId;
	private int rowId;
	private String datetime = "";

	private static int count = 1;
	
	public Labels(){
	}
	
	public Labels(String cellId, int labelId, String trigger, String label, String answer){
		this.cellId = cellId;
		this.labelId = labelId;
		this.trigger = trigger;
		this.label = label;
		this.answer = answer;
		this.rowId = count++;
	}
	
	public String getCellId() {
		return this.cellId;
	}

	public void setCellId(String cellId) {
		this.cellId = cellId;
	}
	
	public int getLabelId() {
		return this.labelId;
	}

	public void setLabelId(int labelId) {
		this.labelId = labelId;
	}
	
	public String getTrigger() {
		return trigger;
	}

	public void setTrigger(String trigger) {
		this.trigger = trigger;
	}
	
	public String getLabel() {
		return label;
	}

	public void setLabel(String label) {
		this.label = label;
	}

	public String getAnswer() {
		return answer;
	}

	public void setAnswer(String answer) {
		this.answer = answer;
	}
	
	public String getDateTime() {
		return datetime;
	}
	
	public void setDateTime(String dt) {
		this.datetime = dt;
	}
	
	public int getRowId() {
		return rowId;
	}

	public void setRowId(int rowId) {
		this.rowId = rowId;
	}
	
	public String toString(){
		String value = "CellId: " + this.cellId + "LabelId: " 
				+ Integer.toString(this.labelId) + " Trigger: " + this.trigger 
				+ " Label: " + this.label + " Answer: " + this.answer;
		return value;
	}
}