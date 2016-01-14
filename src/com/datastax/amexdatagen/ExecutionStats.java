package com.datastax.amexdatagen;

public class ExecutionStats {
	private long operationCount;
	private long minDuration;
	private long maxDuration;
	private long totalDuration;
	
	private String statsName;
	
	public ExecutionStats(String line){
		statsName = line;
		maxDuration=0;
		minDuration=0;
		operationCount=0;
		totalDuration=0;
	}
	
	public void updateStats(long durationMillis){
		operationCount++;
		totalDuration += durationMillis;
		if (durationMillis < minDuration) minDuration = durationMillis;
		if (durationMillis > maxDuration) maxDuration = durationMillis;
	}
	
	public void print(){
		System.out.println(statsName + "\t" + operationCount + "\t" + totalDuration + "\t" + minDuration + "\t" + maxDuration + "\t" + this.getAvgDuration());
	}
	
	public String getName(){
		return statsName;
	}
	
	public double getAvgDuration(){
		return (double)totalDuration/(double)operationCount;
	}
	
	public long getOperationCount() {
		return operationCount;
	}
	public void setOperationCount(long operationCount) {
		this.operationCount = operationCount;
	}
	public long getMinDuration() {
		return minDuration;
	}
	public void setMinDuration(long minDuration) {
		this.minDuration = minDuration;
	}
	public long getMaxDuration() {
		return maxDuration;
	}
	public void setMaxDuration(long maxDuration) {
		this.maxDuration = maxDuration;
	}
	public long getTotalDuration() {
		return totalDuration;
	}
	public void setTotalDuration(long totalDuration) {
		this.totalDuration = totalDuration;
	}
	
}
