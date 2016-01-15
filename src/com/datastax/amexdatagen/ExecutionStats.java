package com.datastax.amexdatagen;

import java.util.concurrent.atomic.AtomicLong;

public class ExecutionStats {
	private AtomicLong operationCount;
	private long minDuration;
	private long maxDuration;
	private AtomicLong totalDuration;
	
	private String statsName;
	
	public ExecutionStats(String line){
		statsName = line;
		maxDuration=0;
		minDuration=0;
		operationCount = new AtomicLong();
		totalDuration = new AtomicLong();
	}
	
	public void updateStats(long durationMillis){
		operationCount.incrementAndGet();
		totalDuration.addAndGet(durationMillis);

		if (durationMillis < minDuration) minDuration = durationMillis;
		if (durationMillis > maxDuration) maxDuration = durationMillis;
	}
	
	public void print(){
		System.out.printf("%30s  %30d  %30d  %30d  %30d  %30.2f %n", statsName, operationCount.get(), totalDuration.get(), minDuration, maxDuration, this.getAvgDuration());
	}
	
	public String getName(){
		return statsName;
	}
	
	public double getAvgDuration(){
		return totalDuration.doubleValue()/operationCount.doubleValue();
	}
	
	public long getOperationCount() {
		return operationCount.get();
	}
	public void setOperationCount(long operationCount) {
		this.operationCount.set(operationCount);
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
		return totalDuration.get();
	}
	public void setTotalDuration(long totalDuration) {
		this.totalDuration.set(totalDuration);
	}
	
}
