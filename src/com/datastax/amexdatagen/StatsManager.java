package com.datastax.amexdatagen;

import java.util.HashMap;

public class StatsManager {
	private HashMap<String, ExecutionStats> statsEntries;
	
	public StatsManager(){
		statsEntries = new HashMap<String, ExecutionStats>();
	}
	
	public void print(){
		System.out.println("statsName" + "\t" + "operationCount" + "\t" + "totalDuration" + "\t" + "minDuration" + "\t" + "maxDuration" + "\t" + "avgDuration");
		for (ExecutionStats e : statsEntries.values()){
			e.print();
		}
	}
	
	public void logStats(String line, long durationMillis){
		ExecutionStats entry = getStats(line);
		entry.updateStats(durationMillis);
	}
	
	private ExecutionStats getStats(String line){
		ExecutionStats entry = statsEntries.get(line);
		return (null==entry) ? newEntry(line) : entry;
	}
	
	private ExecutionStats newEntry(String line){
		ExecutionStats newentry = new ExecutionStats(line);
		statsEntries.put(line, newentry);
		return newentry;
	}
}
