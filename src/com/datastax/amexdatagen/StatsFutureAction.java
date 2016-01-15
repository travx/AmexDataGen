package com.datastax.amexdatagen;

import java.util.concurrent.atomic.AtomicLong;

import com.datastax.driver.core.ResultSet;

public class StatsFutureAction {
	private StatsManager statistics;
	private AtomicLong numInserted;
	private final long period = 100000;
	
	public StatsFutureAction(){
		statistics = new StatsManager();
		numInserted = new AtomicLong(0);
	}
	
    public void onSuccess(ResultSet rs, String line, long startTimeMillis){
    	statistics.logStats(line, System.currentTimeMillis() - startTimeMillis);
    	
	    long cur = numInserted.incrementAndGet();
	    if (0 == (cur % period)) {
	    	printStats(cur);
	    }
    }
    
    public void printStats(){
    	printStats(numInserted.get());
    }
    
    public void printStats(long cur){
    	System.out.println("Total Operations Performed:  " + cur);
    	statistics.print();
    }
    
    public void onFailure(Throwable t, String line){
	    System.out.println("Error inserting: " + t.getMessage());
	    System.out.println(line);
	    t.printStackTrace(System.out);    	
    }
    
    public void onTooManyFailures(){
    	 System.out.println("Too many INSERT errors ... Stopping");
    }
}
