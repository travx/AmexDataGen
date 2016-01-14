package com.datastax.amexdatagen;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;

public class StatsFutureSet extends AbstractFutureManager{
    protected StatsFutureAction futureAction = null;
    protected Semaphore available;
    protected AtomicLong insertErrors;
    protected AtomicLong numInserted;

    public StatsFutureSet(int inSize, long inQueryTimeout, 
			   long inMaxInsertErrors) {
	super(inSize, inQueryTimeout, inMaxInsertErrors);
	futureAction = new StatsFutureAction();
	available = new Semaphore(size, true);
	insertErrors = new AtomicLong(0);
	numInserted = new AtomicLong(0);
    }
    
    public boolean add(ResultSetFuture future, final String line){
    	return this.add(future, line, 0);
    }

    public boolean add(ResultSetFuture future, final String line, long offsetMillis) {
    	final long startTimeMillis = System.currentTimeMillis() - offsetMillis;
		if (maxInsertErrors <= insertErrors.get())
		    return false;
		try {
		    available.acquire();
		}
		catch (InterruptedException e) {
		    return false;
		}
		Futures.addCallback(future, new FutureCallback<ResultSet>() {
			@Override
			public void onSuccess(ResultSet rs) {
			    available.release();
			    numInserted.incrementAndGet();
			    futureAction.onSuccess(rs, line, startTimeMillis);
			}
			@Override
			public void onFailure(Throwable t) {
			    available.release();
			    long numErrors = insertErrors.incrementAndGet();
			    futureAction.onFailure(t, line);
			    if (maxInsertErrors <= numErrors) {
				futureAction.onTooManyFailures();
			    }
			}
		    });
		return true;
	   }

    public boolean cleanup() {
	try {
            available.acquire(this.size);
        } catch (InterruptedException e) {
	    return false;
        }
	return true;
    }

    public long getNumInserted() {
    return numInserted.get();
    }

}
