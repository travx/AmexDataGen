package com.datastax.amexdatagen;

import com.datastax.driver.core.ResultSetFuture;

public interface FutureManager {
    public boolean add(ResultSetFuture future, String line);

    public boolean cleanup();

    public long getNumInserted();
}
