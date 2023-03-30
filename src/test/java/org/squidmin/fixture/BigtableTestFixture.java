package org.squidmin.fixture;

import java.util.UUID;

public abstract class BigtableTestFixture {

    public static final Long timestamp = System.currentTimeMillis() * 1000;

    public static final String uuid = String.valueOf(UUID.randomUUID());

    public static final String rowKeyDelim = "#";

    protected String tableId;
    protected String columnFamily;


    public BigtableTestFixture() {}
    public BigtableTestFixture(String tableId, String columnFamily) {
        this.tableId = tableId;
        this.columnFamily = columnFamily;
    }

}
