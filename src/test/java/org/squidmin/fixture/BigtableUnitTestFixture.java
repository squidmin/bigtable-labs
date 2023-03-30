package org.squidmin.fixture;


import com.google.cloud.bigtable.data.v2.models.*;
import com.google.protobuf.ByteString;

import java.time.Instant;
import java.util.*;

public class BigtableUnitTestFixture extends BigtableTestFixture {


    public static final List<String> rowKeys = buildRowKeys();

    public static class RowFixture {
        static final RowKeyFixture rowkey = RowKeyFixture.builder()
            .foo("bear")
            .bar("capybara")
            .build();
        static final Map<String, String> cordial = new HashMap<>() {{
            put("qualifier", "cordial");
            put("value", "Y");
        }};
        static final Map<String, String> bird = new HashMap<>() {{
            put("qualifier", "bird");
            put("qualifier", "plenty");
        }};
    }

    public static class FilterFixture {
        public static final String matchingFilterString =
            "^" + RowFixture.rowkey.getFoo() + rowKeyDelim + RowFixture.rowkey.getBar() + ".*$";
        public static final String nonMatchingFilterString =
            "^" + "pheaugh" + rowKeyDelim + RowFixture.rowkey.getBar() + rowKeyDelim + ".*$";
    }


    private final String tableId;
    private final String columnFamily;

    public BigtableUnitTestFixture(String tableId, String columnFamily) {
        this.tableId = tableId;
        this.columnFamily = columnFamily;
    }


    public String getTableId() { return tableId; }

    public RowMutation buildRowMutation(String columnFamilyName) {
        return
            RowMutation.create(Objects.requireNonNull(tableId), rowKey())
                .setCell(
                    columnFamilyName,
                    RowFixture.cordial.get("qualifier"),
                    timestamp,
                    RowFixture.cordial.get("value"))
                .setCell(
                    columnFamilyName,
                    RowFixture.bird.get("qualifier"),
                    timestamp,
                    RowFixture.bird.get("value"));
    }

    public static String rowKey() { return rowKeys.get(0); }

    private static List<String> buildRowKeys() {
        String rowKey = RowFixture.rowkey.getFoo() + rowKeyDelim + RowFixture.rowkey.getBar();
        List<String> rowKeys = new ArrayList<>();
        rowKeys.add(rowKey);
        for (int i = 1; i < 6; i++) { rowKeys.add(rowKey + i); }
        return rowKeys;
    }

    public Iterator<Row> rowsIterator() {
        List<Row> rows = new ArrayList<>();
        for (String rowKey : rowKeys) {
            Row row = Row.create(
                ByteString.copyFromUtf8(rowKey),
                expectedRowCells(false)
            );
            rows.add(row);
        }
        return rows.iterator();
    }

    public List<Row> expectedRows() {
        List<Row> rows = new ArrayList<>();
        rowsIterator().forEachRemaining(rows::add);
        return rows;
    }

    public List<RowCell> expectedRowCells(boolean shouldIncludeOldTimestamp) {
        List<RowCell> expectedCells = new ArrayList<>();
        expectedCells.add(
            RowCell.create(
                columnFamily,
                ByteString.copyFromUtf8(RowFixture.cordial.get("qualifier")),
                timestamp,
                new ArrayList<>(),
                ByteString.copyFromUtf8(RowFixture.cordial.get("value"))
            )
        );
        expectedCells.add(
            RowCell.create(
                columnFamily,
                ByteString.copyFromUtf8(RowFixture.bird.get("qualifier")),
                timestamp,
                new ArrayList<>(),
                ByteString.copyFromUtf8(RowFixture.bird.get("value"))
            )
        );
        return expectedCells;
    }


    public BulkMutation bulkMutation(String tableId) {
        return BulkMutation.create(tableId)
            .add(
                RowKeyFixture.builder().foo("fooVal").bar("barVal").build().toString(),
                Mutation.create()
                    .setCell(
                        columnFamily,
                        ByteString.copyFromUtf8(RowFixture.cordial.get("qualifier")),
                        timestamp,
                        ByteString.copyFromUtf8(RowFixture.cordial.get("value"))
                    )
                    .setCell(
                        columnFamily,
                        ByteString.copyFromUtf8(RowFixture.bird.get("qualifier")),
                        timestamp,
                        ByteString.copyFromUtf8(RowFixture.bird.get("value"))
                    )
            );
    }


}
