package org.squidmin.fixture;


import com.google.cloud.bigtable.data.v2.models.RowMutation;
import org.squidmin.model.DtoExample;
import org.squidmin.model.RowKey;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class BigtableIntegrationTestFixture extends BigtableTestFixture {

    public static class RowFixture {
        public static final RowKey rowkey = RowKey.builder()
            .segmentA("test_segmentA")
            .segmentB("test_segmentB")
            .segmentC("test_segmentC")
            .build();
        static final Map<String, String> field_1 = new HashMap<String, String>() {{
            put("qualifier", "field_1");
            put("value", "test_field_1");
        }};
        static final Map<String, String> field_2 = new HashMap<String, String>() {{
            put("qualifier", "field_2");
            put("value", "test_field_2");
        }};
        static final Map<String, String> field_3 = new HashMap<String, String>() {{
            put("qualifier", "field_3");
            put("value", "test_field_3");
        }};
    }

    public static class FilterFixture {

        public static final String matchingFilterString =
            "^" +
                RowFixture.rowkey.getSegmentA() + RowKey.delim +
                RowFixture.rowkey.getSegmentB() + RowKey.delim +
                RowFixture.rowkey.getSegmentC() + RowKey.delim +
                ".*$";

    }


    public BigtableIntegrationTestFixture(String tableId, String columnFamily) {
        super(tableId, columnFamily);
    }

    public String getTableId() { return tableId; }

    public RowMutation buildRowMutation(String columnFamilyName) {
        return
            RowMutation.create(Objects.requireNonNull(tableId), RowFixture.rowkey.toString())
                .setCell(
                    columnFamilyName,
                    RowFixture.field_1.get("qualifier"),
                    timestamp,
                    RowFixture.field_1.get("value"))
                .setCell(
                    columnFamilyName,
                    RowFixture.field_2.get("qualifier"),
                    timestamp,
                    RowFixture.field_2.get("value"))
                .setCell(
                    columnFamilyName,
                    RowFixture.field_3.get("qualifier"),
                    timestamp,
                    RowFixture.field_3.get("value"));
    }


}
