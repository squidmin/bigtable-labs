package testutil;


import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.admin.v2.models.Table;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.StringUtils;
import org.squidmin.client.BigtableClient;
import org.squidmin.config.BigtableClientManager;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.util.AssertionErrors.fail;


@Slf4j
public class BigtableTestUtil {

    public static void setUp(
        BigtableClientManager btConfig,
        String tableId,
        BigtableTableAdminClient tableAdminClient,
        BigtableDataClient dataClient,
        BigtableClient btClient) {
        when(btConfig.getTableAdminClient()).thenReturn(tableAdminClient);
        when(btConfig.getTableAdminClient().exists(anyString())).thenReturn(false);
        Table table = mock(Table.class);
        when(table.getId()).thenReturn(tableId);
        when(btConfig.getTableAdminClient().createTable(any(CreateTableRequest.class))).thenReturn(table);
        when(btConfig.getDataClient()).thenReturn(dataClient);

        btClient.createTable(tableId);
    }


    public static void compareRows(List<Row> expectedRows, List<Row> actualRows, String operator) {
        if (actualRows.size() != expectedRows.size()) {
            fail("Row lengths aren't equal.");
        } else if (StringUtils.equals("==", operator)) {
            assertEqualRows(expectedRows, actualRows);
        } else if (StringUtils.equals("!=", operator)) {
            assertNonEqualRows(expectedRows, actualRows);
        } else {
            fail("Invalid comparison operator.");
        }
    }


    private static void assertEqualRows(List<Row> expectedRows, List<Row> actualRows) {
        String assertionFailReason = "Expected equal rows";
        for (int rowIndex = 0; rowIndex < actualRows.size(); rowIndex++) {
            Row expectedRow = expectedRows.get(rowIndex);
            Row actualRow = actualRows.get(rowIndex);
            List<RowCell> expectedRowCells = expectedRow.getCells();
            List<RowCell> actualRowCells = actualRow.getCells();
            for (int cellIndex = 0; cellIndex < actualRowCells.size(); cellIndex++) {
                RowCell expectedRowCell = expectedRowCells.get(cellIndex);
                RowCell actualRowCell = actualRowCells.get(cellIndex);
                assertEquals(
                    assertionFailReason,
                    expectedRowCell.getFamily(),
                    actualRowCell.getFamily()
                );
                assertEquals(
                    assertionFailReason,
                    expectedRowCell.getQualifier().toStringUtf8(),
                    actualRowCell.getQualifier().toStringUtf8()
                );
                assertEquals(
                    assertionFailReason,
                    expectedRowCell.getValue().toStringUtf8(),
                    actualRowCell.getValue().toStringUtf8()
                );
            }
        }
        log.info("Rows are equal.");
    }


    private static void assertNonEqualRows(List<Row> expectedRows, List<Row> actualRows) {
        String assertionFailReason = "Expected non-equal rows.";
        for (int rowIndex = 0; rowIndex < actualRows.size(); rowIndex++) {
            Row expectedRow = expectedRows.get(rowIndex);
            Row actualRow = actualRows.get(rowIndex);
            List<RowCell> expectedRowCells = expectedRow.getCells();
            List<RowCell> actualRowCells = actualRow.getCells();
            for (int cellIndex = 0; cellIndex < actualRowCells.size(); cellIndex++) {
                RowCell expectedRowCell = expectedRowCells.get(cellIndex);
                RowCell actualRowCell = actualRowCells.get(cellIndex);
                assertEquals(
                    assertionFailReason,
                    expectedRowCell.getFamily(),
                    actualRowCell.getFamily()
                );
                assertEquals(
                    assertionFailReason,
                    expectedRowCell.getQualifier().toStringUtf8(),
                    actualRowCell.getQualifier().toStringUtf8()
                );
                assertNotEquals(
                    assertionFailReason,
                    expectedRowCell.getValue().toStringUtf8(),
                    actualRowCell.getValue().toStringUtf8()
                );
            }
        }
        log.info("Rows are non-equal.");
    }

}
