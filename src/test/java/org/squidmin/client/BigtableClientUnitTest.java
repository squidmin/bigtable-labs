package org.squidmin.client;


import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.squidmin.client.BigtableClient;
import org.squidmin.config.BigtableClientManager;
import org.squidmin.config.UnitTestConfig;
import org.squidmin.fixture.BigtableUnitTestFixture;
import org.squidmin.model.BigtableStructure;
import org.squidmin.model.FamilyToQualifierMapping;
import testutil.BigtableTestUtil;

import java.util.List;

import static com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


@RunWith(SpringRunner.class)
@SpringBootTest(classes = {BigtableClient.class, UnitTestConfig.class})
public class BigtableClientUnitTest {


    @Autowired
    private BigtableTableAdminClient tableAdminClient;

    @Autowired
    private BigtableDataClient dataClient;

    @Autowired
    private BigtableClientManager clientManager;

    @Autowired
    private BigtableStructure btStructure;
    private List<FamilyToQualifierMapping> columnFamilies;

    @Autowired
    private String tableId;
    private int tableIdOffset = 0;


    private BigtableUnitTestFixture fixture;

    private BigtableClient btClient;


    @Before
    public void before() {
        when(clientManager.getBtStructure()).thenReturn(btStructure);
        columnFamilies = btStructure.getColumnFamilies();
        fixture = new BigtableUnitTestFixture(tableId, columnFamilies.get(0).getColumnFamilyName());
        btClient = new BigtableClient(tableId, clientManager);
        BigtableTestUtil.setUp(clientManager, tableId, tableAdminClient, dataClient, btClient);
    }

    @After
    public void after() {
        tableId = tableId + "-" + tableIdOffset++;
        btClient.close();
    }


    @Test
    public void readTable_givenValidWriteData_writeRowAndReadTableAsExpected() {
        ServerStream<Row> rowStream = mock(ServerStream.class);
        when(clientManager.getDataClient().readRows(any(Query.class))).thenReturn(rowStream);
        when(rowStream.iterator()).thenReturn(fixture.rowsIterator());

        List<Row> expectedRows = fixture.expectedRows();
        List<Row> actualRows = btClient.readTable(fixture.getTableId(), true);

        Assertions.assertTrue(0 < actualRows.size());
        BigtableTestUtil.compareRows(expectedRows, actualRows, "==");
    }


    @Test
    public void readTable_givenNotFoundExceptionThrown_returnNull() {
        when(clientManager.getDataClient().readRows(any(Query.class))).thenThrow(NotFoundException.class);
        Assertions.assertNull(btClient.readTable(tableId, true));
    }


    @Test
    public void readByRowKey_givenRowKeyMatch_returnExpectedRow() {
        when(clientManager.getDataClient().readRow(anyString(), anyString()))
            .thenReturn(fixture.expectedRows().get(0));

        Row row = btClient.readByRowKey(tableId, BigtableUnitTestFixture.rowKey());

        Assertions.assertNotNull(row.getKey());
        Assertions.assertNotNull(row.getCells());
        Assertions.assertTrue(0 < row.getCells().size());
        Assertions.assertEquals(fixture.expectedRowCells(false), row.getCells());
    }


    @Test
    public void readFilter_givenFilterMatch_returnRows() {
        ServerStream<Row> rowStream = mock(ServerStream.class);
        when(clientManager.getDataClient().readRows(any(Query.class))).thenReturn(rowStream);
        when(rowStream.iterator()).thenReturn(fixture.rowsIterator());

        Assertions.assertTrue(0 < btClient.readFilter(
            FILTERS.key().regex(BigtableUnitTestFixture.FilterFixture.matchingFilterString)).size()
        );
    }


}
