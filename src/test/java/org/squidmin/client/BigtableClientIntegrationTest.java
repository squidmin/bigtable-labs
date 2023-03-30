package org.squidmin.client;


import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.admin.v2.models.Table;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.squidmin.config.BigtableClientManager;
import org.squidmin.config.IntegrationTestConfig;
import org.squidmin.exception.InvalidSchemaException;
import org.squidmin.fixture.BigtableIntegrationTestFixture;
import org.squidmin.fixture.BigtableUnitTestFixture;
import org.squidmin.model.*;
import org.squidmin.util.BigtableUtil;
import org.squidmin.util.LogFont;

import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;
import static org.junit.jupiter.api.Assertions.assertEquals;


@RunWith(SpringRunner.class)
@SpringBootTest(classes = { BigtableClient.class, IntegrationTestConfig.class })
@ActiveProfiles("integration")
@Slf4j
public class BigtableClientIntegrationTest {

    @Autowired
    private String tableId;

    private List<FamilyToQualifierMapping> columnFamilies;

    @Autowired
    private BigtableClientManager clientManager;

    private BigtableClient btClient;

    private BigtableIntegrationTestFixture fixture;

    private int rowEchoPadding = 0;


    @Before
    public void before() {
        BigtableStructure btStructure = clientManager.getBtStructure();
        columnFamilies = btStructure.getColumnFamilies();

        fixture = new BigtableIntegrationTestFixture(
            tableId,
            btStructure.getColumnFamilies().get(0).getColumnFamilyName()
        );

        btClient = new BigtableClient(tableId, clientManager);
        rowEchoPadding = clientManager.getMaxQualifierLength();
    }

    @After
    public void after() { btClient.close(); }


    @Test
    public void deleteTable() { garbageCollect(); }

    @Test
    public void deleteRows() throws ExecutionException, InterruptedException, TimeoutException {
        String regex = System.getProperty("regex");
        Query query = Query.create(tableId).filter(FILTERS.key().regex("^".concat(regex).concat("$")));
        List<ApiFuture<Void>> futures = Lists.newArrayList();
        ServerStream<Row> serverStream = btClient.readRows(query);
        int numRowsBeforeDeletion = btClient.readTable(tableId, false).size();
        log.info(LogFont.BOLD + LogFont.GREEN + "Number of rows in table before deletion: " + numRowsBeforeDeletion + LogFont.RESET);
        log.info(LogFont.GREEN + "Rows deleted:" + LogFont.RESET);
        int numRowsDeleted = 0;
        for (Row row : serverStream) {
            BigtableUtil.echo(row, rowEchoPadding);
            ApiFuture<Void> future = clientManager.getDataClient().mutateRowAsync(RowMutation.create(tableId, row.getKey()).deleteRow());
            futures.add(future);
            numRowsDeleted++;
        }
        ApiFutures.allAsList(futures).get(10, TimeUnit.MINUTES);
        log.info(LogFont.BOLD + LogFont.GREEN + "Number of rows deleted: " + numRowsDeleted + LogFont.RESET);
        int numRowsAfterDeletion = btClient.readTable(tableId, false).size();
        assertEquals(numRowsDeleted, numRowsBeforeDeletion - numRowsAfterDeletion);
        log.info(LogFont.BOLD + LogFont.GREEN + "Number of rows in table after deletion: " + numRowsAfterDeletion + LogFont.RESET);
    }

    @Test
    public void createTable_givenNonExistentTableId_createTableAsExpected() {
        Table table = btClient.createTable();
        Assertions.assertNotNull(table);
        assertEquals("dto_example", table.getId());
    }

    @Test
    public void createTable_givenExistingTableId_doNotCreateTable_and_returnNull() {
        if (!btClient.getClientManager().getTableAdminClient().exists(tableId)) {
            btClient.createTable(tableId);
        }
        Assertions.assertNull(btClient.createTable(tableId));
    }

    @Test
    public void writeRow() {
        btClient.write(
            BigtableIntegrationTestFixture.RowFixture.rowkey.toString(),
            fixture.buildRowMutation(
                columnFamilies.get(0).getColumnFamilyName()
            )
        );
    }

    @Test
    public void writeSingleRow() throws InvalidSchemaException {

        String fieldA = System.getProperty("fieldA");
        String fieldB = System.getProperty("fieldB");
        String fieldC = System.getProperty("fieldC");

        btClient.writeSingleRow(
            DtoExample.builder()
                .rowKey(
                    RowKey.builder()
                        .segmentA("segmentA")
                        .segmentB("segmentB")
                        .segmentC("segmentC")
                        .build()
                )
                .fieldA(fieldA)
                .fieldB(fieldB)
                .fieldC(fieldC).build()
        );

    }

    @Test
    public void readKey_givenMatchingRowKey_returnRow() {
        btClient.write(
            BigtableIntegrationTestFixture.RowFixture.rowkey.toString(),
            fixture.buildRowMutation(columnFamilies.get(0).getColumnFamilyName())
        );
        Row row = btClient.readByRowKey(BigtableIntegrationTestFixture.RowFixture.rowkey.toString());
        Assertions.assertNotNull(row);
        Assertions.assertTrue(0 < row.getCells().size());
    }

    @Test
    public void readKey_givenNonMatchingRowKey_returnNull() {
        Row row = btClient.readByRowKey(BigtableIntegrationTestFixture.RowFixture.rowkey.toString() + "__invalid");
        Assertions.assertNull(row);
    }

    @Test
    public void readRows() {
        String regex = System.getProperty("regex");
        Query query = Query.create(tableId).filter(FILTERS.key().regex("^".concat(regex).concat("$")));
        ServerStream<Row> rows = btClient.readRows(query);
        int numRowsRead = 0;
        for (Row row : rows) {
            BigtableUtil.echo(row, rowEchoPadding);
            numRowsRead++;
        }
        log.info(LogFont.BOLD + LogFont.GREEN + "Number of rows read: " + numRowsRead + LogFont.RESET);
    }

    @Test
    public void readFilter_givenValidFilterChain_returnRows() {
        Assertions.assertTrue(
            0 < btClient.readFilter(
                FILTERS.chain()
                    .filter(FILTERS.family().exactMatch(columnFamilies.get(0).getColumnFamilyName()))
                    .filter(FILTERS.key().regex(BigtableIntegrationTestFixture.FilterFixture.matchingFilterString))
            ).size()
        );
    }

    @Test
    public void readFilter_givenNonMatchingFilter_returnZeroLengthRows() {
        Assertions.assertEquals(
            0,
            btClient.readFilter(
                tableId,
                FILTERS.key().regex(BigtableUnitTestFixture.FilterFixture.nonMatchingFilterString)
            ).size()
        );
    }

    @Test
    public void readFilterLatest_givenMatchingFilter_returnLatestRow() {

        List<Row> allMatchingRows = btClient.readFilter(
            tableId,
            FILTERS.key().regex(BigtableUnitTestFixture.FilterFixture.matchingFilterString)
        );

        BigtableRowWrapper latestRowWrapper = btClient.readFilterLatest(
            tableId, FILTERS.key().regex(BigtableUnitTestFixture.FilterFixture.matchingFilterString)
        );
        Row latestRow = latestRowWrapper.getValue();

        Assertions.assertTrue(1 < allMatchingRows.size());
        for (int i = 1; i < allMatchingRows.size(); i++) {
            Assertions.assertNotSame(allMatchingRows.get(i), allMatchingRows.get(i - 1));
        }
        Assertions.assertTrue(allMatchingRows.contains(latestRow));
        Assertions.assertEquals(latestRow, allMatchingRows.get(latestRowWrapper.getIndex()));

    }

    @Test
    public void readFilter_given_segmentA_and_segment_B_returnRows() {
        Assertions.assertTrue(
            1 <= btClient.readFilter(
                FILTERS.key().regex(
                    RowKey.builder()
                        .segmentA("test_segmentA")
                        .segmentB("test_segmentB")
                        .build().toString()
                )
            ).size()
        );
    }

    @Test
    public void readFilter_given_segmentA_and_segmentB_returnRows() {
        Assertions.assertTrue(
            1 <= btClient.readFilter(
                FILTERS.key().regex(
                    RowKey.builder()
                        .segmentA("test_segmentA")
                        .segmentB("test_segmentC")
                        .build().toString()
                )
            ).size()
        );
    }

    @Test
    public void readFilter_givenValidTimestampRange_returnRows() {

        LocalDate startDate = LocalDate.of(2022, Month.JANUARY, 1);
        LocalDate endDate = LocalDateTime.ofInstant(
            Instant.ofEpochMilli(BigtableIntegrationTestFixture.timestamp / 1000),
            TimeZone.getDefault().toZoneId()
        ).toLocalDate();

        log.info(LogFont.GREEN + "Querying for rows between dates:\n{} and {}" + LogFont.RESET, startDate, endDate);
        log.info(LogFont.GREEN + "Total days range: {}" + LogFont.RESET, ChronoUnit.DAYS.between(startDate, endDate));

        long start = startDate.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli() * 1000;
        long end = endDate.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli() * 1000;

        Assertions.assertTrue(
            1 <= btClient.readFilter(
                FILTERS.timestamp().range()
                    .startOpen(start)
                    .endClosed(end)
            ).size()
        );

    }

    @Test
    public void readTable() {
        btClient.readTable(tableId, true);
    }

    @Test
    public void countTableRows() {
        int numRows = btClient.readTable(tableId, false).size();
        log.info(LogFont.GREEN + "Number of rows in table: {}" + LogFont.RESET, numRows);
    }

    private void garbageCollect() {
        clientManager.getTableAdminClient().listTables().forEach(tableId -> {
            if (tableId.equalsIgnoreCase(this.tableId)) {
                clientManager.getTableAdminClient().deleteTable(tableId);
            }
        });
        btClient.close();
    }

}
