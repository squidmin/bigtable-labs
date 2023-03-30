package org.squidmin.client;


import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.admin.v2.models.Table;
import com.google.cloud.bigtable.data.v2.models.BulkMutation;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Filters.Filter;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Component;
import org.squidmin.config.BigtableClientManager;
import org.squidmin.exception.InvalidSchemaException;
import org.squidmin.model.BigtableRowWrapper;
import org.squidmin.model.BigtableStructure;
import org.squidmin.model.DtoExample;
import org.squidmin.model.FamilyToQualifierMapping;
import org.squidmin.util.BigtableUtil;
import org.squidmin.util.LogFont;

import java.util.ArrayList;
import java.util.List;


@EnableConfigurationProperties(value = {BigtableStructure.class})
@Component
@Getter
@Slf4j
public class BigtableClient {


    private final String tableId;
    private final BigtableClientManager clientManager;
    private final BigtableStructure btStructure;
    private final int rowEchoPadding;


    @Autowired
    public BigtableClient(@Value("${bigtable.table-id}") String tableId, BigtableClientManager clientManager) {
        this.tableId = tableId;
        this.clientManager = clientManager;
        this.btStructure = clientManager.getBtStructure();
        rowEchoPadding = clientManager.getMaxQualifierLength();
    }

    public Table createTable() {
        return createTable();
    }

    public Table createTable(String tableId) {
        if (!clientManager.getTableAdminClient().exists(tableId)) {
            log.info("Table does not exist, creating table: {}", tableId);
            CreateTableRequest createTableRequest = CreateTableRequest.of(tableId);

            for (FamilyToQualifierMapping mapping : btStructure.getColumnFamilies()) {
                createTableRequest.addFamily(mapping.getColumnFamilyName());
            }
            Table table = clientManager.getTableAdminClient().createTable(createTableRequest);
            log.info("Table: {} created successfully");
            return table;
        } else {
            log.error("Table: {} already exists\n", tableId);
            return null;
        }
    }

    public void write(String rowKey, RowMutation rowMutation) {
        clientManager.getDataClient().mutateRow(rowMutation);
        log.info("Successfully wrote row {}", rowKey);
    }

    public void writeSingleRow(DtoExample request) throws InvalidSchemaException {
        BulkMutation mutation = BigtableUtil.singleRowAsBulkMutation(tableId, clientManager, request);
        clientManager.getDataClient().bulkMutateRows(mutation);
    }

    public Row readByRowKey(String rowKey) {
        return readByRowKey(tableId, rowKey);
    }

    public Row readByRowKey(String tableId, String rowKey) {
        Row row = clientManager.getDataClient().readRow(tableId, rowKey);
        if (null != row) {
            BigtableUtil.echo(row, rowEchoPadding);
        } else {
            log.error("Row does not exist: {}", rowKey);
        }
        return row;
    }

    public List<Row> readFilter(Filter filter) {
        return readFilter(tableId, filter);
    }

    public List<Row> readFilter(String tableId, Filter filter) {
        try {
            log.info("Reading row using filter:\n{}", filter.toProto().toString());
            Query query = Query.create(tableId).filter(filter);
            ServerStream<Row> serverStream = clientManager.getDataClient().readRows(query);
            return BigtableUtil.echo(serverStream, rowEchoPadding);
        } catch (NotFoundException e) {
            log.error(e.getMessage());
            return null;
        }
    }

    public BigtableRowWrapper readFilterLatest(String tableId, Filter filter) {
        try {
            log.info("Reading latest row using rowkey filter: {}", filter.toProto().toString());
            Query query = Query.create(tableId).filter(filter);
            ServerStream<Row> serverStream = clientManager.getDataClient().readRows(query);
            List<Row> rows = BigtableUtil.toList(serverStream);
            if (rows.size() > 1) {
                log.info("Multiple rows matched the query. Returning the latest row.");
            }
            BigtableRowWrapper latestRowWrapper = BigtableUtil.getLatestRow(rows);
            if (null != latestRowWrapper && null != latestRowWrapper.getValue()) {
                log.info("Found latest row.");
                BigtableUtil.echo(latestRowWrapper.getValue(), rowEchoPadding);
                return latestRowWrapper;
            } else {
                return null;
            }
        } catch (NotFoundException e) {
            log.error(e.getMessage());
            return null;
        }
    }

    public ServerStream<Row> readRows(Query query) {
        return clientManager.getDataClient().readRows(query);
    }

    public List<Row> readTable(String tableId, boolean echoRows) {
        return readTable(tableId, Query.create(tableId), echoRows);
    }

    public List<Row> readTable(String tableId, Query query, boolean echoRows) {
        try {
            log.info(LogFont.BOLD + LogFont.GREEN + "Reading the entire table" + LogFont.RESET, tableId);
            ServerStream<Row> rowStream = clientManager.getDataClient().readRows(query);
            if (echoRows) {
                return BigtableUtil.echo(rowStream, rowEchoPadding);
            } else {
                List<Row> rows = new ArrayList<>();
                rowStream.forEach(rows::add);
                return rows;
            }
        } catch (NotFoundException e) {
            log.error("Failed to read a non-existent table: {}", e.getMessage());
            return null;
        }
    }

    public void close() {
        clientManager.getDataClient().close();
        clientManager.getTableAdminClient().close();
    }


}
