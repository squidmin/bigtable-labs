package org.squidmin.util;

import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.data.v2.models.*;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import lombok.extern.slf4j.Slf4j;
import org.squidmin.config.BigtableClientManager;
import org.squidmin.exception.InvalidSchemaException;
import org.squidmin.model.BigtableRowWrapper;
import org.squidmin.model.DtoExample;
import org.squidmin.model.FamilyToQualifierMapping;

import java.util.*;

@Slf4j
public class BigtableUtil {


    public static void echo(Row row, int paddingAmount) {
        log.info(LogFont.GREEN + "Row key: {}", row.getKey().toStringUtf8() + LogFont.GREEN);
        echoRowCells(row, paddingAmount);
    }


    public static List<Row> echo(ServerStream<Row> rowStream, int paddingAmount) {
        List<Row> rows = new ArrayList<>();
        if (null == rowStream) {
            log.info("No rows found matching query");
        } else {
            for (Row row : rowStream) {
                log.info(LogFont.GREEN + "Row Key:{}" + LogFont.RESET, row.getKey().toStringUtf8());
                echoRowCells(row, paddingAmount);
                rows.add(row);
            }
        }
        return rows;
    }


    private static void echoRowCells(Row row, int paddingAmount) {
        for (RowCell cell : row.getCells()) {
            String familyFormat = String.format("%-20s", "Family: {}");
            String qualifierFormat = String.format("%-20s", "Qualifier: {}");
            String qualifier = cell.getQualifier().toStringUtf8();
            String valueFormat = String.format(
                "%" + (8 + (paddingAmount + 1 - qualifier.length())) + "s", "Value: {}"
            );
            log.info(
                LogFont.CYAN + familyFormat.concat(qualifierFormat).concat(valueFormat) + LogFont.RESET,
                cell.getFamily(),
                qualifier,
                cell.getValue().toStringUtf8()
            );
        }
        log.info("\n");
    }


    public static RowMutation getRowMutation(
        String tableId,
        String rowKey,
        String columnFamily,
        Set<String> qualifiers,
        List<String> values) throws InvalidSchemaException {

        Iterator<String> valuesIterator = values.iterator();
        Iterator<String> qualifiersIterator = qualifiers.iterator();

        RowMutation rowMutation;
        if (qualifiers.size() == values.size()) {
            rowMutation = RowMutation.create(tableId, rowKey);
            while (qualifiersIterator.hasNext()) {
                rowMutation.setCell(
                    columnFamily,
                    ByteString.copyFrom(qualifiersIterator.next().getBytes()),
                    System.currentTimeMillis() * 1000,
                    ByteString.copyFromUtf8(valuesIterator.next())
                );
            }
        } else {
            log.error(
                "\nqualifiers and values should be 1 : 1."
                  + "\nqualifiers: " + qualifiers
                  + "\nvalues: " + values
            );
            throw new InvalidSchemaException("Invalid row mutation.");
        }

        return rowMutation;
    }


    public static BigtableRowWrapper getLatestRow(List<Row> rows) {
        if (0 == rows.size()) {
            log.info("Calling BigtableUtil.getLatestRow() with 0 rows.");
            return null;
        } else if (1 == rows.size()) {
            log.info("Calling BigtableUtil.getLatestRow() with 1 row. Returning first row.");
            return BigtableRowWrapper.builder()
                .index(0)
                .value(rows.get(0))
                .build();
        } else {
            List<Long> timestamps = Lists.newArrayList();
            int latestTimestampIndex = 0;
            for (int rowIndex = 0; rowIndex < rows.size(); rowIndex++) {
                List<RowCell> rowCells = rows.get(rowIndex).getCells();
                timestamps.add(rowCells.get(0).getTimestamp());
                for (int cellIndex = 1; cellIndex < rowCells.size(); cellIndex++) {
                    Long timestamp = rowCells.get(cellIndex).getTimestamp();
                    if (timestamps.get(rowIndex) < timestamp) {
                        timestamps.set(rowIndex, timestamp);
                    }
                }
                if (2 <=  timestamps.size() && timestamps.get(rowIndex) > timestamps.get(rowIndex - 1)) {
                    latestTimestampIndex = rowIndex;
                }
            }
            return BigtableRowWrapper.builder()
                .index(latestTimestampIndex)
                .value(rows.get(latestTimestampIndex))
                .build();
        }
    }


    public static BulkMutation singleRowAsBulkMutation(
        String tableId,
        BigtableClientManager clientManager,
        DtoExample row) throws InvalidSchemaException {
        long timestamp = System.currentTimeMillis() * 1000;
        FamilyToQualifierMapping ftqMapping = clientManager.getBtStructure().getColumnFamilies().get(0);
        String columnFamilyName = ftqMapping.getColumnFamilyName();
        Set<String> qualifiers = ftqMapping.getQualifierNames();
        Iterator<String> qnIterator = qualifiers.iterator();
        List<String> values = createValuesList(row);
        if (qualifiers.size() == values.size()) {
            BulkMutation bulkMutation = BulkMutation.create(tableId);
            Mutation mutation = Mutation.create();
            int index = 0;
            while (qnIterator.hasNext()) {
                mutation
                    .setCell(
                        columnFamilyName,
                        ByteString.copyFromUtf8(qnIterator.next()),
                        timestamp,
                        ByteString.copyFromUtf8(values.get(index))
                    );
                index++;
            }
            bulkMutation.add(row.getRowKey().toString(), mutation);
        }
        return null;
    }


    public static List<Row> toList(ServerStream<Row> rowStream) {
        Optional<List<Row>> rows;
        if (null == rowStream) {
            log.info("No rows found matching query.");
            rows = Optional.empty();
        } else {
            rows = Optional.of(new ArrayList<>());
            for (Row row : rowStream) {
                rows.get().add(row);
            }
        }
        return rows.orElseGet(Lists::newArrayList);
    }


    public static List<String> createValuesList(DtoExample request) {

        List<String> values = new ArrayList<>();
        values.add(request.getFieldA());
        values.add(request.getFieldB());
        values.add(request.getFieldC());
        values.removeIf(Objects::isNull);
        return values;

    }


}
