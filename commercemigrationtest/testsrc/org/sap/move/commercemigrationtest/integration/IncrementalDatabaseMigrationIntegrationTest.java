package org.sap.move.commercemigrationtest.integration;

import com.google.common.base.Joiner;
import de.hybris.bootstrap.annotations.IntegrationTest;
import org.junit.Test;
import org.sap.commercemigration.MarkersQueryDefinition;
import org.sap.commercemigration.SeekQueryDefinition;
import org.sap.commercemigration.adapter.DataRepositoryAdapter;
import org.sap.commercemigration.adapter.impl.ContextualDataRepositoryAdapter;
import org.sap.commercemigration.context.CopyContext;
import org.sap.commercemigration.dataset.DataSet;
import org.sap.commercemigration.provider.CopyItemProvider;
import org.sap.commercemigration.repository.DataRepository;
import org.sap.commercemigration.service.DatabaseMigrationService;
import org.sap.move.commercemigrationtest.AbstractDatabaseMigrationTest;

import javax.annotation.Resource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

@IntegrationTest
public class IncrementalDatabaseMigrationIntegrationTest extends AbstractDatabaseMigrationTest {

    @Resource
    private DatabaseMigrationService databaseMigrationService;
    @Resource(name = "dataCopyItemProvider")
    private CopyItemProvider dataCopyItemProvider;

    @Test
    public void testIncrementalDataMigration() throws Exception {
        String table = "usergroups";
        int modCount = 1;
        int insCount = 9;
        int batchSize = 2;
        int batchCount = (modCount + insCount) / batchSize;
        String checkId = UUID.randomUUID().toString();
        //setup the migration context for incremental
        getMigrationContext().setIncrementalModeEnabled(true);
        getMigrationContext().getIncrementalTables().add(table);
        getMigrationContext().setIncrementalTimestamp(now());
        getMigrationContext().setTruncateEnabled(false);
        getMigrationContext().setReaderBatchSize(batchSize);

        //check that only the table specified in the context will be taken into consideration
        Set<CopyContext.DataCopyItem> dataCopyItems = dataCopyItemProvider.get(getMigrationContext());
        assertThat(1).isEqualTo(dataCopyItems.size());
        String sourceItem = dataCopyItems.stream().findFirst().get().getSourceItem();
        assertThat(sourceItem.toLowerCase()).isEqualTo(table.toLowerCase());

        //generate some random data in the source db
        updateRows(modCount, table, now(), checkId);
        insertRows(insCount, table, now(), checkId);

        //make sure we have a delta in source and target table
        long sourceRowCountBefore = getMigrationContext().getDataSourceRepository().getRowCount(table);
        long targetRowCountBefore = getMigrationContext().getDataTargetRepository().getRowCount(table);

        assertThat(sourceRowCountBefore - targetRowCountBefore).isEqualTo(insCount);

        //check that the batching logic works properly for incremental
        DataRepositoryAdapter dataRepositoryAdapter = new ContextualDataRepositoryAdapter(getMigrationContext().getDataSourceRepository());

        DataSet markers = dataRepositoryAdapter.getBatchMarkersOrderedByColumn(getMigrationContext(), createMarkersQueryDefinition(table, "PK", batchSize));
        List<List<Object>> markerResults = markers.getAllResults();
        assertThat(markerResults.size()).isEqualTo(batchCount);

        int rowsToCopy = 0;
        for (List<Object> markerResult : markerResults) {
            DataSet batch = dataRepositoryAdapter.getBatchOrderedByColumn(getMigrationContext(), createSeekQueryDefinition(table, "PK", markerResult.get(0), batchSize));
            List<List<Object>> allResults = batch.getAllResults();
            for (List<Object> row : allResults) {
                Object p_name = batch.getColumnValue("p_name", row);
                Object modifiedTS = batch.getColumnValue("modifiedTS", row);
                assertThat(p_name).isEqualTo(checkId);
                assertThat(modifiedTS).isInstanceOf(Timestamp.class);
                Timestamp ts = (Timestamp) modifiedTS;
                assertThat(ts.toInstant().isAfter(getMigrationContext().getIncrementalTimestamp())).isTrue();
                rowsToCopy++;
            }
        }
        assertThat(rowsToCopy).isEqualTo(modCount + insCount);


        //start the actual migration and make sure the "delta" was copied over correctly
        startMigrationAndWaitForFinish(databaseMigrationService);
        for (CopyContext.DataCopyItem copyItem : dataCopyItems) {
            long sourceRowCount = getMigrationContext().getDataSourceRepository().getRowCount(copyItem.getSourceItem());
            long targetRowCount = getMigrationContext().getDataTargetRepository().getRowCount(copyItem.getTargetItem());
            assertThat(sourceRowCount).isEqualTo(targetRowCount);
            long detectedChanges = checkChanges(checkId, copyItem.getTargetItem());
            assertThat(detectedChanges).isEqualTo(modCount + insCount);
        }

        //cleanup for further test iterations
        cleanupInserts(getMigrationContext().getDataSourceRepository(), insCount, table);
        cleanupInserts(getMigrationContext().getDataTargetRepository(), insCount, table);
        long sourceRowCountCleanup = getMigrationContext().getDataSourceRepository().getRowCount(table);
        long targetRowCountCleanup = getMigrationContext().getDataTargetRepository().getRowCount(table);
        assertThat(sourceRowCountCleanup).isEqualTo(targetRowCountCleanup);
    }

    private SeekQueryDefinition createSeekQueryDefinition(String table, String column, Object lastValue, int batchSize) {
        SeekQueryDefinition queryDefinition = new SeekQueryDefinition();
        queryDefinition.setTable(table);
        queryDefinition.setColumn(column);
        queryDefinition.setLastColumnValue(lastValue);
        queryDefinition.setBatchSize(batchSize);
        return queryDefinition;
    }

    private MarkersQueryDefinition createMarkersQueryDefinition(String table, String column, int batchSize) {
        MarkersQueryDefinition queryDefinition = new MarkersQueryDefinition();
        queryDefinition.setTable(table);
        queryDefinition.setColumn(column);
        queryDefinition.setBatchSize(batchSize);
        return queryDefinition;
    }

    private Instant now() {
        return LocalDateTime.now().toInstant(ZoneOffset.UTC);
    }

    private long checkChanges(String checkId, String table) throws Exception {
        try (Connection connection = getMigrationContext().getDataTargetRepository().getConnection();
             Statement stmt = connection.createStatement();
             ResultSet resultSet = stmt.executeQuery(String.format("select count(*) from %s where p_name='%s'", table, checkId))
        ) {
            long value = 0;
            if (resultSet.next()) {
                value = resultSet.getLong(1);
            }
            return value;
        }
    }

    private void updateRows(int count, String table, Instant instant, String checkId) throws Exception {
        String sql = String.format("UPDATE %s SET modifiedts = ?, p_name = ? where pk in (%s)", table, Joiner.on(",").join(getPKs(count, table)));
        try (Connection connection = getMigrationContext().getDataSourceRepository().getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ) {
            preparedStatement.setTimestamp(1, Timestamp.from(instant));
            preparedStatement.setString(2, checkId);
            preparedStatement.executeUpdate();
            if (!connection.getAutoCommit()) {
                connection.commit();
            }
        }
    }

    private void insertRows(int count, String table, Instant instant, String checkId) throws Exception {
        String sql = String.format("INSERT INTO %s (createdTS,modifiedTS,pk,p_name,p_uid) VALUES (?,?,?,?,?)", table, table);
        try (Connection connection = getMigrationContext().getDataSourceRepository().getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ) {
            for (int i = 0; i < count; i++) {
                preparedStatement.setTimestamp(1, Timestamp.from(instant));
                preparedStatement.setTimestamp(2, Timestamp.from(instant));
                preparedStatement.setString(3, String.valueOf(i));
                preparedStatement.setString(4, checkId);
                preparedStatement.setString(5, String.valueOf(i));
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
            if (!connection.getAutoCommit()) {
                connection.commit();
            }
        }
    }

    private void cleanupInserts(DataRepository repository, int count, String table) throws Exception {
        String sql = String.format("DELETE FROM %s WHERE pk=?", table);
        try (Connection connection = repository.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ) {
            for (int i = 0; i < count; i++) {
                preparedStatement.setString(1, String.valueOf(i));
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
            if (!connection.getAutoCommit()) {
                connection.commit();
            }
        }
    }

    private Set<String> getPKs(int count, String table) throws Exception {
        String sql = String.format("SELECT PK from %s", table);
        try (Connection connection = getMigrationContext().getDataSourceRepository().getConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql);
        ) {
            Set<String> pks = new HashSet<>();
            while (resultSet.next()) {
                pks.add(resultSet.getString(1));
            }
            return pks.stream().sorted(Comparator.reverseOrder()).limit(count).collect(Collectors.toSet());
        }
    }

}