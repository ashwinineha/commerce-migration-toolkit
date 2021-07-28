package org.sap.move.commercemigrationtest.integration;

import de.hybris.bootstrap.annotations.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sap.commercemigration.MarkersQueryDefinition;
import org.sap.commercemigration.SeekQueryDefinition;
import org.sap.commercemigration.adapter.impl.ContextualDataRepositoryAdapter;
import org.sap.commercemigration.dataset.DataSet;
import org.sap.commercemigration.repository.DataRepository;
import org.sap.move.commercemigrationtest.AbstractDatabaseMigrationTest;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@IntegrationTest
public class CopyStrategyTest extends AbstractDatabaseMigrationTest {

    private static final String CUSTOM_TABLE = "modresilienttable";
    private static final String PK_COLUMN = "PK";

    @Before
    public void before() {
        // create the custom table in the source DB
        final DataRepository sourceRepository = getMigrationContext().getDataSourceRepository();
        final DataSource sourceDb = sourceRepository.getDataSource();

        final JdbcTemplate jdbcTemplate = new JdbcTemplate(sourceDb);
        jdbcTemplate.execute("CREATE TABLE " + CUSTOM_TABLE + "(PK INT)");
        for (int i = 1; i <= 10; i++) {
            jdbcTemplate.execute("INSERT INTO " + CUSTOM_TABLE + "(PK) VALUES (" + i + ")");
        }
        jdbcTemplate.execute("CREATE INDEX IDX_" + CUSTOM_TABLE + " ON " + CUSTOM_TABLE + "(PK)");

        getMigrationContext().getCustomTables().add(CUSTOM_TABLE);
    }

    @Test
    public void testModificationResilientCopy() throws Exception {
        int batchSize = 2;
        DataRepository dataSourceRepository = getMigrationContext().getDataSourceRepository();
        ContextualDataRepositoryAdapter adapter = new ContextualDataRepositoryAdapter(dataSourceRepository);
        MarkersQueryDefinition markersQueryDefinition = new MarkersQueryDefinition();
        markersQueryDefinition.setTable(CUSTOM_TABLE);
        markersQueryDefinition.setColumn(PK_COLUMN);
        markersQueryDefinition.setBatchSize(batchSize);
        DataSet batchMarkersOrderedByColumn = adapter.getBatchMarkersOrderedByColumn(getMigrationContext(), markersQueryDefinition);
        List<List<Object>> batchMarkersList = batchMarkersOrderedByColumn.getAllResults();
        assertThat(batchMarkersList.size()).isEqualTo(5);
        for (int i = 0; i < batchMarkersList.size(); i++) {
            Number pk = (Number) batchMarkersList.get(i).get(0);
            assertThat(pk.intValue()).isEqualTo(i * 2 + 1);
        }

        deletePk(5);

        for (int i = 0; i < batchMarkersList.size(); i++) {
            Number lastPk = (Number) batchMarkersList.get(i).get(0);
            Number nextPk = null;
            if (i + 1 < batchMarkersList.size()) {
                nextPk = (Number) batchMarkersList.get(i + 1).get(0);
            }
            SeekQueryDefinition seekQueryDefinition = new SeekQueryDefinition();
            seekQueryDefinition.setBatchSize(batchSize);
            seekQueryDefinition.setTable(CUSTOM_TABLE);
            seekQueryDefinition.setLastColumnValue(lastPk);
            seekQueryDefinition.setNextColumnValue(nextPk);
            seekQueryDefinition.setColumn(PK_COLUMN);
            DataSet batchOrderedByColumn = adapter.getBatchOrderedByColumn(getMigrationContext(), seekQueryDefinition);
            if (lastPk.intValue() != 5) {
                assertThat(batchOrderedByColumn.getAllResults().size()).isEqualTo(2);
            } else {
                assertThat(batchOrderedByColumn.getAllResults().size()).isEqualTo(1);
            }

        }
    }

    private void deletePk(Integer pk) {
        final DataRepository sourceRepository = getMigrationContext().getDataSourceRepository();
        final DataSource sourceDb = sourceRepository.getDataSource();

        final JdbcTemplate jdbcTemplate = new JdbcTemplate(sourceDb);
        jdbcTemplate.execute("DELETE FROM " + CUSTOM_TABLE + " WHERE PK = " + pk);
    }

    @After
    public void after() {
        // cleanup
        final DataRepository sourceRepository = getMigrationContext().getDataSourceRepository();
        final DataSource sourceDb = sourceRepository.getDataSource();

        final JdbcTemplate sourceTemplate = new JdbcTemplate(sourceDb);
        sourceTemplate.execute("DROP TABLE " + CUSTOM_TABLE);

        getMigrationContext().getCustomTables().remove(CUSTOM_TABLE);
    }


}
