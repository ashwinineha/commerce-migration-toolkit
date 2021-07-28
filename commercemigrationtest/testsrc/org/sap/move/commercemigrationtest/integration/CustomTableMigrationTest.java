package org.sap.move.commercemigrationtest.integration;

import de.hybris.bootstrap.annotations.IntegrationTest;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Index;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sap.commercemigration.context.CopyContext;
import org.sap.commercemigration.context.CopyContext.DataCopyItem;
import org.sap.commercemigration.repository.DataRepository;
import org.sap.move.commercemigrationtest.AbstractDatabaseMigrationTest;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@IntegrationTest
public class CustomTableMigrationTest extends AbstractDatabaseMigrationTest {

    private static final String CUSTOM_TABLE = "customtable";

    @Before
    public void before() {
        // create the custom table in the source DB
        final DataRepository sourceRepository = getMigrationContext().getDataSourceRepository();
        final DataSource sourceDb = sourceRepository.getDataSource();

        final JdbcTemplate jdbcTemplate = new JdbcTemplate(sourceDb);
        jdbcTemplate.execute("CREATE TABLE " + CUSTOM_TABLE + "(ID INT)");
        jdbcTemplate.execute("INSERT INTO " + CUSTOM_TABLE + "(ID) VALUES (1)");
        jdbcTemplate.execute("CREATE INDEX IDX_" + CUSTOM_TABLE + " ON " + CUSTOM_TABLE + "(ID)");

        getMigrationContext().getCustomTables().add(CUSTOM_TABLE);
    }

    @Test
    public void testCustomTablesAreMigrated() throws Exception {
        // Schema migration
        final Database sourceDatabase = getMigrationContext().getDataSourceRepository().asDatabase();

        getMigrationContext().setSchemaMigrationEnabled(true);
        getMigrationContext().setRemoveMissingColumnsToSchemaEnabled(false);
        getMigrationContext().setAddMissingColumnsToSchemaEnabled(false);
        getMigrationContext().setRemoveMissingTablesToSchemaEnabled(false);
        getMigrationContext().setAddMissingTablesToSchemaEnabled(true);
        assertTrue(getDifference().hasDifferences());

        schemaDifferenceService.executeSchemaDifferences(getMigrationContext());
        assertFalse(getDifference().hasDifferences());

        Index[] sourceIndices = sourceDatabase.findTable(CUSTOM_TABLE, false).getIndices();
        Index[] targetIndices = getMigrationContext().getDataTargetRepository().asDatabase().findTable(CUSTOM_TABLE, false).getIndices();
        assertThat(sourceIndices.length).isGreaterThan(0);

        for (int i = 0; i < sourceIndices.length; i++) {
            assertTrue(sourceIndices[i].equalsIgnoreCase(targetIndices[i]));
        }

        // Data migration
        final Set<DataCopyItem> dataCopyItems = dataCopyItemProvider.get(getMigrationContext());

        assertTrue(dataCopyItems.stream().map(DataCopyItem::getSourceItem).collect(Collectors.toSet()).contains(CUSTOM_TABLE));

        startMigrationAndWaitForFinish(databaseMigrationService);

        for (final CopyContext.DataCopyItem copyItem : dataCopyItems) {
            long sourceRowCount = getMigrationContext().getDataSourceRepository().getRowCount(copyItem.getSourceItem());
            long targetRowCount = getMigrationContext().getDataTargetRepository().getRowCount(copyItem.getTargetItem());
            assertEquals(sourceRowCount, targetRowCount);
        }
    }

    @After
    public void after() {
        // cleanup
        final DataRepository sourceRepository = getMigrationContext().getDataSourceRepository();
        final DataSource sourceDb = sourceRepository.getDataSource();
        final DataRepository targetRepository = getMigrationContext().getDataTargetRepository();
        final DataSource targetDb = targetRepository.getDataSource();

        final JdbcTemplate sourceTemplate = new JdbcTemplate(sourceDb);
        sourceTemplate.execute("DROP TABLE " + CUSTOM_TABLE);

        final JdbcTemplate targetTemplate = new JdbcTemplate(targetDb);
        targetTemplate.execute("DROP TABLE " + CUSTOM_TABLE);

        getMigrationContext().getCustomTables().remove(CUSTOM_TABLE);
    }


}
