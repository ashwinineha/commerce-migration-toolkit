package org.sap.move.commercemigrationtest.integration;

import de.hybris.bootstrap.annotations.IntegrationTest;
import org.junit.Test;
import org.sap.commercemigration.profile.DataSourceConfiguration;
import org.sap.commercemigration.repository.DataRepository;
import org.sap.commercemigration.service.DatabaseSchemaDifferenceService;
import org.sap.move.commercemigrationtest.AbstractDatabaseMigrationTest;
import org.sap.move.commercemigrationtest.DatabaseHolder;
import org.sap.move.commercemigrationtest.ccv2.Ccv2TypeSystemHelper;
import org.sap.move.commercemigrationtest.configuration.TestDataSourceConfiguration;
import org.sap.move.commercemigrationtest.context.TestMigrationContext;

import javax.annotation.Resource;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

@IntegrationTest
public class CCv2SchemaDuplicateTableRemoveTest extends AbstractDatabaseMigrationTest {

    private static final String SOURCE_TABLE_PREFIX = "srcpfx_";
    private static final String TARGET_TABLE_PREFIX = "tgtpfx_";
    private static final String TARGET_TABLE_PREFIX_EXTRA = "tgtpfxextra_";

    @Resource(name = "schemaDifferenceService")
    private DatabaseSchemaDifferenceService databaseSchemaDifferenceService;

    @Override
    protected TestMigrationContext createMigrationContext(DatabaseHolder sourceDatabase, DatabaseHolder targetDatabase) throws Exception {
        TestMigrationContext migrationContext = super.createMigrationContext(sourceDatabase, targetDatabase);
        setConfigPrefix(migrationContext.getDataSourceRepository(), SOURCE_TABLE_PREFIX);
        setConfigPrefix(migrationContext.getDataTargetRepository(), TARGET_TABLE_PREFIX);
        return migrationContext;
    }

    @Override
    protected void initializeRepository(DataRepository repository, boolean target) throws Exception {
        //only initialize source
        if (!target) {
            super.initializeRepository(repository, target);
        }
    }

    protected void delayedInitializeRepository(DataRepository repository, boolean target) throws Exception {
        //initialize target (has to be done for each iteration because of deletions)
        if (target) {
            super.initializeRepository(repository, target);
            // create the ccv2 tables using the helper tool
            new Ccv2TypeSystemHelper(repository).loadRecords();
            ;
            // create another set of tables
            setConfigPrefix(repository, TARGET_TABLE_PREFIX_EXTRA);
            super.initializeRepository(repository, true);
            // create the ccv2 tables using the helper tool extra prefix
            new Ccv2TypeSystemHelper(repository).loadRecords();
        }
    }

    private void setConfigPrefix(DataRepository repository, String prefixValue) {
        DataSourceConfiguration dataSourceConfiguration = repository.getDataSourceConfiguration();
        assertThat(dataSourceConfiguration).isInstanceOf(TestDataSourceConfiguration.class);
        ((TestDataSourceConfiguration) dataSourceConfiguration).setTablePrefix(prefixValue);
    }


    @Test
    public void testSchemaRemoveDuplicateTables() throws Exception {
        delayedInitializeRepository(getMigrationContext().getDataTargetRepository(), true);
        int originalMaxStaged = getMigrationContext().getMaxTargetStagedMigrations();
        //0 max staged migrations means we only allow: current db.tableprefix and current migration.target.db.tableprefix. Everything else is removed.
        getMigrationContext().setMaxTargetStagedMigrations(0);
        assertThat(getTablesForPrefix(getMigrationContext().getDataTargetRepository(), TARGET_TABLE_PREFIX).size()).isEqualTo((int) getTableCountForInitialization() + 1);
        assertThat(getTablesForPrefix(getMigrationContext().getDataTargetRepository(), TARGET_TABLE_PREFIX_EXTRA).size()).isEqualTo((int) getTableCountForInitialization() + 1);
        databaseSchemaDifferenceService.executeSchemaDifferences(getMigrationContext());
        System.out.println(getTablesForPrefix(getMigrationContext().getDataTargetRepository(), TARGET_TABLE_PREFIX).size());
        System.out.println(getTablesForPrefix(getMigrationContext().getDataTargetRepository(), TARGET_TABLE_PREFIX_EXTRA).size());
        assertThat(getTablesForPrefix(getMigrationContext().getDataTargetRepository(), TARGET_TABLE_PREFIX).size()).isEqualTo(0);
        assertThat(getTablesForPrefix(getMigrationContext().getDataTargetRepository(), TARGET_TABLE_PREFIX_EXTRA).size()).isEqualTo((int) getTableCountForInitialization() + 1);
        getMigrationContext().setMaxTargetStagedMigrations(originalMaxStaged);
    }

    private Set<String> getTablesForPrefix(DataRepository dataTargetRepository, String prefix) throws Exception {
        return dataTargetRepository.getAllTableNames().stream().filter(n -> n.startsWith(prefix)).collect(Collectors.toSet());
    }

}
