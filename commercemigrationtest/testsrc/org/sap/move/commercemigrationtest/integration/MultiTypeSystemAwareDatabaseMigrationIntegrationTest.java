package org.sap.move.commercemigrationtest.integration;

import de.hybris.bootstrap.annotations.IntegrationTest;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Table;
import org.junit.After;
import org.junit.Test;
import org.sap.commercemigration.adapter.DataRepositoryAdapter;
import org.sap.commercemigration.adapter.impl.ContextualDataRepositoryAdapter;
import org.sap.commercemigration.context.CopyContext;
import org.sap.commercemigration.profile.DataSourceConfiguration;
import org.sap.commercemigration.provider.CopyItemProvider;
import org.sap.commercemigration.repository.DataRepository;
import org.sap.commercemigration.service.DatabaseMigrationService;
import org.sap.commercemigration.service.DatabaseSchemaDifferenceService;
import org.sap.commercemigration.service.impl.DefaultDatabaseSchemaDifferenceService;
import org.sap.move.commercemigrationtest.AbstractDatabaseMigrationTest;
import org.sap.move.commercemigrationtest.DatabaseHolder;
import org.sap.move.commercemigrationtest.configuration.TestDataSourceConfiguration;
import org.sap.move.commercemigrationtest.context.TestMigrationContext;

import javax.annotation.Resource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

@IntegrationTest
public class MultiTypeSystemAwareDatabaseMigrationIntegrationTest extends AbstractDatabaseMigrationTest {

    private static final String SRC_TYPESYSTEM_NAME = "SRCTSTEST";
    private static final String SRC_TYPESYSTEM_SUFFIX = "0";

    private static final String TGTTMP_TYPESYSTEM_NAME = "TGTTMP";

    private static final String TGT_TYPESYSTEM_NAME = "TGTTSTEST";
    private static final String TGT_TYPESYSTEM_SUFFIX = "1";

    private static final String ADD_COLUMN = "mytestcolumn";

    private static final String CHECK_TABLE = "attributedescriptors";


    @Resource
    private DatabaseMigrationService databaseMigrationService;
    @Resource(name = "dataCopyItemProvider")
    private CopyItemProvider dataCopyItemProvider;
    @Resource
    private DatabaseSchemaDifferenceService schemaDifferenceService;

    @Override
    protected TestMigrationContext createMigrationContext(DatabaseHolder sourceDatabase, DatabaseHolder targetDatabase) throws Exception {
        TestMigrationContext migrationContext = super.createMigrationContext(sourceDatabase, targetDatabase);
        DataSourceConfiguration sourceConfiguration = migrationContext.getDataSourceRepository().getDataSourceConfiguration();
        assertThat(sourceConfiguration).isInstanceOf(TestDataSourceConfiguration.class);
        ((TestDataSourceConfiguration) sourceConfiguration).setTypeSystemName(SRC_TYPESYSTEM_NAME);
        ((TestDataSourceConfiguration) sourceConfiguration).setTypeSystemSuffix(SRC_TYPESYSTEM_SUFFIX);

        DataSourceConfiguration targetConfiguration = migrationContext.getDataTargetRepository().getDataSourceConfiguration();
        assertThat(targetConfiguration).isInstanceOf(TestDataSourceConfiguration.class);
        ((TestDataSourceConfiguration) targetConfiguration).setTypeSystemName(TGT_TYPESYSTEM_NAME);
        ((TestDataSourceConfiguration) targetConfiguration).setTypeSystemSuffix(TGT_TYPESYSTEM_SUFFIX);
        return migrationContext;
    }

    @Override
    protected void initializeRepository(DataRepository repository, boolean target) throws Exception {
        super.initializeRepository(repository, target);
        if (target) {
            if (!getTargetDatabase().isInitialized()) {
                //creating 2 new typesystems in target, first one is a dummy ts and is not used for the test
                createTypeSystem(getMigrationContext().getDataTargetRepository(), TGTTMP_TYPESYSTEM_NAME);
                createTypeSystem(getMigrationContext().getDataTargetRepository(), TGT_TYPESYSTEM_NAME);
            }
        } else {
            createTypeSystem(getMigrationContext().getDataSourceRepository(), SRC_TYPESYSTEM_NAME);
            //add column to source ts to check schema diff later on
            Database database = getMigrationContext().getDataSourceRepository().asDatabase();
            Column column = createColumn(ADD_COLUMN);
            column.setPrecisionRadix(1);
            database.findTable(CHECK_TABLE + SRC_TYPESYSTEM_SUFFIX, false).addColumn(column);
            String schema = getMigrationContext().getDataSourceRepository().getDataSourceConfiguration().getSchema();
            getMigrationContext().getDataSourceRepository().asPlatform().alterTables(null, schema, null, database, false);
        }
    }


    @Test
    public void testDatabaseMigrationWithNonDefaultTs() throws Exception {
        //After creating a new typesystem, we expect the schema diff service to report a difference from attributedescriptors0->attributedescriptors1.
        DefaultDatabaseSchemaDifferenceService.SchemaDifferenceResult difference = schemaDifferenceService.getDifference(getMigrationContext());
        assertThat(difference.hasDifferences()).isTrue();
        assertThat(difference.getTargetSchema().getMissingColumnsInTable().size()).isEqualTo(1);

        //Get the detected difference
        Map.Entry<DefaultDatabaseSchemaDifferenceService.TableKeyPair, String> missingItem = difference.getTargetSchema().getMissingColumnsInTable().entries().stream().findFirst().get();
        assertThat(missingItem.getKey().getLeftName()).isEqualToIgnoringCase("attributedescriptors0");
        assertThat(missingItem.getKey().getRightName()).isEqualToIgnoringCase("attributedescriptors1");
        assertThat(missingItem.getValue()).isEqualToIgnoringCase(ADD_COLUMN);

        //apply difference and check
        schemaDifferenceService.executeSchemaDifferences(getMigrationContext());
        assertThat(getMigrationContext().getDataTargetRepository().asDatabase().findTable(CHECK_TABLE + TGT_TYPESYSTEM_SUFFIX, false).findColumn(ADD_COLUMN)).isNotNull();

        /*
         * proceed with data copy stuff
         */
        Set<CopyContext.DataCopyItem> dataCopyItems = dataCopyItemProvider.get(getMigrationContext());
        assertThat(getTableCountForInitialization()).isEqualTo(dataCopyItems.size());
        Set<CopyContext.DataCopyItem> attributedescriptorsItems = dataCopyItems.stream()
                .filter(i -> i.getTargetItem().equalsIgnoreCase(CHECK_TABLE + TGT_TYPESYSTEM_SUFFIX))
                .collect(Collectors.toSet());
        assertThat(attributedescriptorsItems.size()).isEqualTo(1);
        CopyContext.DataCopyItem attributedescriptorsItem = attributedescriptorsItems.stream().findFirst().get();
        assertThat(attributedescriptorsItem.getSourceItem()).isEqualToIgnoringCase(CHECK_TABLE + SRC_TYPESYSTEM_SUFFIX);
        startMigrationAndWaitForFinish(databaseMigrationService);
        DataRepositoryAdapter adapter = new ContextualDataRepositoryAdapter(getMigrationContext().getDataSourceRepository());
        for (CopyContext.DataCopyItem copyItem : dataCopyItems) {
            long sourceRowCount = adapter.getRowCount(getMigrationContext(), copyItem.getSourceItem());
            long targetRowCount = getMigrationContext().getDataTargetRepository().getRowCount(copyItem.getTargetItem());
            assertThat(sourceRowCount).isEqualTo(targetRowCount);
        }
        //Check that typesystemname has been correctly adjusted in target ydeployments table
        String targetTypeSystemName = getMigrationContext().getDataTargetRepository().getDataSourceConfiguration().getTypeSystemName();
        try (Connection connection = getMigrationContext().getDataTargetRepository().getConnection();
             Statement stmt = connection.createStatement();
             ResultSet resultSet = stmt.executeQuery(String.format("select TypeSystemName from %s", "ydeployments"))
        ) {
            if (resultSet.next()) {
                assertThat(resultSet.getString(1)).isEqualTo(targetTypeSystemName);
            }
        }
    }

    @Test
    public void testDatabaseMigrationWithBulkAndWithNonDefaultTs() throws Exception {
        getMigrationContext().setBulkCopyEnabled(true);
        testDatabaseMigrationWithNonDefaultTs();
        getMigrationContext().setBulkCopyEnabled(false);
    }

    @Test
    public void testTableFiltersWithNonDefaultTs() throws Exception {
        getMigrationContext().getIncludedTables().add(CHECK_TABLE);

        /**
         * check that table is correctly filtered for schema stuff
         */
        DefaultDatabaseSchemaDifferenceService.SchemaDifferenceResult difference = schemaDifferenceService.getDifference(getMigrationContext());
        assertThat(difference.hasDifferences()).isTrue();
        assertThat(difference.getTargetSchema().getMissingColumnsInTable().size()).isEqualTo(1);

        //Get the detected difference
        Map.Entry<DefaultDatabaseSchemaDifferenceService.TableKeyPair, String> missingItem = difference.getTargetSchema().getMissingColumnsInTable().entries().stream().findFirst().get();
        assertThat(missingItem.getKey().getLeftName()).isEqualToIgnoringCase(CHECK_TABLE+SRC_TYPESYSTEM_SUFFIX);
        assertThat(missingItem.getKey().getRightName()).isEqualToIgnoringCase(CHECK_TABLE+TGT_TYPESYSTEM_SUFFIX);
        assertThat(missingItem.getValue()).isEqualToIgnoringCase(ADD_COLUMN);

        //=> now we know that the schema diff service takes into account the "check table" since it reports differences on it.

        //apply difference and check
        schemaDifferenceService.executeSchemaDifferences(getMigrationContext());
        assertThat(getMigrationContext().getDataTargetRepository().asDatabase().findTable(CHECK_TABLE + TGT_TYPESYSTEM_SUFFIX, false).findColumn(ADD_COLUMN)).isNotNull();

        /**
         * check filter for data copy item stuff
         */
        Set<CopyContext.DataCopyItem> dataCopyItems = dataCopyItemProvider.get(getMigrationContext());
        assertThat(dataCopyItems.size()).isEqualTo(1);
        assertThat(dataCopyItems.stream().findFirst().get().getSourceItem()).isEqualTo(CHECK_TABLE+SRC_TYPESYSTEM_SUFFIX);
        assertThat(dataCopyItems.stream().findFirst().get().getTargetItem()).isEqualTo(CHECK_TABLE+TGT_TYPESYSTEM_SUFFIX);

        //=> now we know that the data copy item provider takes into account only the included table

        getMigrationContext().getIncludedTables().clear();


    }


    @After
    public void cleanupSchema() {
        Database database = getMigrationContext().getDataTargetRepository().asDatabase();
        Table table = database.findTable(CHECK_TABLE + TGT_TYPESYSTEM_SUFFIX, false);
        if (table != null) {
            Column column = table.findColumn(ADD_COLUMN);
            if (column != null) {
                table.removeColumn(column);
            }
        }
        try (Connection conn = getMigrationContext().getDataTargetRepository().getConnection();
             Statement statement = conn.createStatement()
        ) {
            statement.execute(String.format("ALTER TABLE %s%s DROP COLUMN %s", CHECK_TABLE, TGT_TYPESYSTEM_SUFFIX, ADD_COLUMN));
        } catch (Exception e) {
            //ignore
        }
    }

}
