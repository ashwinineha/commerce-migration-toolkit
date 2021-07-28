package org.sap.move.commercemigrationtest.integration;

import de.hybris.bootstrap.annotations.IntegrationTest;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.sap.commercemigration.adapter.DataRepositoryAdapter;
import org.sap.commercemigration.adapter.impl.ContextualDataRepositoryAdapter;
import org.sap.commercemigration.context.CopyContext;
import org.sap.commercemigration.profile.DataSourceConfiguration;
import org.sap.commercemigration.provider.CopyItemProvider;
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
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

@IntegrationTest
public class TypeSystemAwareDatabaseMigrationIntegrationTest extends AbstractDatabaseMigrationTest {

    private static final String TYPESYSTEM_NAME = "MYTESTTS";
    private static final String TYPESYSTEM_SUFFIX = "0";

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
        ((TestDataSourceConfiguration) sourceConfiguration).setTypeSystemName(TYPESYSTEM_NAME);
        ((TestDataSourceConfiguration) sourceConfiguration).setTypeSystemSuffix(TYPESYSTEM_SUFFIX);

        DataSourceConfiguration targetConfiguration = migrationContext.getDataTargetRepository().getDataSourceConfiguration();
        assertThat(targetConfiguration).isInstanceOf(TestDataSourceConfiguration.class);
        ((TestDataSourceConfiguration) targetConfiguration).setTypeSystemName("DEFAULT");
        ((TestDataSourceConfiguration) targetConfiguration).setTypeSystemSuffix(StringUtils.EMPTY);
        return migrationContext;
    }

    @Test
    public void testDatabaseMigrationWithNonDefaultSourceTs() throws Exception {
        String checkTable = "attributedescriptors";
        createTypeSystem(getMigrationContext().getDataSourceRepository(), TYPESYSTEM_NAME);
        //After creating a new source typesystem, we expect the schema diff service not to report any differences, as other TS should be ignored.
        DefaultDatabaseSchemaDifferenceService.SchemaDifferenceResult difference = schemaDifferenceService.getDifference(getMigrationContext());
        assertThat(difference.hasDifferences()).isFalse();

        Set<CopyContext.DataCopyItem> dataCopyItems = dataCopyItemProvider.get(getMigrationContext());
        assertThat(getTableCountForInitialization()).isEqualTo(dataCopyItems.size());
        Set<CopyContext.DataCopyItem> attributedescriptorsItems = dataCopyItems.stream().filter(i -> {
            return i.getTargetItem().equalsIgnoreCase(checkTable);
        }).collect(Collectors.toSet());
        assertThat(attributedescriptorsItems.size()).isEqualTo(1);
        CopyContext.DataCopyItem attributedescriptorsItem = attributedescriptorsItems.stream().findFirst().get();
        assertThat(attributedescriptorsItem.getSourceItem()).isEqualToIgnoringCase(checkTable + TYPESYSTEM_SUFFIX);
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

}