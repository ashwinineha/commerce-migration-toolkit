package org.sap.move.commercemigrationtest.integration;

import de.hybris.bootstrap.annotations.IntegrationTest;
import org.junit.Test;
import org.sap.commercemigration.context.CopyContext;
import org.sap.commercemigration.profile.DataSourceConfiguration;
import org.sap.commercemigration.provider.CopyItemProvider;
import org.sap.commercemigration.service.DatabaseMigrationService;
import org.sap.move.commercemigrationtest.AbstractDatabaseMigrationTest;
import org.sap.move.commercemigrationtest.DatabaseHolder;
import org.sap.move.commercemigrationtest.configuration.TestDataSourceConfiguration;
import org.sap.move.commercemigrationtest.context.TestMigrationContext;

import javax.annotation.Resource;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

@IntegrationTest
public class PrefixedDatabaseMigrationIntegrationTest extends AbstractDatabaseMigrationTest {

    private static final String SOURCE_TABLE_PREFIX = "srcpfx_";
    private static final String TARGET_TABLE_PREFIX = "tgtpfx_";
    @Resource
    private DatabaseMigrationService databaseMigrationService;
    @Resource(name = "dataCopyItemProvider")
    private CopyItemProvider dataCopyItemProvider;

    @Override
    protected TestMigrationContext createMigrationContext(DatabaseHolder sourceDatabase, DatabaseHolder targetDatabase) throws Exception {
        TestMigrationContext migrationContext = super.createMigrationContext(sourceDatabase, targetDatabase);
        DataSourceConfiguration sourceConfiguration = migrationContext.getDataSourceRepository().getDataSourceConfiguration();
        assertThat(sourceConfiguration).isInstanceOf(TestDataSourceConfiguration.class);
        ((TestDataSourceConfiguration) sourceConfiguration).setTablePrefix(SOURCE_TABLE_PREFIX);

        DataSourceConfiguration targetConfiguration = migrationContext.getDataTargetRepository().getDataSourceConfiguration();
        assertThat(targetConfiguration).isInstanceOf(TestDataSourceConfiguration.class);
        ((TestDataSourceConfiguration) targetConfiguration).setTablePrefix(TARGET_TABLE_PREFIX);
        return migrationContext;
    }

    @Test
    public void testDatabaseMigrationWithDifferentSourceAndTargetTablePrefix() throws Exception {
        Set<CopyContext.DataCopyItem> dataCopyItems = dataCopyItemProvider.get(getMigrationContext());
        assertThat(getTableCountForInitialization()).isEqualTo(dataCopyItems.size());
        String sourceCheck = SOURCE_TABLE_PREFIX + "ydeployments";
        String targetCheck = TARGET_TABLE_PREFIX + "ydeployments";
        assertThat(dataCopyItems.stream())
                .filteredOn(item -> item.getSourceItem().equalsIgnoreCase(sourceCheck) && item.getTargetItem().equalsIgnoreCase(targetCheck))
                .isNotEmpty();
        startMigrationAndWaitForFinish(databaseMigrationService);
        for (CopyContext.DataCopyItem copyItem : dataCopyItems) {
            long sourceRowCount = getMigrationContext().getDataSourceRepository().getRowCount(copyItem.getSourceItem());
            long targetRowCount = getMigrationContext().getDataTargetRepository().getRowCount(copyItem.getTargetItem());
            assertThat(sourceRowCount).isEqualTo(targetRowCount);
        }
    }

}