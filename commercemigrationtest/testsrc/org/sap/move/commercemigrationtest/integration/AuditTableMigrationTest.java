package org.sap.move.commercemigrationtest.integration;

import de.hybris.bootstrap.annotations.IntegrationTest;
import org.junit.Before;
import org.junit.Test;
import org.sap.commercemigration.context.CopyContext.DataCopyItem;
import org.sap.commercemigration.repository.DataRepository;
import org.sap.move.commercemigrationtest.AbstractDatabaseMigrationTest;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import static org.sap.commercemigration.provider.impl.DefaultDataCopyItemProvider.SN_SUFFIX;

@IntegrationTest
public class AuditTableMigrationTest extends AbstractDatabaseMigrationTest {

    private static boolean hasAuditTables(final DataRepository dataSourceRepository) throws Exception {
        return dataSourceRepository.getAllTableNames().stream().anyMatch(t -> t.endsWith(SN_SUFFIX));
    }

    @Before
    public void before() throws Exception {
        assumeTrue(hasAuditTables(getMigrationContext().getDataSourceRepository()));
    }

    @Test
    public void testAuditTablesAreExcludedFromMigration() throws Exception {
        getMigrationContext().setAuditTableMigrationEnabled(false);

        final Set<DataCopyItem> dataCopyItems = dataCopyItemProvider.get(getMigrationContext());

        assertTrue(dataCopyItems.stream().map(DataCopyItem::getSourceItem).noneMatch(i -> i.endsWith(SN_SUFFIX)));
        assertThat(dataCopyItems.size()).isLessThan(getMigrationContext().getDataSourceRepository().getAllTableNames().size());
    }

    @Test
    public void testAuditTablesAreIncludedInMigration() throws Exception {
        getMigrationContext().setAuditTableMigrationEnabled(true);

        final Set<DataCopyItem> dataCopyItems = dataCopyItemProvider.get(getMigrationContext());
        assertTrue(dataCopyItems.stream().map(DataCopyItem::getSourceItem).anyMatch(i -> i.endsWith(SN_SUFFIX)));
        assertEquals(getMigrationContext().getDataSourceRepository().getAllTableNames().size(), dataCopyItems.size());
    }

}
