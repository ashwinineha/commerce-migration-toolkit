package org.sap.move.commercemigrationtest.integration;

import de.hybris.bootstrap.annotations.IntegrationTest;
import org.junit.Test;
import org.sap.commercemigration.context.CopyContext;
import org.sap.commercemigration.dataset.DataSet;
import org.sap.commercemigration.service.impl.DefaultDatabaseSchemaDifferenceService;
import org.sap.move.commercemigrationtest.AbstractDatabaseMigrationTest;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

@IntegrationTest
public class DatabaseMigrationIntegrationTest extends AbstractDatabaseMigrationTest {

    @Test
    public void testDataRepositoryValidateSourceConnection() throws Exception {
        assertThat(getMigrationContext().getDataSourceRepository().validateConnection()).isTrue();
        assertThat(getMigrationContext().getDataTargetRepository().validateConnection()).isTrue();
    }

    @Test
    public void testRepositoryGetAllTableNames() throws Exception {
        assertThat(getTableCountForInitialization()).isEqualTo(getMigrationContext().getDataSourceRepository().getAllTableNames().size());
        assertThat(getTableCountForInitialization()).isEqualTo(getMigrationContext().getDataTargetRepository().getAllTableNames().size());
    }

    @Test
    public void testDataCopyItemProviderGetWithIdenticalSchemaAndNoFilters() throws Exception {
        assertThat(getTableCountForInitialization()).isEqualTo(dataCopyItemProvider.get(getMigrationContext()).size());
    }

    @Test
    public void testDataCopyItemProviderGetWithIdenticalSchemaAndInclusionFilter() throws Exception {
        getMigrationContext().getIncludedTables().clear();
        getMigrationContext().getIncludedTables().add("ydeployments");
        assertThat(dataCopyItemProvider.get(getMigrationContext()).size()).isEqualTo(1);
        getMigrationContext().getIncludedTables().clear();
    }

    @Test
    public void testDataCopyItemProviderGetWithIdenticalSchemaAndExclusionFilter() throws Exception {
        getMigrationContext().getIncludedTables().clear();
        getMigrationContext().getExcludedTables().add("ydeployments");
        assertThat(getTableCountForInitialization() - 1).isEqualTo(dataCopyItemProvider.get(getMigrationContext()).size());
        getMigrationContext().getExcludedTables().clear();
    }

    @Test
    public void testDatabaseMigration() throws Exception {
        Set<CopyContext.DataCopyItem> dataCopyItems = dataCopyItemProvider.get(getMigrationContext());
        startMigrationAndWaitForFinish(databaseMigrationService);
        for (CopyContext.DataCopyItem copyItem : dataCopyItems) {
            long sourceRowCount = getMigrationContext().getDataSourceRepository().getRowCount(copyItem.getSourceItem());
            long targetRowCount = getMigrationContext().getDataTargetRepository().getRowCount(copyItem.getTargetItem());
            assertThat(sourceRowCount).isEqualTo(targetRowCount);
        }
    }

    @Test
    public void testDatabaseMigrationWithNullifyColumn() throws Exception {
        DataSet attributeDescriptorsBefore = getMigrationContext().getDataTargetRepository().getAll("attributedescriptors");
        for (List<Object> row : attributeDescriptorsBefore.getAllResults()) {
            assertThat(attributeDescriptorsBefore.getColumnValue("p_extensionname", row)).isNotNull();
        }
        getMigrationContext().getNullifyColumns().clear();
        getMigrationContext().getNullifyColumns().put("attributedescriptors", setOf("p_extensionname"));
        Set<CopyContext.DataCopyItem> dataCopyItems = dataCopyItemProvider.get(getMigrationContext());
        startMigrationAndWaitForFinish(databaseMigrationService);
        for (CopyContext.DataCopyItem copyItem : dataCopyItems) {
            long sourceRowCount = getMigrationContext().getDataSourceRepository().getRowCount(copyItem.getSourceItem());
            long targetRowCount = getMigrationContext().getDataTargetRepository().getRowCount(copyItem.getTargetItem());
            assertThat(sourceRowCount).isEqualTo(targetRowCount);
        }
        DataSet attributeDescriptorsAfter = getMigrationContext().getDataTargetRepository().getAll("attributedescriptors");
        for (List<Object> row : attributeDescriptorsAfter.getAllResults()) {
            assertThat(attributeDescriptorsAfter.getColumnValue("p_extensionname", row)).isNull();
        }
    }

    @Test
    public void testDatabaseMigrationWithExcludedColumn() throws Exception {
        getMigrationContext().setTruncateEnabled(true);
        DataSet targetAttributeDescriptorsBefore = getMigrationContext().getDataTargetRepository().getAll("attributedescriptors");
        for (List<Object> row : targetAttributeDescriptorsBefore.getAllResults()) {
            assertThat(targetAttributeDescriptorsBefore.getColumnValue("modifiedTS", row)).isNotNull();
        }
        DataSet sourceAttributeDescriptorsBefore = getMigrationContext().getDataSourceRepository().getAll("attributedescriptors");
        for (List<Object> row : sourceAttributeDescriptorsBefore.getAllResults()) {
            assertThat(sourceAttributeDescriptorsBefore.getColumnValue("modifiedTS", row)).isNotNull();
        }
        getMigrationContext().getExcludedColumns().clear();
        getMigrationContext().getExcludedColumns().put("attributedescriptors", setOf("modifiedTS"));
        Set<CopyContext.DataCopyItem> dataCopyItems = dataCopyItemProvider.get(getMigrationContext());
        startMigrationAndWaitForFinish(databaseMigrationService);
        for (CopyContext.DataCopyItem copyItem : dataCopyItems) {
            long sourceRowCount = getMigrationContext().getDataSourceRepository().getRowCount(copyItem.getSourceItem());
            long targetRowCount = getMigrationContext().getDataTargetRepository().getRowCount(copyItem.getTargetItem());
            assertThat(sourceRowCount).isEqualTo(targetRowCount);
        }
        DataSet attributeDescriptorsAfter = getMigrationContext().getDataTargetRepository().getAll("attributedescriptors");
        for (List<Object> row : attributeDescriptorsAfter.getAllResults()) {
            assertThat(attributeDescriptorsAfter.getColumnValue("modifiedTS", row)).isNull();
        }
    }

    @Test
    public void testSchemaDiff() throws Exception {
        DefaultDatabaseSchemaDifferenceService.SchemaDifferenceResult difference = schemaDifferenceService.getDifference(getMigrationContext());
        assertThat(difference.getSourceSchema().getMissingTables().size()).isEqualTo(0);
        assertThat(difference.getSourceSchema().getMissingColumnsInTable().size()).isEqualTo(0);
        assertThat(difference.getTargetSchema().getMissingTables().size()).isEqualTo(0);
        assertThat(difference.getTargetSchema().getMissingColumnsInTable().size()).isEqualTo(0);
    }

    @Test
    public void testDatabaseMigrationWithIndexesDisabled() throws Exception {
        getMigrationContext().setTruncateEnabled(true);
        getMigrationContext().setDisableAllIndexesEnabled(true);
        Set<CopyContext.DataCopyItem> dataCopyItems = dataCopyItemProvider.get(getMigrationContext());
        startMigrationAndWaitForFinish(databaseMigrationService);
        for (CopyContext.DataCopyItem copyItem : dataCopyItems) {
            long sourceRowCount = getMigrationContext().getDataSourceRepository().getRowCount(copyItem.getSourceItem());
            long targetRowCount = getMigrationContext().getDataTargetRepository().getRowCount(copyItem.getTargetItem());
            assertThat(sourceRowCount).isEqualTo(targetRowCount);
        }
    }

}
