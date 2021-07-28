package org.sap.move.commercemigrationtest.integration;

import de.hybris.bootstrap.annotations.IntegrationTest;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Index;
import org.apache.ddlutils.model.Table;
import org.junit.Test;
import org.sap.move.commercemigrationtest.AbstractDatabaseMigrationTest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

@IntegrationTest
public class SchemaMigrationIntegrationTest extends AbstractDatabaseMigrationTest {

    @Test
    public void testRemoveColumnWithIndexInTarget() throws Exception {
        String tableName = "usergroups";
        String columnName = "mytestcolumn";
        String indexName = "mytestcolumnindex";
        Database database = getMigrationContext().getDataTargetRepository().asDatabase();
        Platform platform = getMigrationContext().getDataTargetRepository().asPlatform();
        Table table = database.findTable(tableName, false);
        Column column = createColumn(columnName);
        Index index = createIndex(indexName, column);
        doTestRemoveColumnWithIndexInTarget(database, platform, table, column, index);
    }

    @Test
    public void testRemoveColumnWithCompositeIndexInTarget() throws Exception {
        String tableName = "usergroups";
        String columnName = "mytestcolumn";
        String existingColumnName = "p_name";
        String indexName = "mytestcolumnindex";
        Database database = getMigrationContext().getDataTargetRepository().asDatabase();
        Platform platform = getMigrationContext().getDataTargetRepository().asPlatform();
        Table table = database.findTable(tableName, false);
        Column column = createColumn(columnName);
        Column existingColumn = table.findColumn(existingColumnName, false);
        Index index = createIndex(indexName, column, existingColumn);
        doTestRemoveColumnWithIndexInTarget(database, platform, table, column, index);
    }

    @Test
    public void testAddMissingTableToTarget() throws Exception {
        final String tableName = "usergroups";
        Database targetDatabase = getMigrationContext().getDataTargetRepository().asDatabase();
        Database sourceDatabase = getMigrationContext().getDataSourceRepository().asDatabase();

        Platform targetPlatform = getMigrationContext().getDataTargetRepository().asPlatform();
        Table table = targetDatabase.findTable(tableName, false);
        getMigrationContext().getIncludedTables().add(table.getName());
        getMigrationContext().setSchemaMigrationEnabled(true);
        getMigrationContext().setRemoveMissingColumnsToSchemaEnabled(false);
        getMigrationContext().setAddMissingColumnsToSchemaEnabled(false);
        getMigrationContext().setRemoveMissingTablesToSchemaEnabled(false);
        getMigrationContext().setAddMissingTablesToSchemaEnabled(true);
        assertThat(getDifference().hasDifferences()).isFalse();
        targetDatabase.removeTable(table);
        targetPlatform.alterTables(targetDatabase, false);
        assertThat(getDifference().hasDifferences()).isTrue();
        schemaDifferenceService.executeSchemaDifferences(getMigrationContext());
        assertThat(getDifference().hasDifferences()).isFalse();
        Index[] sourceIndices = sourceDatabase.findTable(tableName, false).getIndices();
        Index[] targetIndices = getMigrationContext().getDataTargetRepository().asDatabase().findTable(tableName, false).getIndices();
        assertThat(sourceIndices.length).isGreaterThan(0);

        for (int i = 0; i < sourceIndices.length; i++) {
            assertTrue(sourceIndices[i].equalsIgnoreCase(targetIndices[i]));
        }
    }

    private void doTestRemoveColumnWithIndexInTarget(Database database, Platform platform, Table table, Column column, Index index) throws Exception {
        getMigrationContext().getIncludedTables().add(table.getName());
        getMigrationContext().setSchemaMigrationEnabled(true);
        getMigrationContext().setRemoveMissingColumnsToSchemaEnabled(true);
        getMigrationContext().setAddMissingColumnsToSchemaEnabled(false);
        getMigrationContext().setRemoveMissingTablesToSchemaEnabled(false);
        getMigrationContext().setAddMissingTablesToSchemaEnabled(false);

        assertThat(getDifference().hasDifferences()).isFalse();
        table.addColumn(column);
        platform.alterTables(database, false);
        assertThat(getDifference().hasDifferences()).isTrue();
        schemaDifferenceService.executeSchemaDifferences(getMigrationContext());
        assertThat(getDifference().hasDifferences()).isFalse();
        table.addColumn(column);
        table.addIndex(index);
        platform.alterTables(database, false);
        assertThat(getDifference().hasDifferences()).isTrue();
        schemaDifferenceService.executeSchemaDifferences(getMigrationContext());
        assertThat(getDifference().hasDifferences()).isFalse();
    }

}
