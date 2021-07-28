package org.sap.move.commercemigrationtest.integration;

import de.hybris.bootstrap.annotations.IntegrationTest;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Table;
import org.junit.Test;
import org.sap.commercemigration.context.CopyContext;
import org.sap.commercemigration.dataset.DataSet;
import org.sap.commercemigration.provider.CopyItemProvider;
import org.sap.commercemigration.repository.DataRepository;
import org.sap.commercemigration.service.DatabaseMigrationService;
import org.sap.move.commercemigrationtest.AbstractDatabaseMigrationTest;

import javax.annotation.Resource;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

@IntegrationTest
public class UseCasesIntegrationTest extends AbstractDatabaseMigrationTest {

    @Resource
    private DatabaseMigrationService databaseMigrationService;

    @Resource(name = "dataCopyItemProvider")
    private CopyItemProvider dataCopyItemProvider;

    /**
     * Use case description:
     * <p>
     * Database table(s) created by the Customer directly in the database outside of the SAP Commerce type system.
     * The table(s) is not referenced in either source nor target database type system.
     * <p>
     * Behaviour:
     * <p>
     * Ignore the table(s).
     * If the table(s) need to be migrated, it should be added as a type to the *-items.xml,
     * and the database migration should be re-tried.
     */
    @Test
    public void testUseCase1() throws Exception {
        String customTableName = "MYCUSTOMTABLE";

        //create source custom table
        DataRepository dataSourceRepository = getMigrationContext().getDataSourceRepository();
        Platform sourcePlatform = dataSourceRepository.asPlatform();
        Database sourceDatabase = dataSourceRepository.asDatabase();
        Table yDeploymentsSource = sourceDatabase.findTable("ydeployments", false);
        String sql = String.format("CREATE TABLE %s (NAME VARCHAR(45))", customTableName);
        dataSourceRepository.executeUpdateAndCommit(sql);


        //Check custom table is now part of the schema
        assertThat(dataSourceRepository.getAllTableNames()).contains(customTableName);

        //Check not referenced in source type system
        DataSet yDeploymentsSourceResultSet = dataSourceRepository.getAll(yDeploymentsSource.getName());
        for (List<Object> row : yDeploymentsSourceResultSet.getAllResults()) {
            String tableName = (String) yDeploymentsSourceResultSet.getColumnValue("TableName", row);
            assertThat(tableName).isNotEqualToIgnoringCase(customTableName);
        }

        //Check not referenced in target type system
        DataRepository dataTargetRepository = getMigrationContext().getDataTargetRepository();
        Database targetDatabase = dataTargetRepository.asDatabase();
        Table yDeploymentsTarget = sourceDatabase.findTable("ydeployments", false);
        DataSet yDeploymentsTargetResultSet = dataTargetRepository.getAll(yDeploymentsTarget.getName());
        for (List<Object> row : yDeploymentsTargetResultSet.getAllResults()) {
            String tableName = (String) yDeploymentsTargetResultSet.getColumnValue("TableName", row);
            assertThat(tableName).isNotEqualToIgnoringCase(customTableName);
        }
        //If test did not fail yet, we have a custom table in the db (hybris schema) which is not part of source and target ydeployments

        //Check that the custom table is ignored for migration
        Set<CopyContext.DataCopyItem> dataCopyItems = dataCopyItemProvider.get(getMigrationContext());
        for (CopyContext.DataCopyItem copyItem : dataCopyItems) {
            assertThat(copyItem.getSourceItem()).isNotEqualToIgnoringCase(customTableName);
            assertThat(copyItem.getTargetItem()).isNotEqualToIgnoringCase(customTableName);
        }

        //cleanup
        dataSourceRepository.executeUpdateAndCommit(String.format("DROP TABLE %s", customTableName));

        //Check custom table is not part of the schema anymore
        assertThat(dataSourceRepository.getAllTableNames()).doesNotContain(customTableName);
    }

    /**
     * Use case description:
     * <p>
     * Different deployment names (but not type codes) between source and target due to historical reasons.
     * This can happen because update system ignores changing deployment names.
     * So *-items.xml might be in sync with the target database type system but not in sync with the source database type system.
     * <p>
     * Behaviour:
     * <p>
     * CMT performs proper mapping of these data, so that data is written to the right table in the target system.
     */
    public void testUseCase2() {

    }

    /**
     * Use case description:
     * <p>
     * Missing deployment tables in source or target
     * <p>
     * Behaviour:
     * <p>
     * CMT will fail the migration and provide the user with a report listing such types. These issues have to be fixed by the user before trying the migration again.
     */
    public void testUseCase3() {

    }

    /**
     * Use case description:
     * <p>
     * Orphaned tables at source after removing a type from the type system.
     * It might happen after removing a type from *-items.xml and running orphaned types cleanup.
     * <p>
     * Behaviour:
     * <p>
     * The tables will be ignored.
     * This is the same use case as #1 (i.e. tables unknown to Commerce type system) as in practice orphaned tables cannot be recognized by the system.
     */
    @Test
    public void testUseCase4() throws Exception {
        testUseCase1();
    }

    /**
     * Use case description:
     * <p>
     * Tables created at source as a result of rolling updates or type system versioning (i.e. using ant createtypesystem)
     * <p>
     * Behaviour:
     * <p>
     * CMT will make sure the right type system tables are used in the migration. The current source type system name has to be provided as a parameter.
     * Only current database type system is considered. Older database type systems are ignored.
     * The tables related to older type systems will not be reported as ignored tables in compatibility report, as the older type system as a whole is ignored.
     */
    public void testUseCase5() {

    }

    /**
     * Use case description:
     * <p>
     * Columns created by the Customer (e.g. direct manipulation of the database schema bypassing SAP Commerce type system)
     * <p>
     * Behaviour:
     * <p>
     * Such columns will be ignored by CMT.
     */
    public void testUseCase6() {

    }

    /**
     * Use case description:
     * <p>
     * Different column names between source and target due to historical reasons.
     * A special case is an attribute being mapped to the props table in one of the systems.
     * <p>
     * Behaviour:
     * <p>
     * Perform data mapping during data migration.
     */
    public void testUseCase7() {

    }

    /**
     * Use case description:
     * <p>
     * Orphaned columns after removing an attribute from the type system.
     * <p>
     * Behaviour:
     * <p>
     * Indistinguishable from use case #6, such columns will be ignored.
     */
    public void testUseCase8() {
        testUseCase6();
    }

    /**
     * Use case description:
     * <p>
     * Column data type different between source and target.
     * An edge case that might be caused by different type system attributes being mapped to the same column name in source and target (due to historical reasons - software upgrades history)
     * <p>
     * Behaviour:
     * <p>
     * Report and fail.
     */
    public void testUseCase9() {

    }

    /**
     * Use case description:
     * <p>
     * Missing type in target system;
     * could be caused by the removal of a type in *-items.xml as performing system update does not remove type information from the database type system. This requires cleaning of orphaned types to be explicitly performed.
     * <p>
     * Behaviour:
     * <p>
     * Report and fail (require the customer to perform orphaned types cleanup at source system) - unless there are no instance
     */
    public void testUseCase10() {

    }

    /**
     * Use case description:
     * <p>
     * Missing attribute in target type system
     * <p>
     * Behaviour:
     * <p>
     * Report and fail unless all values are NULL, empty or no relation elements found.
     */
    public void testUseCase11() {

    }


}
