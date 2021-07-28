package org.sap.move.commercemigrationtest.integration;

import de.hybris.bootstrap.annotations.IntegrationTest;
import org.junit.Test;
import org.sap.commercemigration.MigrationProgress;
import org.sap.commercemigration.service.DatabaseMigrationService;
import org.sap.move.commercemigrationtest.AbstractDatabaseMigrationTest;

import javax.annotation.Resource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

@IntegrationTest
public class DatabaseMigrationSchedulerTest extends AbstractDatabaseMigrationTest {
    @Resource
    private DatabaseMigrationService databaseMigrationService;

    @Test
    public void testFailOnError() throws Exception {
        //Given
        getMigrationContext().setFailOnErrorEnabled(true);
        try {
            //force error: missing column in target table
            getMigrationContext().getDataTargetRepository().executeUpdateAndCommit("ALTER TABLE users DROP COLUMN p_name");
        } catch (Exception e) {
            //ignore
        }
        //When
        Throwable caught = catchThrowable(() -> startMigrationAndWaitForFinish(databaseMigrationService));
        //Then
        assertThat(caught).hasMessage("Database migration failed");
        assertThat(databaseMigrationService.getMigrationState(getMigrationContext(), getLatestMigrationId()).getStatus()).isEqualTo(MigrationProgress.ABORTED);
        assertThat(getMigrationContext().getDataTargetRepository().getRowCount("users")).isZero();

    }
}
