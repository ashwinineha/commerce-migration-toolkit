package org.sap.commercemigration.jobs;

import de.hybris.platform.cronjob.model.CronJobModel;
import de.hybris.platform.servicelayer.cronjob.AbstractJobPerformable;
import de.hybris.platform.servicelayer.cronjob.CronJobService;
import org.apache.commons.lang.StringUtils;
import org.sap.commercemigration.MigrationStatus;
import org.sap.commercemigration.context.IncrementalMigrationContext;
import org.sap.commercemigration.model.cron.FullMigrationCronJobModel;
import org.sap.commercemigration.model.cron.IncrementalMigrationCronJobModel;
import org.sap.commercemigration.service.DatabaseMigrationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractMigrationJobPerformable extends AbstractJobPerformable<CronJobModel> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractMigrationJobPerformable.class);


    protected DatabaseMigrationService databaseMigrationService;
    protected IncrementalMigrationContext incrementalMigrationContext;
    protected CronJobService cronJobService;
    protected String currentMigrationId;

    protected void doPoll(CronJobModel cronJobModel) {
        MigrationStatus currentState;
        try {
            do {
                currentState = databaseMigrationService.getMigrationState(incrementalMigrationContext, this.currentMigrationId);
                LOG.info("IncrementalMigrationJob : {} is running ", cronJobModel.getCode());
                Thread.sleep(5000);

            } while (!currentState.isCompleted());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected void checkRunningOrRestartedCronJobs(CronJobModel cronJobModel) {
        getCronJobService().getRunningOrRestartedCronJobs().forEach(cronJob ->
        {
            if ((cronJob instanceof IncrementalMigrationCronJobModel
                    || cronJob instanceof FullMigrationCronJobModel)
                    && !StringUtils.equals(cronJob.getCode(), cronJobModel.getCode())) {
                throw new IllegalStateException("Previous migrations job already running " + cronJob.getCode());
            }
        });
    }

    @Override
    public boolean isAbortable() {
        return true;
    }

    public IncrementalMigrationContext getIncrementalMigrationContext() {
        return incrementalMigrationContext;
    }

    public void setIncrementalMigrationContext(IncrementalMigrationContext incrementalMigrationContext) {
        this.incrementalMigrationContext = incrementalMigrationContext;
    }

    public CronJobService getCronJobService() {
        return cronJobService;
    }

    public void setCronJobService(CronJobService cronJobService) {
        this.cronJobService = cronJobService;
    }

    public DatabaseMigrationService getDatabaseMigrationService() {
        return databaseMigrationService;
    }

    public void setDatabaseMigrationService(DatabaseMigrationService databaseMigrationService) {
        this.databaseMigrationService = databaseMigrationService;
    }
}
