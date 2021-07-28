/*
 * Copyright (c) 2020 SAP SE or an SAP affiliate company. All rights reserved.
 */
package org.sap.commercemigration.jobs;

import de.hybris.platform.cronjob.enums.CronJobResult;
import de.hybris.platform.cronjob.enums.CronJobStatus;
import de.hybris.platform.cronjob.model.CronJobModel;
import de.hybris.platform.servicelayer.cronjob.PerformResult;
import org.apache.commons.collections4.CollectionUtils;
import org.sap.commercemigration.MigrationStatus;
import org.sap.commercemigration.model.cron.IncrementalMigrationCronJobModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;


/**
 * This class offers functionality for IncrementalMigrationJob.
 */
public class IncrementalMigrationJob extends AbstractMigrationJobPerformable {

  private static final Logger LOG = LoggerFactory.getLogger(IncrementalMigrationJob.class);

  @Override
  public PerformResult perform(final CronJobModel cronJobModel) {
    IncrementalMigrationCronJobModel incrementalMigrationCronJob;

    if (cronJobModel instanceof IncrementalMigrationCronJobModel) {
      incrementalMigrationCronJob = (IncrementalMigrationCronJobModel) cronJobModel;
    } else {
      throw new IllegalStateException("Wrong cronJob Model " + cronJobModel.getCode());
    }

    boolean caughtExeption = false;
    try {
      checkRunningOrRestartedCronJobs(incrementalMigrationCronJob);

      if (null != incrementalMigrationCronJob.getLastStartTime()) {
        Instant timeStampInstant = incrementalMigrationCronJob.getLastStartTime().toInstant();
        if (LOG.isDebugEnabled()) {
          LOG.debug("For {} IncrementalTimestamp : {}  ", incrementalMigrationCronJob.getCode(),
              timeStampInstant);
        }
        incrementalMigrationContext.setIncrementalMigrationTimestamp(timeStampInstant);
      }
      if (CollectionUtils.isNotEmpty(incrementalMigrationCronJob.getMigrationItems())) {
        incrementalMigrationContext
            .setIncrementalTables(incrementalMigrationCronJob.getMigrationItems());
      }
      this.incrementalMigrationContext
          .setTruncateEnabled(incrementalMigrationCronJob.isTruncateEnabled());
      this.incrementalMigrationContext
          .setSchemaMigrationAutoTriggerEnabled(incrementalMigrationCronJob.isSchemaAutotrigger());
      this.incrementalMigrationContext.setIncrementalModeEnabled(true);
      this.currentMigrationId = databaseMigrationService
          .startMigration(incrementalMigrationContext);

      MigrationStatus currentState = databaseMigrationService
          .waitForFinish(this.incrementalMigrationContext, this.currentMigrationId);
    } catch (final Exception e) {
      caughtExeption = true;
      LOG.error("Exception caught:", e);
    }
    if (!caughtExeption) {
      incrementalMigrationCronJob.setLastStartTime(cronJobModel.getStartTime());
      modelService.save(cronJobModel);
    }

    return new PerformResult(caughtExeption ? CronJobResult.FAILURE : CronJobResult.SUCCESS,
        CronJobStatus.FINISHED);
  }

}
