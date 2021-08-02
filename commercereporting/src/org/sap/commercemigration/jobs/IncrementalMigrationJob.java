/*
 * Copyright (c) 2020 SAP SE or an SAP affiliate company. All rights reserved.
 */
package org.sap.commercemigration.jobs;

import de.hybris.platform.cronjob.enums.CronJobResult;
import de.hybris.platform.cronjob.enums.CronJobStatus;
import de.hybris.platform.cronjob.model.CronJobModel;
import de.hybris.platform.servicelayer.cronjob.PerformResult;
import de.hybris.platform.util.Config;
import java.time.Instant;
import java.util.Set;
import java.util.TreeSet;
import org.apache.commons.collections4.CollectionUtils;
import org.sap.commercemigration.MigrationStatus;
import org.sap.commercemigration.constants.CommercereportingConstants;
import org.sap.commercemigration.model.cron.IncrementalMigrationCronJobModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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

    boolean isDeletionsEnabled = Config
        .getBoolean(CommercereportingConstants.MIGRATION_DATA_DELETION_ENABLED, true);
    String deletionTable = Config
        .getString(CommercereportingConstants.MIGRATION_DATA_DELETION_TABLE, "itemdeletionmarkers");
    final Set<String> deletionTableSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

    MigrationStatus currentState;


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
      this.incrementalMigrationContext
          .setTruncateEnabled(incrementalMigrationCronJob.isTruncateEnabled());
      this.incrementalMigrationContext.setIncrementalModeEnabled(true);
      // if deletion enabled
      if (isDeletionsEnabled) {
        deletionTableSet.add(deletionTable);
        this.incrementalMigrationContext
            .setSchemaMigrationAutoTriggerEnabled(false);

        incrementalMigrationContext
            .setIncrementalTables(deletionTableSet);
        incrementalMigrationContext.setDeletionEnabled(true);

        this.currentMigrationId = databaseMigrationService
            .startMigration(incrementalMigrationContext);

         currentState = databaseMigrationService
            .waitForFinish(this.incrementalMigrationContext, this.currentMigrationId);
      }

      incrementalMigrationContext.setDeletionEnabled(false);

      if (CollectionUtils.isNotEmpty(incrementalMigrationCronJob.getMigrationItems())) {
        incrementalMigrationContext
            .setIncrementalTables(incrementalMigrationCronJob.getMigrationItems());
      }
      this.incrementalMigrationContext
          .setSchemaMigrationAutoTriggerEnabled(incrementalMigrationCronJob.isSchemaAutotrigger());
      this.currentMigrationId = databaseMigrationService
          .startMigration(incrementalMigrationContext);
       currentState = databaseMigrationService
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
