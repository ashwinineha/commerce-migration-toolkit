/*
 * Copyright (c) 2020 SAP SE or an SAP affiliate company. All rights reserved.
 */
package org.sap.commercemigration.jobs;

import com.google.common.base.Preconditions;
import de.hybris.platform.cronjob.enums.CronJobResult;
import de.hybris.platform.cronjob.enums.CronJobStatus;
import de.hybris.platform.cronjob.model.CronJobModel;
import de.hybris.platform.servicelayer.cronjob.PerformResult;
import org.apache.commons.collections4.CollectionUtils;
import org.sap.commercemigration.MigrationStatus;
import org.sap.commercemigration.model.cron.FullMigrationCronJobModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class offers functionality for IncrementalMigrationJob.
 */
public class FullMigrationJob extends AbstractMigrationJobPerformable {

  private static final Logger LOG = LoggerFactory.getLogger(IncrementalMigrationJob.class);

  @Override
  public PerformResult perform(final CronJobModel cronJobModel) {
    FullMigrationCronJobModel fullMigrationCronJobModel;

    Preconditions
        .checkState((cronJobModel instanceof FullMigrationCronJobModel),
            "cronJobModel must the instance of FullMigrationCronJobModel");
    fullMigrationCronJobModel = (FullMigrationCronJobModel) cronJobModel;
    Preconditions.checkNotNull(fullMigrationCronJobModel.getMigrationItems(),
        "We expect at least one table for the full migration");
    Preconditions.checkState(
        null != fullMigrationCronJobModel.getMigrationItems() && !fullMigrationCronJobModel
            .getMigrationItems().isEmpty(),
        "We expect at least one table for the full migration");

    boolean caughtExeption = false;
    try {


        incrementalMigrationContext
            .setIncludedTables(fullMigrationCronJobModel.getMigrationItems());
        updateTypesystemTable(fullMigrationCronJobModel.getMigrationItems());

      incrementalMigrationContext.setDeletionEnabled(false);
      incrementalMigrationContext
          .setTruncateEnabled(fullMigrationCronJobModel.isTruncateEnabled());
      incrementalMigrationContext
          .setSchemaMigrationAutoTriggerEnabled(fullMigrationCronJobModel.isSchemaAutotrigger());
      incrementalMigrationContext.setIncrementalModeEnabled(false);
      currentMigrationId = databaseMigrationService
          .startMigration(incrementalMigrationContext);

      MigrationStatus currentState = databaseMigrationService
          .waitForFinish(incrementalMigrationContext, currentMigrationId);

    } catch (final Exception e) {
      caughtExeption = true;
      LOG.error("Exception caught:", e);
    }

    return new PerformResult(caughtExeption ? CronJobResult.FAILURE : CronJobResult.SUCCESS,
        CronJobStatus.FINISHED);
  }

}
