/*
 * Copyright (c) 2020 SAP SE or an SAP affiliate company. All rights reserved.
 */
package org.sap.commercemigration.jobs;

import de.hybris.platform.cronjob.enums.CronJobResult;
import de.hybris.platform.cronjob.enums.CronJobStatus;
import de.hybris.platform.cronjob.model.CronJobModel;
import de.hybris.platform.servicelayer.cronjob.PerformResult;
import org.apache.commons.collections4.CollectionUtils;
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

		if (cronJobModel instanceof FullMigrationCronJobModel) {
			fullMigrationCronJobModel = (FullMigrationCronJobModel) cronJobModel;
		} else {
			throw new IllegalStateException("Wrong cronJob Model " + cronJobModel.getCode());
		}
		boolean caughtExeption = false;
		try {
			checkRunningOrRestartedCronJobs(fullMigrationCronJobModel);

			if (CollectionUtils.isNotEmpty(fullMigrationCronJobModel.getMigrationItems())) {
				this.incrementalMigrationContext.setIncludedTables(fullMigrationCronJobModel.getMigrationItems());
				updateTypesystemTable(fullMigrationCronJobModel.getMigrationItems());
			}

			this.incrementalMigrationContext.setTruncateEnabled(fullMigrationCronJobModel.isTruncateEnabled());
			this.incrementalMigrationContext.setSchemaMigrationAutoTriggerEnabled(fullMigrationCronJobModel.isSchemaAutotrigger());
			this.incrementalMigrationContext.setIncrementalModeEnabled(false);
			this.currentMigrationId = databaseMigrationService.startMigration(this.incrementalMigrationContext);

			doPoll(fullMigrationCronJobModel);

		} catch (final Exception e) {
			caughtExeption = true;
			LOG.error("Exception caught:", e);
		}

		return new PerformResult(caughtExeption ? CronJobResult.FAILURE : CronJobResult.SUCCESS, CronJobStatus.FINISHED);
	}

}
