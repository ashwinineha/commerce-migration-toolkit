/*
 * Copyright (c) 2020 SAP SE or an SAP affiliate company. All rights reserved.
 */
package org.sap.commercemigration.jobs;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import de.hybris.platform.cronjob.enums.CronJobResult;
import de.hybris.platform.cronjob.enums.CronJobStatus;
import de.hybris.platform.cronjob.model.CronJobModel;
import de.hybris.platform.jalo.type.TypeManager;
import de.hybris.platform.servicelayer.cronjob.PerformResult;
import de.hybris.platform.servicelayer.type.TypeService;
import de.hybris.platform.util.Config;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import javax.annotation.Resource;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;
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

  @Resource(name = "typeService")
  private TypeService typeService;

  @Override
  public PerformResult perform(final CronJobModel cronJobModel) {
    IncrementalMigrationCronJobModel incrementalMigrationCronJob;

    Preconditions
        .checkState((cronJobModel instanceof IncrementalMigrationCronJobModel),
            "cronJobModel must the instance of FullMigrationCronJobModel");

    incrementalMigrationCronJob = (IncrementalMigrationCronJobModel) cronJobModel;
    Preconditions.checkState(
        null != incrementalMigrationCronJob.getMigrationItems() && !incrementalMigrationCronJob
            .getMigrationItems().isEmpty(),
        "We expect at least one table for the incremental migration");

    final Set<String> deletionTableSet = getDeletionTableSet(incrementalMigrationCronJob.getMigrationItems());

    MigrationStatus currentState;

    String currentMigrationId;
    boolean caughtExeption = false;
    try {

      if (null != incrementalMigrationCronJob.getLastStartTime()) {
        Instant timeStampInstant = incrementalMigrationCronJob.getLastStartTime().toInstant();
        if (LOG.isDebugEnabled()) {
          LOG.debug("For {} IncrementalTimestamp : {}  ", incrementalMigrationCronJob.getCode(),
              timeStampInstant);
        }
        incrementalMigrationContext.setIncrementalMigrationTimestamp(timeStampInstant);
      }
      this.incrementalMigrationContext.setIncrementalModeEnabled(true);

      incrementalMigrationContext
          .setTruncateEnabled(false);
      // if deletion enabled
      if (CollectionUtils.isNotEmpty(deletionTableSet)) {
      //  deletionTableSet.add(deletionTable);
        this.incrementalMigrationContext
            .setSchemaMigrationAutoTriggerEnabled(false);

        incrementalMigrationContext
            .setIncrementalTables(deletionTableSet);
        incrementalMigrationContext.setDeletionEnabled(true);

        currentMigrationId = databaseMigrationService
            .startMigration(incrementalMigrationContext);

         currentState = databaseMigrationService
            .waitForFinish(this.incrementalMigrationContext, currentMigrationId);
      }

      incrementalMigrationContext.setDeletionEnabled(false);
      if (CollectionUtils.isNotEmpty(incrementalMigrationCronJob.getMigrationItems())) {
        incrementalMigrationContext
            .setIncrementalTables(incrementalMigrationCronJob.getMigrationItems());
      }
      this.incrementalMigrationContext
          .setSchemaMigrationAutoTriggerEnabled(incrementalMigrationCronJob.isSchemaAutotrigger());

      currentMigrationId = databaseMigrationService
          .startMigration(incrementalMigrationContext);

       currentState = databaseMigrationService
          .waitForFinish(this.incrementalMigrationContext, currentMigrationId);
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

  private Set<String> getDeletionTableSetFromItemType(Set<String> incMigrationItems) {
    String deletionItemTypes = Config
        .getString(CommercereportingConstants.MIGRATION_DATA_INCREMENTAL_DELETIONS_ITEMTYPES, "");
    if (StringUtils.isEmpty(deletionItemTypes)) {
      return Collections.emptySet();
    }

    final Set<String> result = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

    final List<String> itemtypesArray = Splitter.on(",")
        .omitEmptyStrings()
        .trimResults()
        .splitToList(deletionItemTypes.toLowerCase());

    String tableName;
    for(String itemType : itemtypesArray){
      tableName = typeService.getComposedTypeForCode(itemType).getTable();
      if(incMigrationItems.contains(tableName)){
        result.add(tableName);
      }
    }
    return result;
  }

  private Set<String> getDeletionTableSetFromTypeCodes(Set<String> incMigrationItems) {
    String deletionTypecodes = Config
        .getString(CommercereportingConstants.MIGRATION_DATA_INCREMENTAL_DELETIONS_TYPECODES, "");
    if (StringUtils.isEmpty(deletionTypecodes)) {
      return Collections.emptySet();
    }

    final Set<String> result = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

    final List<String> typecodeArray = Splitter.on(",")
        .omitEmptyStrings()
        .trimResults()
        .splitToList(deletionTypecodes.toLowerCase());

    String tableName;
    for(String typecode : typecodeArray){
      tableName = TypeManager.getInstance()
          .getRootComposedType(Integer.valueOf(typecode)).getTable();
      if(incMigrationItems.contains(tableName)){
        result.add(tableName);
      }
    }
    return result;
  }

  // TO do , change to static varriable
  private Set<String> getDeletionTableSet(Set<String> incMigrationItems){
    if(Config
        .getBoolean(CommercereportingConstants.MIGRATION_DATA_INCREMENTAL_DELETIONS_TYPECODES_ENABLED, false)){
      return getDeletionTableSetFromTypeCodes(incMigrationItems);
    }
    else if(Config
        .getBoolean(CommercereportingConstants.MIGRATION_DATA_INCREMENTAL_DELETIONS_ITEMTYPES_ENABLED, false)){
      getDeletionTableSetFromItemType(incMigrationItems);
    }
     return Collections.emptySet();
  }

}
