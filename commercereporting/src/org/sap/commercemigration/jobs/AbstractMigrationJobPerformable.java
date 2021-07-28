/*
 * [y] hybris Platform
 *
 * Copyright (c) 2018 SAP SE or an SAP affiliate company. All rights reserved.
 *
 * This software is the confidential and proprietary information of SAP
 * ("Confidential Information"). You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms of the
 * license agreement you entered into with SAP.
 */
package org.sap.commercemigration.jobs;

import de.hybris.platform.cronjob.model.CronJobModel;
import de.hybris.platform.servicelayer.cronjob.AbstractJobPerformable;
import de.hybris.platform.servicelayer.cronjob.CronJobService;
import de.hybris.platform.util.Config;
import org.apache.commons.lang.StringUtils;
import org.sap.commercemigration.MigrationStatus;
import org.sap.commercemigration.constants.CommercemigrationConstants;
import org.sap.commercemigration.context.IncrementalMigrationContext;
import org.sap.commercemigration.model.cron.FullMigrationCronJobModel;
import org.sap.commercemigration.model.cron.IncrementalMigrationCronJobModel;
import org.sap.commercemigration.repository.DataRepository;
import org.sap.commercemigration.service.DatabaseMigrationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Set;

public abstract class AbstractMigrationJobPerformable extends AbstractJobPerformable<CronJobModel> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractMigrationJobPerformable.class);

    private static final String[] TYPE_SYSTEM_RELATED_TYPES = new String[]{"atomictypes", "attributeDescriptors", "collectiontypes", "composedtypes", "enumerationvalues", "maptypes"};

    private static final String SOURCE_TYPESYSTEMNAME = "migration.ds.source.db.typesystemname";

    private static final String SOURCE_TYPESYSTEMSUFFIX = "migration.ds.source.db.typesystemsuffix";

    private static final String TYPESYSTEM_SELECT_STATEMENT = "IF (EXISTS (SELECT * \n" +
            "  FROM INFORMATION_SCHEMA.TABLES \n" +
            "  WHERE TABLE_SCHEMA = '%s' \n" +
            "  AND TABLE_NAME = '%2$s'))\n" +
            "BEGIN\n" +
            "  select name from %2$s where state = 'current'\n" +
            "END";

    protected DatabaseMigrationService databaseMigrationService;
    protected IncrementalMigrationContext incrementalMigrationContext;
    protected CronJobService cronJobService;
    protected String currentMigrationId;
    private  JdbcTemplate jdbcTemplate;

    protected void doPoll(CronJobModel cronJobModel) {
        MigrationStatus currentState;
        try {
            do {
                currentState = databaseMigrationService.getMigrationState(incrementalMigrationContext, this.currentMigrationId);
                LOG.info("{} is running , {}/{} tables migrated. {} failed. State: {}",cronJobModel.getCode(), currentState.getCompletedTasks(),
                        currentState.getTotalTasks(), currentState.getFailedTasks(), currentState.getStatus());

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

    protected void updateTypesystemTable(Set<String> migrationItems) throws Exception {
        for(final String tableName: migrationItems){
            if(Arrays.stream(TYPE_SYSTEM_RELATED_TYPES).anyMatch(t -> StringUtils.startsWithIgnoreCase(tableName, t)))
            {
                try {
                    final DataRepository sourceRepository = incrementalMigrationContext.getDataSourceRepository();
                         Connection connection = sourceRepository.getConnection();
                         Statement stmt = connection.createStatement();
                         ResultSet resultSet = stmt.executeQuery(String.format(TYPESYSTEM_SELECT_STATEMENT,
                                 sourceRepository.getDataSourceConfiguration().getSchema(),"CCV2_TYPESYSTEM_MIGRATIONS"));

                         String typeSystemName = null;
                        if (resultSet.next()) {
                            typeSystemName = resultSet.getString("name");;
                        } else{
                            return;
                        }

                    String typeSystemTablesQuery = String.format("SELECT TableName FROM %s WHERE Typecode IS NOT NULL AND TableName LIKE '%s' AND TypeSystemName = '%s'",
                            CommercemigrationConstants.DEPLOYMENTS_TABLE, tableName+"%", typeSystemName);
                    ResultSet typeSystemtableresultSet = stmt.executeQuery(typeSystemTablesQuery);
                    String typeSystemTableName = null;
                    if (typeSystemtableresultSet.next()) {
                        typeSystemTableName = typeSystemtableresultSet.getString("TableName");;
                    }
                    Config.setParameter(SOURCE_TYPESYSTEMNAME, typeSystemName);
                    final String typesystemsuffix = typeSystemTableName.substring(tableName.length());

                    Config.setParameter(SOURCE_TYPESYSTEMSUFFIX, typesystemsuffix);
                }
                catch (Exception ex){
                    ex.printStackTrace();
                }
            }
        }
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

    public JdbcTemplate getJdbcTemplate() {
        return jdbcTemplate;
    }

    public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }
}
