package org.sap.commercemigration.context.impl;


import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.sap.commercemigration.constants.CommercemigrationConstants;
import org.sap.commercemigration.context.MigrationContext;
import org.sap.commercemigration.profile.DataSourceConfiguration;
import org.sap.commercemigration.repository.DataRepository;
import org.sap.commercemigration.repository.impl.DataRepositoryFactory;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

public class DefaultMigrationContext implements MigrationContext {
    private final DataRepository dataSourceRepository;
    private final DataRepository dataTargetRepository;

    public final Configuration configuration;

    public DefaultMigrationContext(final DataSourceConfiguration sourceDataSourceConfiguration,
                                   final DataSourceConfiguration targetDataSourceConfiguration,
                                   final DataRepositoryFactory dataRepositoryFactory,
                                   final Configuration configuration) throws Exception {
        this.dataSourceRepository = dataRepositoryFactory.create(sourceDataSourceConfiguration);
        this.dataTargetRepository = dataRepositoryFactory.create(targetDataSourceConfiguration);
        this.configuration = configuration;
        ensureDefaultLocale(configuration);
    }

    private void ensureDefaultLocale(Configuration configuration) {
        String localeProperty = configuration.getString(CommercemigrationConstants.MIGRATION_LOCALE_DEFAULT);
        Locale locale = Locale.forLanguageTag(localeProperty);
        Locale.setDefault(locale);
    }


    @Override
    public DataRepository getDataSourceRepository() {
        return dataSourceRepository;
    }

    @Override
    public DataRepository getDataTargetRepository() {
        return dataTargetRepository;
    }

    @Override
    public boolean isMigrationTriggeredByUpdateProcess() {
        return getBooleanProperty(CommercemigrationConstants.MIGRATION_TRIGGER_UPDATESYSTEM);
    }

    @Override
    public boolean isSchemaMigrationEnabled() {
        return getBooleanProperty(CommercemigrationConstants.MIGRATION_SCHEMA_ENABLED);
    }

    @Override
    public boolean isAddMissingTablesToSchemaEnabled() {
        return getBooleanProperty(CommercemigrationConstants.MIGRATION_SCHEMA_TARGET_TABLES_ADD_ENABLED);
    }

    @Override
    public boolean isRemoveMissingTablesToSchemaEnabled() {
        return getBooleanProperty(CommercemigrationConstants.MIGRATION_SCHEMA_TARGET_TABLES_REMOVE_ENABLED);
    }

    @Override
    public boolean isAddMissingColumnsToSchemaEnabled() {
        return getBooleanProperty(CommercemigrationConstants.MIGRATION_SCHEMA_TARGET_COLUMNS_ADD_ENABLED);
    }

    @Override
    public boolean isRemoveMissingColumnsToSchemaEnabled() {
        return getBooleanProperty(CommercemigrationConstants.MIGRATION_SCHEMA_TARGET_COLUMNS_REMOVE_ENABLED);
    }

    @Override
    public boolean isSchemaMigrationAutoTriggerEnabled() {
        return getBooleanProperty(CommercemigrationConstants.MIGRATION_SCHEMA_AUTOTRIGGER_ENABLED);
    }

    @Override
    public int getReaderBatchSize() {
        return getNumericProperty(CommercemigrationConstants.MIGRATION_DATA_READER_BATCHSIZE);
    }

    @Override
    public boolean isTruncateEnabled() {
        return getBooleanProperty(CommercemigrationConstants.MIGRATION_DATA_TRUNCATE_ENABLED);
    }

    @Override
    public boolean isAuditTableMigrationEnabled() {
        return getBooleanProperty(CommercemigrationConstants.MIGRATION_DATA_TABLES_AUDIT_ENABLED);
    }

    @Override
    public Set<String> getTruncateExcludedTables() {
        return getListProperty(CommercemigrationConstants.MIGRATION_DATA_TRUNCATE_EXCLUDED);
    }

    @Override
    public int getMaxParallelReaderWorkers() {
        return getNumericProperty(CommercemigrationConstants.MIGRATION_DATA_WORKERS_READER_MAXTASKS);
    }

    @Override
    public int getMaxParallelWriterWorkers() {
        return getNumericProperty(CommercemigrationConstants.MIGRATION_DATA_WORKERS_WRITER_MAXTASKS);
    }

    @Override
    public int getMaxWorkerRetryAttempts() {
        return getNumericProperty(CommercemigrationConstants.MIGRATION_DATA_WORKERS_RETRYATTEMPTS);
    }


    @Override
    public int getMaxParallelTableCopy() {
        return getNumericProperty(CommercemigrationConstants.MIGRATION_DATA_MAXPRALLELTABLECOPY);
    }

    @Override
    public boolean isFailOnErrorEnabled() {
        return getBooleanProperty(CommercemigrationConstants.MIGRATION_DATA_FAILONEERROR_ENABLED);
    }

    @Override
    public Map<String, Set<String>> getExcludedColumns() {
        return getDynamicPropertyKeys(CommercemigrationConstants.MIGRATION_DATA_COLUMNS_EXCLUDED);
    }

    public Map<String, Set<String>> getNullifyColumns() {
        return getDynamicPropertyKeys(CommercemigrationConstants.MIGRATION_DATA_COLUMNS_NULLIFY);
    }

    @Override
    public Set<String> getCustomTables() {
        return getListProperty(CommercemigrationConstants.MIGRATION_DATA_TABLES_CUSTOM);
    }

    @Override
    public Set<String> getExcludedTables() {
        return getListProperty(CommercemigrationConstants.MIGRATION_DATA_TABLES_EXCLUDED);
    }

    @Override
    public Set<String> getIncludedTables() {
        return getListProperty(CommercemigrationConstants.MIGRATION_DATA_TABLES_INCLUDED);
    }

    @Override
    public boolean isDropAllIndexesEnabled() {
        return getBooleanProperty(CommercemigrationConstants.MIGRATION_DATA_INDICES_DROP_ENABLED);
    }

    @Override
    public boolean isDisableAllIndexesEnabled() {
        return getBooleanProperty(CommercemigrationConstants.MIGRATION_DATA_INDICES_DISABLE_ENABLED);
    }

    @Override
    public Set<String> getDisableAllIndexesIncludedTables() {
        return getListProperty(CommercemigrationConstants.MIGRATION_DATA_INDICES_DISABLE_INCLUDED);
    }


    @Override
    public boolean isClusterMode() {
        return getBooleanProperty(CommercemigrationConstants.MIGRATION_CLUSTER_ENABLED);
    }

    @Override
    public boolean isIncrementalModeEnabled() {
        return getBooleanProperty(CommercemigrationConstants.MIGRATION_DATA_INCREMENTAL_ENABLED);
    }

    @Override
    public Set<String> getIncrementalTables() {
        return getListProperty(CommercemigrationConstants.MIGRATION_DATA_INCREMENTAL_TABLES);
    }

    @Override
    public Instant getIncrementalTimestamp() {
        String timeStamp = getStringProperty(CommercemigrationConstants.MIGRATION_DATA_INCREMENTAL_TIMESTAMP);
        if (StringUtils.isEmpty(timeStamp)) {
            return null;
        }
        return ZonedDateTime.parse(timeStamp, DateTimeFormatter.ISO_ZONED_DATE_TIME).toInstant();
    }

    @Override
    public boolean isBulkCopyEnabled() {
        return getBooleanProperty(CommercemigrationConstants.MIGRATION_DATA_BULKCOPY_ENABLED);
    }

    @Override
    public int getDataPipeTimeout() {
        return getNumericProperty(CommercemigrationConstants.MIGRATION_DATA_PIPE_TIMEOUT);
    }

    @Override
    public int getDataPipeCapacity() {
        return getNumericProperty(CommercemigrationConstants.MIGRATION_DATA_PIPE_CAPACITY);
    }

    @Override
    public String getMigrationReportConnectionString() {
        return getStringProperty(CommercemigrationConstants.MIGRATION_DATA_REPORT_CONNECTIONSTRING);
    }

    @Override
    public int getMaxTargetStagedMigrations() {
        return getNumericProperty(CommercemigrationConstants.MIGRATION_TARGET_MAX_STAGE_MIGRATIONS);
    }

    @Override
    public int getStalledTimeout() {
        return getNumericProperty(CommercemigrationConstants.MIGRATION_STALLED_TIMEOUT);
    }

    protected boolean getBooleanProperty(final String key) {
        return configuration.getBoolean(key);
    }

    protected int getNumericProperty(final String key) {
        return configuration.getInt(key);
    }

    protected String getStringProperty(final String key) {
        return configuration.getString(key);
    }

    private Set<String> getListProperty(final String key) {
        final String tables = configuration.getString(key);

        if (StringUtils.isEmpty(tables)) {
            return Collections.emptySet();
        }

        final Set<String> result = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        final String[] tablesArray = tables.split(",");
        result.addAll(Arrays.stream(tablesArray).collect(Collectors.toSet()));

        return result;
    }

    private Map<String, Set<String>> getDynamicPropertyKeys(final String key) {
        final Map<String, Set<String>> map = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        final Configuration subset = configuration.subset(key);
        final Iterator<String> keys = subset.getKeys();
        while (keys.hasNext()) {
            final String current = keys.next();
            map.put(current, getListProperty(key + "." + current));
        }
        return map;
    }

    private Map<String, String[]> getDynamicPropertyKeysValue(final String key) {
        final Map<String, String[]> map = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        final Configuration subset = configuration.subset(key);
        final Iterator<String> keys = subset.getKeys();

        while (keys.hasNext()) {
            final String current = keys.next();
            final String params = configuration.getString(key + "." + current);
            final String[] paramsArray = params.split(",");
            map.put(current, paramsArray);
        }
        return map;
    }

}
