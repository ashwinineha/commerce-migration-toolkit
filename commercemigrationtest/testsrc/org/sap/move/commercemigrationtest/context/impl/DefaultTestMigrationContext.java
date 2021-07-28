package org.sap.move.commercemigrationtest.context.impl;

import org.sap.commercemigration.profile.DataSourceConfiguration;
import org.sap.commercemigration.repository.DataRepository;
import org.sap.commercemigration.repository.impl.DataRepositoryFactory;
import org.sap.move.commercemigrationtest.context.TestMigrationContext;

import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class DefaultTestMigrationContext implements TestMigrationContext {

    private final DataRepository dataSourceRepository;
    private final DataRepository dataTargetRepository;

    private final boolean migrationTriggeredByInitProcess = false;
    private final Set<String> truncateExcludedTables = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    private final Map<String, Set<String>> excludedColumns = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    private final Map<String, String[]> dataTypeCheck = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    private final Map<String, Set<String>> nullifyColumns = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    private final Set<String> customTables = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    private final Set<String> excludedTables = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    private final Set<String> includedTables = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    private final Set<String> disableAllIndexesIncludedTables = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    private boolean schemaMigrationEnabled = true;
    private boolean addMissingTablesToSchemaEnabled = false;
    private boolean removeMissingTablesToSchemaEnabled = false;
    private boolean addMissingColumnsToSchemaEnabled = true;
    private boolean removeMissingColumnsToSchemaEnabled = true;
    private boolean schemaMigrationAutoTriggerEnabled = false;
    private int readerBatchSize = 100;
    private boolean truncateEnabled = true;
    private boolean auditTableMigrationEnabled = true;
    private int maxParallelReaderWorkers = 2;
    private int maxParallelWriterWorkers = 6;
    private int maxWorkerRetryAttempts = 3;
    private int maxParallelTableCopy = 5;
    private boolean failOnErrorEnabled = false;
    private boolean dropAllIndexesEnabled = false;
    private boolean disableAllIndexesEnabled = false;
    private boolean clusterMode = false;
    private boolean isIncrementalModeEnabled = false;
    private Set<String> incrementalTables = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    private Instant incrementalTimestamp;
    private boolean isBulkCopyEnabled = false;
    private int dataPipeTimeout = 600;
    private int dataPipeCapacity = 100;
    private int stalledTimeout = 600;
    private String migrationReportConnectionString;
    private int maxTargetStagedMigrations = 10;

    public DefaultTestMigrationContext(DataSourceConfiguration sourceDataSourceConfiguration, DataSourceConfiguration targetDataSourceConfiguration, DataRepositoryFactory dataRepositoryFactory) throws Exception {
        this.dataSourceRepository = dataRepositoryFactory.create(sourceDataSourceConfiguration);
        this.dataTargetRepository = dataRepositoryFactory.create(targetDataSourceConfiguration);
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
        return migrationTriggeredByInitProcess;
    }

    @Override
    public boolean isSchemaMigrationEnabled() {
        return schemaMigrationEnabled;
    }

    @Override
    public void setSchemaMigrationEnabled(boolean schemaMigrationEnabled) {
        this.schemaMigrationEnabled = schemaMigrationEnabled;
    }

    @Override
    public boolean isAddMissingTablesToSchemaEnabled() {
        return addMissingTablesToSchemaEnabled;
    }

    @Override
    public void setAddMissingTablesToSchemaEnabled(boolean addMissingTablesToSchemaEnabled) {
        this.addMissingTablesToSchemaEnabled = addMissingTablesToSchemaEnabled;
    }

    @Override
    public boolean isRemoveMissingTablesToSchemaEnabled() {
        return removeMissingTablesToSchemaEnabled;
    }

    @Override
    public void setRemoveMissingTablesToSchemaEnabled(boolean removeMissingTablesToSchemaEnabled) {
        this.removeMissingTablesToSchemaEnabled = removeMissingTablesToSchemaEnabled;
    }

    @Override
    public boolean isSchemaMigrationAutoTriggerEnabled() {
        return schemaMigrationAutoTriggerEnabled;
    }

    @Override
    public void setSchemaMigrationAutoTriggerEnabled(boolean schemaMigrationAutoTriggerEnabled) {
        this.schemaMigrationAutoTriggerEnabled = schemaMigrationAutoTriggerEnabled;
    }

    @Override
    public boolean isAddMissingColumnsToSchemaEnabled() {
        return addMissingColumnsToSchemaEnabled;
    }

    @Override
    public void setAddMissingColumnsToSchemaEnabled(boolean addMissingColumnsToSchemaEnabled) {
        this.addMissingColumnsToSchemaEnabled = addMissingColumnsToSchemaEnabled;
    }

    @Override
    public boolean isRemoveMissingColumnsToSchemaEnabled() {
        return removeMissingColumnsToSchemaEnabled;
    }

    @Override
    public void setRemoveMissingColumnsToSchemaEnabled(boolean removeMissingColumnsToSchemaEnabled) {
        this.removeMissingColumnsToSchemaEnabled = removeMissingColumnsToSchemaEnabled;
    }

    @Override
    public int getReaderBatchSize() {
        return readerBatchSize;
    }

    @Override
    public void setReaderBatchSize(int readerBatchSize) {
        this.readerBatchSize = readerBatchSize;
    }

    @Override
    public boolean isTruncateEnabled() {
        return truncateEnabled;
    }

    @Override
    public void setTruncateEnabled(boolean truncateEnabled) {
        this.truncateEnabled = truncateEnabled;
    }

    @Override
    public boolean isAuditTableMigrationEnabled() {
        return auditTableMigrationEnabled;
    }

    @Override
    public void setAuditTableMigrationEnabled(boolean auditTableMigrationEnabled) {
        this.auditTableMigrationEnabled = auditTableMigrationEnabled;
    }

    @Override
    public Set<String> getTruncateExcludedTables() {
        return truncateExcludedTables;
    }

    @Override
    public int getMaxParallelReaderWorkers() {
        return maxParallelReaderWorkers;
    }

    @Override
    public void setMaxParallelReaderWorkers(int maxParallelReaderWorkers) {
        this.maxParallelReaderWorkers = maxParallelReaderWorkers;
    }

    @Override
    public int getMaxParallelWriterWorkers() {
        return maxParallelWriterWorkers;
    }

    @Override
    public void setMaxParallelWriterWorkers(int maxParallelWriterWorkers) {
        this.maxParallelWriterWorkers = maxParallelWriterWorkers;
    }

    @Override
    public int getMaxWorkerRetryAttempts() {
        return maxWorkerRetryAttempts;
    }

    @Override
    public void setMaxWorkerRetryAttempts(int maxWorkerRetryAttempts) {
        this.maxWorkerRetryAttempts = maxWorkerRetryAttempts;
    }

    @Override
    public int getMaxParallelTableCopy() {
        return maxParallelTableCopy;
    }

    @Override
    public void setMaxParallelTableCopy(int maxParallelTableCopy) {
        this.maxParallelTableCopy = maxParallelTableCopy;
    }

    @Override
    public boolean isFailOnErrorEnabled() {
        return failOnErrorEnabled;
    }

    @Override
    public void setFailOnErrorEnabled(boolean failOnErrorEnabled) {
        this.failOnErrorEnabled = failOnErrorEnabled;
    }


    @Override
    public Map<String, Set<String>> getExcludedColumns() {
        return excludedColumns;
    }

    @Override
    public Map<String, Set<String>> getNullifyColumns() {
        return nullifyColumns;
    }

    @Override
    public Set<String> getCustomTables() {
        return customTables;
    }

    @Override
    public Set<String> getExcludedTables() {
        return excludedTables;
    }

    @Override
    public Set<String> getIncludedTables() {
        return includedTables;
    }

    @Override
    public boolean isDropAllIndexesEnabled() {
        return dropAllIndexesEnabled;
    }

    @Override
    public void setDropAllIndexesEnabled(boolean dropAllIndexesEnabled) {
        this.dropAllIndexesEnabled = dropAllIndexesEnabled;
    }

    @Override
    public boolean isDisableAllIndexesEnabled() {
        return disableAllIndexesEnabled;
    }

    @Override
    public void setDisableAllIndexesEnabled(boolean disableAllIndexesEnabled) {
        this.disableAllIndexesEnabled = disableAllIndexesEnabled;
    }

    @Override
    public Set<String> getDisableAllIndexesIncludedTables() {
        return disableAllIndexesIncludedTables;
    }

    @Override
    public boolean isClusterMode() {
        return clusterMode;
    }

    @Override
    public void setClusterMode(boolean clusterMode) {
        this.clusterMode = clusterMode;
    }

    @Override
    public boolean isIncrementalModeEnabled() {
        return isIncrementalModeEnabled;
    }

    @Override
    public void setIncrementalModeEnabled(boolean incrementalModeEnabled) {
        isIncrementalModeEnabled = incrementalModeEnabled;
    }

    @Override
    public Set<String> getIncrementalTables() {
        return incrementalTables;
    }

    @Override
    public void setIncrementalTables(Set<String> incrementalTables) {
        this.incrementalTables = incrementalTables;
    }

    @Override
    public Instant getIncrementalTimestamp() {
        return incrementalTimestamp;
    }

    @Override
    public void setIncrementalTimestamp(Instant incrementalTimestamp) {
        this.incrementalTimestamp = incrementalTimestamp;
    }

    @Override
    public boolean isBulkCopyEnabled() {
        return isBulkCopyEnabled;
    }

    @Override
    public void setBulkCopyEnabled(boolean bulkCopyEnabled) {
        isBulkCopyEnabled = bulkCopyEnabled;
    }

    @Override
    public int getDataPipeTimeout() {
        return dataPipeTimeout;
    }

    @Override
    public void setDataPipeTimeout(int dataPipeTimeout) {
        this.dataPipeTimeout = dataPipeTimeout;
    }

    @Override
    public int getDataPipeCapacity() {
        return dataPipeCapacity;
    }

    @Override
    public void setDataPipeCapacity(int dataPipeCapacity) {
        this.dataPipeCapacity = dataPipeCapacity;
    }

    @Override
    public int getStalledTimeout() {
        return this.stalledTimeout;
    }

    @Override
    public void setStalledTimeout(int stalledTimeout) {
        this.stalledTimeout = stalledTimeout;
    }

    @Override
    public String getMigrationReportConnectionString() {
        return migrationReportConnectionString;
    }

    @Override
    public void setMigrationReportConnectionString(String migrationReportConnectionString) {
        this.migrationReportConnectionString = migrationReportConnectionString;
    }

    @Override
    public int getMaxTargetStagedMigrations() {
        return maxTargetStagedMigrations;
    }

    public void setMaxTargetStagedMigrations(int maxTargetStagedMigrations) {
        this.maxTargetStagedMigrations = maxTargetStagedMigrations;
    }
}
