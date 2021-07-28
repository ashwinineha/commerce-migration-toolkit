package org.sap.move.commercemigrationtest.context;

import org.sap.commercemigration.context.MigrationContext;
import org.sap.commercemigration.repository.DataRepository;

import java.time.Instant;
import java.util.Map;
import java.util.Set;

public interface TestMigrationContext extends MigrationContext {
    @Override
    DataRepository getDataSourceRepository();

    @Override
    DataRepository getDataTargetRepository();

    @Override
    boolean isMigrationTriggeredByUpdateProcess();

    @Override
    boolean isSchemaMigrationEnabled();

    void setSchemaMigrationEnabled(boolean schemaMigrationEnabled);

    @Override
    boolean isAddMissingTablesToSchemaEnabled();

    void setAddMissingTablesToSchemaEnabled(boolean addMissingTablesToSchemaEnabled);

    @Override
    boolean isRemoveMissingTablesToSchemaEnabled();

    void setRemoveMissingTablesToSchemaEnabled(boolean removeMissingTablesToSchemaEnabled);

    @Override
    boolean isSchemaMigrationAutoTriggerEnabled();

    void setSchemaMigrationAutoTriggerEnabled(boolean schemaMigrationAutoTriggerEnabled);

    @Override
    boolean isAddMissingColumnsToSchemaEnabled();

    void setAddMissingColumnsToSchemaEnabled(boolean addMissingColumnsToSchemaEnabled);

    @Override
    boolean isRemoveMissingColumnsToSchemaEnabled();

    void setRemoveMissingColumnsToSchemaEnabled(boolean removeMissingColumnsToSchemaEnabled);

    @Override
    int getReaderBatchSize();

    void setReaderBatchSize(int readerBatchSize);

    @Override
    boolean isTruncateEnabled();

    void setTruncateEnabled(boolean truncateEnabled);

    @Override
    Set<String> getTruncateExcludedTables();

    @Override
    int getMaxParallelReaderWorkers();

    void setMaxParallelReaderWorkers(int maxParallelReaderWorkers);

    @Override
    int getMaxParallelWriterWorkers();

    void setMaxParallelWriterWorkers(int maxParallelWriterWorkers);

    @Override
    int getMaxWorkerRetryAttempts();

    void setMaxWorkerRetryAttempts(int maxWorkerRetryAttempts);

    @Override
    int getMaxParallelTableCopy();

    void setMaxParallelTableCopy(int maxParallelTableCopy);

    @Override
    boolean isFailOnErrorEnabled();

    void setFailOnErrorEnabled(boolean failOnErrorEnabled);

    @Override
    Map<String, Set<String>> getExcludedColumns();

    Map<String, Set<String>> getNullifyColumns();

    @Override
    Set<String> getCustomTables();

    @Override
    Set<String> getExcludedTables();

    @Override
    Set<String> getIncludedTables();

    @Override
    boolean isDropAllIndexesEnabled();

    void setDropAllIndexesEnabled(boolean dropAllIndexesEnabled);

    @Override
    boolean isDisableAllIndexesEnabled();

    void setDisableAllIndexesEnabled(boolean disableAllIndexesEnabled);

    @Override
    Set<String> getDisableAllIndexesIncludedTables();

    @Override
    boolean isClusterMode();

    void setClusterMode(boolean clusterMode);

    @Override
    boolean isIncrementalModeEnabled();

    void setIncrementalModeEnabled(boolean incrementalModeEnabled);

    @Override
    Set<String> getIncrementalTables();

    void setIncrementalTables(Set<String> incrementalTables);

    @Override
    Instant getIncrementalTimestamp();

    void setIncrementalTimestamp(Instant incrementalTimestamp);

    @Override
    boolean isBulkCopyEnabled();

    void setBulkCopyEnabled(boolean bulkCopyEnabled);

    @Override
    int getDataPipeTimeout();

    void setDataPipeTimeout(int dataPipeTimeout);

    @Override
    int getDataPipeCapacity();

    void setDataPipeCapacity(int dataPipeCapacity);

    @Override
    int getStalledTimeout();

    void setStalledTimeout(int stalledTimeout);

    @Override
    String getMigrationReportConnectionString();

    void setMigrationReportConnectionString(String connectionString);

    @Override
    boolean isAuditTableMigrationEnabled();

    void setAuditTableMigrationEnabled(boolean auditTableMigrationEnabled);

    @Override
    int getMaxTargetStagedMigrations();

    void setMaxTargetStagedMigrations(int maxTargetStagedMigrations);
}
