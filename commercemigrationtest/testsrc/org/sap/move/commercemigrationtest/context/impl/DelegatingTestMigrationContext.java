package org.sap.move.commercemigrationtest.context.impl;

import org.sap.commercemigration.repository.DataRepository;
import org.sap.move.commercemigrationtest.context.TestMigrationContext;

import java.time.Instant;
import java.util.Map;
import java.util.Set;

public class DelegatingTestMigrationContext implements TestMigrationContext {

    private TestMigrationContext context;

    @Override
    public DataRepository getDataSourceRepository() {
        return context.getDataSourceRepository();
    }

    @Override
    public DataRepository getDataTargetRepository() {
        return context.getDataTargetRepository();
    }

    @Override
    public boolean isMigrationTriggeredByUpdateProcess() {
        return context.isMigrationTriggeredByUpdateProcess();
    }

    @Override
    public boolean isSchemaMigrationEnabled() {
        return context.isSchemaMigrationEnabled();
    }

    @Override
    public void setSchemaMigrationEnabled(boolean schemaMigrationEnabled) {
        context.setSchemaMigrationEnabled(schemaMigrationEnabled);
    }

    @Override
    public boolean isAddMissingTablesToSchemaEnabled() {
        return context.isAddMissingTablesToSchemaEnabled();
    }

    @Override
    public void setAddMissingTablesToSchemaEnabled(boolean addMissingTablesToSchemaEnabled) {
        context.setAddMissingTablesToSchemaEnabled(addMissingTablesToSchemaEnabled);
    }

    @Override
    public boolean isRemoveMissingTablesToSchemaEnabled() {
        return context.isRemoveMissingTablesToSchemaEnabled();
    }

    @Override
    public void setRemoveMissingTablesToSchemaEnabled(boolean removeMissingTablesToSchemaEnabled) {
        context.setRemoveMissingTablesToSchemaEnabled(removeMissingTablesToSchemaEnabled);
    }

    @Override
    public boolean isSchemaMigrationAutoTriggerEnabled() {
        return context.isSchemaMigrationAutoTriggerEnabled();
    }

    @Override
    public void setSchemaMigrationAutoTriggerEnabled(boolean schemaMigrationAutoTriggerEnabled) {
        context.setSchemaMigrationAutoTriggerEnabled(schemaMigrationAutoTriggerEnabled);
    }

    @Override
    public boolean isAddMissingColumnsToSchemaEnabled() {
        return context.isAddMissingColumnsToSchemaEnabled();
    }

    @Override
    public void setAddMissingColumnsToSchemaEnabled(boolean addMissingColumnsToSchemaEnabled) {
        context.setAddMissingColumnsToSchemaEnabled(addMissingColumnsToSchemaEnabled);
    }

    @Override
    public boolean isRemoveMissingColumnsToSchemaEnabled() {
        return context.isRemoveMissingColumnsToSchemaEnabled();
    }

    @Override
    public void setRemoveMissingColumnsToSchemaEnabled(boolean removeMissingColumnsToSchemaEnabled) {
        context.setRemoveMissingColumnsToSchemaEnabled(removeMissingColumnsToSchemaEnabled);
    }

    @Override
    public int getReaderBatchSize() {
        return context.getReaderBatchSize();
    }

    @Override
    public void setReaderBatchSize(int readerBatchSize) {
        context.setReaderBatchSize(readerBatchSize);
    }

    @Override
    public boolean isTruncateEnabled() {
        return context.isTruncateEnabled();
    }

    @Override
    public void setTruncateEnabled(boolean truncateEnabled) {
        context.setTruncateEnabled(truncateEnabled);
    }

    @Override
    public boolean isAuditTableMigrationEnabled() {
        return context.isAuditTableMigrationEnabled();
    }

    @Override
    public void setAuditTableMigrationEnabled(boolean auditTableMigrationEnabled) {
        context.setAuditTableMigrationEnabled(auditTableMigrationEnabled);
    }

    @Override
    public Set<String> getTruncateExcludedTables() {
        return context.getTruncateExcludedTables();
    }

    @Override
    public int getMaxParallelReaderWorkers() {
        return context.getMaxParallelReaderWorkers();
    }

    @Override
    public void setMaxParallelReaderWorkers(int maxParallelReaderWorkers) {
        context.setMaxParallelReaderWorkers(maxParallelReaderWorkers);
    }

    @Override
    public int getMaxParallelWriterWorkers() {
        return context.getMaxParallelWriterWorkers();
    }

    @Override
    public void setMaxParallelWriterWorkers(int maxParallelWriterWorkers) {
        context.setMaxParallelWriterWorkers(maxParallelWriterWorkers);
    }

    @Override
    public int getMaxWorkerRetryAttempts() {
        return context.getMaxWorkerRetryAttempts();
    }

    @Override
    public void setMaxWorkerRetryAttempts(int maxWorkerRetryAttempts) {
        context.setMaxWorkerRetryAttempts(maxWorkerRetryAttempts);
    }

    @Override
    public int getMaxParallelTableCopy() {
        return context.getMaxParallelTableCopy();
    }

    @Override
    public void setMaxParallelTableCopy(int maxParallelTableCopy) {
        context.setMaxParallelTableCopy(maxParallelTableCopy);
    }

    @Override
    public boolean isFailOnErrorEnabled() {
        return context.isFailOnErrorEnabled();
    }

    @Override
    public void setFailOnErrorEnabled(boolean failOnErrorEnabled) {
        context.setFailOnErrorEnabled(failOnErrorEnabled);
    }

    @Override
    public Map<String, Set<String>> getExcludedColumns() {
        return context.getExcludedColumns();
    }

    @Override
    public Map<String, Set<String>> getNullifyColumns() {
        return context.getNullifyColumns();
    }

    @Override
    public Set<String> getCustomTables() {
        return context.getCustomTables();
    }

    @Override
    public Set<String> getExcludedTables() {
        return context.getExcludedTables();
    }

    @Override
    public Set<String> getIncludedTables() {
        return context.getIncludedTables();
    }

    @Override
    public boolean isDropAllIndexesEnabled() {
        return context.isDropAllIndexesEnabled();
    }

    @Override
    public void setDropAllIndexesEnabled(boolean dropAllIndexesEnabled) {
        context.setDropAllIndexesEnabled(dropAllIndexesEnabled);
    }

    @Override
    public boolean isDisableAllIndexesEnabled() {
        return context.isDisableAllIndexesEnabled();
    }

    @Override
    public void setDisableAllIndexesEnabled(boolean disableAllIndexesEnabled) {
        context.setDisableAllIndexesEnabled(disableAllIndexesEnabled);
    }

    @Override
    public Set<String> getDisableAllIndexesIncludedTables() {
        return context.getDisableAllIndexesIncludedTables();
    }

    @Override
    public boolean isClusterMode() {
        return context.isClusterMode();
    }

    @Override
    public void setClusterMode(boolean clusterMode) {
        context.setClusterMode(clusterMode);
    }

    @Override
    public boolean isIncrementalModeEnabled() {
        return context.isIncrementalModeEnabled();
    }

    @Override
    public void setIncrementalModeEnabled(boolean incrementalModeEnabled) {
        context.setIncrementalModeEnabled(incrementalModeEnabled);
    }

    @Override
    public Set<String> getIncrementalTables() {
        return context.getIncrementalTables();
    }

    @Override
    public void setIncrementalTables(Set<String> incrementalTables) {
        context.setIncrementalTables(incrementalTables);
    }

    @Override
    public Instant getIncrementalTimestamp() {
        return context.getIncrementalTimestamp();
    }

    @Override
    public void setIncrementalTimestamp(Instant incrementalTimestamp) {
        context.setIncrementalTimestamp(incrementalTimestamp);
    }

    @Override
    public boolean isBulkCopyEnabled() {
        return context.isBulkCopyEnabled();
    }

    @Override
    public void setBulkCopyEnabled(boolean bulkCopyEnabled) {
        context.setBulkCopyEnabled(bulkCopyEnabled);
    }

    @Override
    public int getDataPipeTimeout() {
        return context.getDataPipeTimeout();
    }

    @Override
    public void setDataPipeTimeout(int dataPipeTimeout) {
        context.setDataPipeTimeout(dataPipeTimeout);
    }

    @Override
    public int getDataPipeCapacity() {
        return context.getDataPipeCapacity();
    }

    @Override
    public void setDataPipeCapacity(int dataPipeCapacity) {
        context.setDataPipeCapacity(dataPipeCapacity);
    }

    @Override
    public int getStalledTimeout() {
        return context.getStalledTimeout();
    }

    @Override
    public void setStalledTimeout(int stalledTimeout) {
        context.setStalledTimeout(stalledTimeout);
    }

    @Override
    public String getMigrationReportConnectionString() {
        return context.getMigrationReportConnectionString();
    }

    @Override
    public void setMigrationReportConnectionString(String connectionString) {
        context.setMigrationReportConnectionString(connectionString);
    }

    @Override
    public int getMaxTargetStagedMigrations() {
        return context.getMaxTargetStagedMigrations();
    }

    @Override
    public boolean isDeletionEnabled() {
        return false;
    }

    @Override
    public void setMaxTargetStagedMigrations(int maxTargetStagedMigrations) {
        context.setMaxTargetStagedMigrations(maxTargetStagedMigrations);
    }

    public TestMigrationContext getContext() {
        return context;
    }

    public void setContext(TestMigrationContext context) {
        this.context = context;
    }
}
