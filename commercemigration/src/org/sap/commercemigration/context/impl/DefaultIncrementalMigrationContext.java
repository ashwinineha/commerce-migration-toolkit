package org.sap.commercemigration.context.impl;


import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.sap.commercemigration.constants.CommercemigrationConstants;
import org.sap.commercemigration.context.IncrementalMigrationContext;
import org.sap.commercemigration.profile.DataSourceConfiguration;
import org.sap.commercemigration.repository.impl.DataRepositoryFactory;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class DefaultIncrementalMigrationContext extends DefaultMigrationContext implements IncrementalMigrationContext {

    private static final Logger LOG = Logger.getLogger(DefaultIncrementalMigrationContext.class.getName());
    private Instant timestampInstant;
    private Set<String> incrementalTables;
    private Set<String> includedTables;


    public DefaultIncrementalMigrationContext(DataSourceConfiguration sourceDataSourceConfiguration, DataSourceConfiguration targetDataSourceConfiguration, DataRepositoryFactory dataRepositoryFactory, Configuration configuration) throws Exception {
        super(sourceDataSourceConfiguration, targetDataSourceConfiguration, dataRepositoryFactory, configuration);
    }

    @Override
    public Instant getIncrementalMigrationTimestamp() {
        return timestampInstant;
    }

    @Override
    public void setIncrementalMigrationTimestamp(Instant timeStampInstant) {
        this.timestampInstant = timeStampInstant;
    }

    @Override
    public Set<String> setIncrementalTables(Set<String> incrementalTables) {
        return this.incrementalTables = incrementalTables;
    }


    @Override
    public Set<String> getIncrementalTables() {
        return CollectionUtils.isNotEmpty(this.incrementalTables) ?
                this.incrementalTables : getListProperty(CommercemigrationConstants.MIGRATION_DATA_INCREMENTAL_TABLES);
    }

    @Override
    public boolean isIncrementalModeEnabled() {
        return getBooleanProperty(CommercemigrationConstants.MIGRATION_DATA_INCREMENTAL_ENABLED);
    }

    @Override
    public void setIncrementalModeEnabled(boolean incrementalModeEnabled) {
        configuration.setProperty(CommercemigrationConstants.MIGRATION_DATA_INCREMENTAL_ENABLED,
                Boolean.toString(incrementalModeEnabled));
    }


    @Override
    public Instant getIncrementalTimestamp() {
        if (null != getIncrementalMigrationTimestamp()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Here getIncrementalTimestamp(): " + timestampInstant);
            }
            return getIncrementalMigrationTimestamp();
        }
        String timeStamp = getStringProperty(CommercemigrationConstants.MIGRATION_DATA_INCREMENTAL_TIMESTAMP);
        if (StringUtils.isEmpty(timeStamp)) {
            return null;
        }
        return ZonedDateTime.parse(timeStamp, DateTimeFormatter.ISO_ZONED_DATE_TIME).toInstant();
    }

    @Override
    public Set<String> getIncludedTables() {
        if (isIncrementalModeEnabled()) {
            return Collections.emptySet();
        }
        return CollectionUtils.isNotEmpty(includedTables) ? includedTables :
                getListProperty(CommercemigrationConstants.MIGRATION_DATA_TABLES_INCLUDED);
    }

    @Override
    public void setIncludedTables(Set<String> includedTables) {
        this.includedTables = includedTables;
    }

    private Set<String> getListProperty(final String key) {
        final String tables = super.configuration.getString(key);

        if (StringUtils.isEmpty(tables)) {
            return Collections.emptySet();
        }

        final Set<String> result = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        final String[] tablesArray = tables.split(",");
        result.addAll(Arrays.stream(tablesArray).collect(Collectors.toSet()));

        return result;
    }
}
