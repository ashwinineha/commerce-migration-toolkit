package org.sap.commercemigration.repository.impl;

import com.google.common.base.Strings;
import org.sap.commercemigration.profile.DataSourceConfiguration;
import org.sap.commercemigration.repository.DataRepository;
import org.sap.commercemigration.service.DatabaseMigrationDataTypeMapperService;

public class DataIncrementalRepositoryFactory extends DataRepositoryFactory {

    public DataIncrementalRepositoryFactory(DatabaseMigrationDataTypeMapperService databaseMigrationDataTypeMapperService) {
        super(databaseMigrationDataTypeMapperService);
    }

    public DataRepository create(DataSourceConfiguration dataSourceConfiguration)
            throws Exception {
        String connectionString = dataSourceConfiguration.getConnectionString();
        if (Strings.isNullOrEmpty(connectionString)) {
            throw new RuntimeException("No connection string provided for data source '" + dataSourceConfiguration.getProfile() + "'");
        } else {
            String connectionStringLower = connectionString.toLowerCase();
            if (connectionStringLower.startsWith("jdbc:mysql")) {
                return new MySQLIncrementalDataRepository(dataSourceConfiguration, databaseMigrationDataTypeMapperService);
            } else if (connectionStringLower.startsWith("jdbc:sqlserver")) {
                return new AzureIncrementalDataRepository(dataSourceConfiguration, databaseMigrationDataTypeMapperService);
            } else if (connectionStringLower.startsWith("jdbc:oracle")) {
                return new OracleDataRepository(dataSourceConfiguration, databaseMigrationDataTypeMapperService);
            } else if (connectionStringLower.startsWith("jdbc:sap")) {
                return new HanaDataRepository(dataSourceConfiguration, databaseMigrationDataTypeMapperService);
            } else if (connectionStringLower.startsWith("jdbc:hsqldb")) {
                return new HsqlRepository(dataSourceConfiguration, databaseMigrationDataTypeMapperService);
            }
        }
        throw new RuntimeException("Cannot handle connection string for " + connectionString);
    }
}
