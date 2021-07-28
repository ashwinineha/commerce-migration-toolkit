package org.sap.move.commercemigrationtest.configuration;

import org.sap.commercemigration.profile.DataSourceConfiguration;

public class TestDataSourceConfiguration implements DataSourceConfiguration {

    private String profile;
    private String driver;
    private String connectionString;
    private String userName;
    private String password;
    private String schema;
    private String catalog;
    private String tablePrefix;
    private String typeSystemName;
    private String typeSystemSuffix;
    private int maxActive = 90;
    private int maxIdle = 90;
    private int minIdle = 2;
    private boolean removeAbandoned;

    public TestDataSourceConfiguration(String profile) {
        this.profile = profile;
    }

    @Override
    public String getProfile() {
        return profile;
    }

    public void setProfile(String profile) {
        this.profile = profile;
    }

    @Override
    public String getDriver() {
        return driver;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }

    @Override
    public String getConnectionString() {
        return connectionString;
    }

    public void setConnectionString(String connectionString) {
        this.connectionString = connectionString;
    }

    @Override
    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    @Override
    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    @Override
    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    @Override
    public String getCatalog() {
        return catalog;
    }

    public void setCatalog(String catalog) {
        this.catalog = catalog;
    }

    @Override
    public String getTablePrefix() {
        return tablePrefix;
    }

    public void setTablePrefix(String tablePrefix) {
        this.tablePrefix = tablePrefix;
    }

    @Override
    public int getMaxActive() {
        return maxActive;
    }

    public void setMaxActive(int maxActive) {
        this.maxActive = maxActive;
    }

    @Override
    public int getMaxIdle() {
        return maxIdle;
    }

    public void setMaxIdle(int maxIdle) {
        this.maxIdle = maxIdle;
    }

    @Override
    public int getMinIdle() {
        return minIdle;
    }

    public void setMinIdle(int minIdle) {
        this.minIdle = minIdle;
    }

    @Override
    public boolean isRemoveAbandoned() {
        return removeAbandoned;
    }

    public void setRemoveAbandoned(boolean removeAbandoned) {
        this.removeAbandoned = removeAbandoned;
    }

    @Override
    public String getTypeSystemName() {
        return typeSystemName;
    }

    public void setTypeSystemName(String typeSystemName) {
        this.typeSystemName = typeSystemName;
    }

    @Override
    public String getTypeSystemSuffix() {
        return typeSystemSuffix;
    }

    public void setTypeSystemSuffix(String typeSystemSuffix) {
        this.typeSystemSuffix = typeSystemSuffix;
    }
}
