package org.sap.move.commercemigrationtest;

public interface DatabaseHolder {
    String getId();

    boolean isInitialized();

    void setInitialized(boolean initialized);

    String getSchemaName();

    String getUserName();

    String getConnectionString();

    boolean isRunning();

    void create();

    void destroy();

    String getDriverClassName();

    String getPassword();
}
