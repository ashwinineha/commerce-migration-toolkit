package org.sap.move.commercemigrationtest;

import java.text.MessageFormat;

public class HanaDatabaseHolder extends AbstractNativeDbHolder {

    public static final int DEFAULT_PORT = 39017;

    public HanaDatabaseHolder(String id, String jdbc, String username, String password) {
        super(id, jdbc, username, password);
    }

    @Override
    protected int defaultPort() {
        //System DB
        return DEFAULT_PORT;
    }

    @Override
    protected String setupSchemaScript() {
        return MessageFormat.format("CREATE USER {0} PASSWORD \"{1}\" NO FORCE_FIRST_PASSWORD_CHANGE;" +
                "ALTER USER {0} DISABLE PASSWORD LIFETIME;", this.database, DEFAULT_CI_PASS);
    }

    @Override
    protected String adminJDBUrl() {
        return String.format("jdbc:sap://%s:%d/?reconnect=true", this.host, this.port);
    }

    @Override
    protected String teardownSchemaScript() {
        return MessageFormat.format("DROP USER {0} CASCADE", this.database);
    }

    @Override
    public String getSchemaName() {
        return this.database.toUpperCase();
    }

    @Override
    public String getConnectionString() {
        return String.format("jdbc:sap://%s:%d/?currentSchema=%s&reconnect=true&statementCacheSize=512", this.host, this.port, this.database.toUpperCase());
    }

    @Override
    public String getDriverClassName() {
        return "com.sap.db.jdbc.Driver";
    }

    @Override
    public String getUserName() {
        return this.database;
    }
}
