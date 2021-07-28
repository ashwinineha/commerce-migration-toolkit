package org.sap.move.commercemigrationtest;

import java.text.MessageFormat;

public class MySQLDatabaseHolder extends AbstractNativeDbHolder {

    public static final int DEFAULT_PORT = 3306;

    public MySQLDatabaseHolder(String id, String jdbc, String username, String password) {
        super(id, jdbc, username, password);
    }

    @Override
    protected int defaultPort() {
        return DEFAULT_PORT;
    }

    @Override
    public String getUserName() {
        return this.database;
    }

    @Override
    protected String setupSchemaScript() {
        String script = MessageFormat.format("CREATE DATABASE {0};" +
                "CREATE USER ''{0}''@''%'' IDENTIFIED BY ''{1}'';" +
                "GRANT ALL PRIVILEGES ON {0}.* TO ''{0}''@''%'';" +
                "FLUSH PRIVILEGES;", this.database, DEFAULT_CI_PASS);
        return script;
    }

    @Override
    protected String adminJDBUrl() {
        return String.format("jdbc:mysql://%s:%d", this.host, this.port);
    }

    @Override
    protected String teardownSchemaScript() {
        String script = MessageFormat.format("DROP DATABASE {0};" +
                "DROP USER ''{0}''@''%'';", this.database);
        return script;
    }

    @Override
    public String getSchemaName() {
        return this.database;
    }

    @Override
    public String getConnectionString() {
        return String.format("jdbc:mysql://%s:%d/%s?useConfigs=maxPerformance", this.host, this.port, this.database);
    }

    @Override
    public String getDriverClassName() {
        return "com.mysql.jdbc.Driver";
    }

}
