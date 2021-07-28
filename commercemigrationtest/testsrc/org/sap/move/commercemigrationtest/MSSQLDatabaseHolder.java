package org.sap.move.commercemigrationtest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class MSSQLDatabaseHolder extends AbstractNativeDbHolder {

    public static final int DEFAULT_PORT = 1433;

    public MSSQLDatabaseHolder(String id, String jdbc, String username, String password) {
        super(id, jdbc, username, password);
    }

    @Override
    public String getSchemaName() {
        return "dbo";
    }

    @Override
    public String getConnectionString() {
        return String.format("jdbc:sqlserver://%s:%d;databaseName=%s;disableStatementPooling=false;statementPoolingCacheSize=1000;", this.host, this.port, this.database);
    }

    @Override
    protected String setupSchemaScript() {
        String script = MessageFormat.format("DROP DATABASE IF EXISTS {0};\n" +
                "CREATE DATABASE {0} containment = partial;\n" +
                "ALTER DATABASE {0} SET READ_COMMITTED_SNAPSHOT ON;\n" +
                "ALTER DATABASE {0} SET ALLOW_SNAPSHOT_ISOLATION ON;\n", this.database);
        return script;
    }

    @Override
    public void create() {
        super.create();
        Properties connectionProps = new Properties();
        connectionProps.put("user", this.adminUsr);
        connectionProps.put("password", this.adminPsw);
        List<String> userStatements = Arrays.asList(
                MessageFormat.format("USE {0}; \n" +
                        "CREATE USER {0} WITH PASSWORD = ''{1}''; \n", this.database, DEFAULT_CI_PASS),
                MessageFormat.format("USE {0}; \n" +
                        "ALTER ROLE db_owner ADD MEMBER {0}; \n", this.database)
        );
        for (String userStatement : userStatements) {
            try (Connection conn = DriverManager.getConnection(adminJDBUrl(), connectionProps)) {
                try (Statement statement = conn.createStatement()) {
                    statement.execute(userStatement);
                    conn.commit();
                }
            } catch (SQLException exception) {
                throw new RuntimeException(exception);
            }
        }
    }

    @Override
    protected String adminJDBUrl() {
        return String.format("jdbc:sqlserver://%s:%d;", this.host, this.port);
    }

    @Override
    protected int defaultPort() {
        return DEFAULT_PORT;
    }

    @Override
    protected String teardownSchemaScript() {
        String script = MessageFormat.format("USE master;\n" +
                "ALTER DATABASE {0} SET SINGLE_USER WITH ROLLBACK IMMEDIATE;\n" +
                "DROP DATABASE {0};\n" +
                "DROP USER IF EXISTS {0}; ", this.database);
        return script;
    }

    @Override
    public String getDriverClassName() {
        return "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    }

    @Override
    public String getUserName() {
        return this.database;
    }
}
