package org.sap.move.commercemigrationtest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.Properties;

public class OracleDatabaseHolder extends AbstractNativeDbHolder {

    public static final int DEFAULT_PORT = 1521;

    public OracleDatabaseHolder(String id, String jdbc, String username, String password) {
        super(id, jdbc, username, password);
    }

    @Override
    protected int defaultPort() {
        return DEFAULT_PORT;
    }

    @Override
    public String getUserName() {
        return this.database.toUpperCase();
    }

    @Override
    protected String setupSchemaScript() {
        String script = MessageFormat.format("CREATE USER {0} IDENTIFIED BY \"{1}\";\n" +
                "GRANT CREATE SESSION, ALTER SESSION, CREATE DATABASE LINK,\n" +
                " CREATE MATERIALIZED VIEW, CREATE PROCEDURE, CREATE PUBLIC SYNONYM,\n" +
                " CREATE ROLE, CREATE SEQUENCE, CREATE SYNONYM, CREATE TABLE,\n" +
                " CREATE TRIGGER, CREATE TYPE, CREATE VIEW, UNLIMITED TABLESPACE\n" +
                " TO {0};\n" +
                "ALTER USER {0} default tablespace USERS quota unlimited on USERS;", this.database.toUpperCase(), DEFAULT_CI_PASS);
        return script;
    }

    @Override
    protected String adminJDBUrl() {
        return String.format("jdbc:oracle:thin:@%s:%d:xe", this.host, this.port);
    }

    @Override
    protected String teardownSchemaScript() {
        String script = "DECLARE\n" +
                "  open_count integer;\n" +
                "BEGIN\n" +
                //-- prevent any further connections
                "  EXECUTE IMMEDIATE 'alter user {0} account lock';\n" +
                //  --kill all sessions
                "  FOR session IN (SELECT sid, serial# \n" +
                "                  FROM  v$session \n" +
                "                  WHERE username = '{0}')\n" +
                "  LOOP\n" +
                //    -- the most brutal way to kill a session
                "    EXECUTE IMMEDIATE 'alter system disconnect session ''' || session.sid || ',' || session.serial# || ''' immediate';\n" +
                "  END LOOP;\n" +
                //  -- killing is done in the background, so we need to wait a bit
                "  LOOP\n" +
                "    SELECT COUNT(*) \n" +
                "      INTO open_count \n" +
                "      FROM  v$session WHERE username = '{0}';\n" +
                "    EXIT WHEN open_count = 0;\n" +
                "    dbms_lock.sleep(0.5);\n" +
                "  END LOOP;\n" +
                // -- finally, it is safe to issue the drop statement (in the background, we don't want to wait for it)
                " DBMS_SCHEDULER.CREATE_JOB(\n" +
                "   job_name          =>  '{0}',\n" +
                "   enabled => TRUE,\n" +
                "   job_type          =>  'PLSQL_BLOCK',\n" +
                "   job_action        =>  'execute immediate ''drop user {0} cascade'';'\n" +
                "  );\n" +
                "END;";
        script = script.replaceAll("\\{0}", this.database.toUpperCase());
        return script;
    }

    @Override
    public void destroy() {
        String script = teardownSchemaScript();
        Properties connectionProps = new Properties();
        connectionProps.put("user", this.adminUsr);
        connectionProps.put("password", this.adminPsw);
        System.out.printf("Dropping schema %s...\n", this.database);
        try (Connection conn = DriverManager.getConnection(adminJDBUrl(), connectionProps)) {
            try (Statement statement = conn.createStatement()) {
                statement.executeUpdate(script);
            }
        } catch (SQLException throwables) {
            throw new RuntimeException(throwables);
        }
        running = false;
    }

    @Override
    public String getSchemaName() {
        return this.database.toUpperCase();
    }

    @Override
    public String getConnectionString() {
        return String.format("jdbc:oracle:thin:@%s:%d:xe", this.host, this.port);
    }

    @Override
    public String getDriverClassName() {
        return "oracle.jdbc.driver.OracleDriver";
    }
}
