package org.sap.move.commercemigrationtest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.HANAContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.OracleContainer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

// use singleton to start DBs only _once_
// 'ryuk' container provided by testcontainers framework does the cleanup for us
// https://www.testcontainers.org/test_framework_integration/manual_lifecycle_control/#singleton-containers
public class TestcontainerHolder {
    private static final Logger LOG = LoggerFactory.getLogger(TestcontainerHolder.class);

    private static Map<TYPE, JdbcDatabaseContainer> INSTANCES = new ConcurrentHashMap<>();

    public static JdbcDatabaseContainer get(TYPE type) {
        return INSTANCES.computeIfAbsent(type, t -> {
            JdbcDatabaseContainer container;
            LOG.info("Bootstrapping {}", t);
            switch (t) {
                case MSSQL:
                    container = new MSSQLServerContainer("mcr.microsoft.com/mssql/server:2019-latest").withInitScript("mssql_init.sql");
                    break;
                case MYSQL:
                    MySQLContainer mySQLContainer = new MySQLContainer("mysql:5.7");
                    mySQLContainer.withCommand("--default-authentication-plugin=mysql_native_password");
                    container = mySQLContainer;
                    break;
                case ORACLE:
                    container = new OracleContainer("oracleinanutshell/oracle-xe-11g");
                    break;
                case HANA:
                    container = new HANAContainer("store/saplabs/hanaexpress:2.00.045.00.20200121.1");
                    ((HANAContainer) container).acceptLicense();
                    container.withStartupTimeoutSeconds(60000);
                    break;
                default:
                    throw new IllegalStateException();
            }
            container.start();
            return container;
        });
    }

    public enum TYPE {
        MSSQL, MYSQL, ORACLE, HANA
    }
}
