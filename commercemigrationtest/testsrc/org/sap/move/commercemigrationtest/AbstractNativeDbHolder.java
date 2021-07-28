package org.sap.move.commercemigrationtest;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.support.EncodedResource;
import org.springframework.jdbc.datasource.init.ScriptUtils;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;

public abstract class AbstractNativeDbHolder implements DatabaseHolder {

    protected static final String DEFAULT_CI_PASS = "DefaultCIUserPass1!";
    private static final Logger LOG = LoggerFactory.getLogger(AbstractNativeDbHolder.class);
    protected final String id;
    protected final String database;
    protected final String adminUsr;
    protected final String adminPsw;
    protected final String host;
    protected final int port;
    protected boolean initialized = false;
    protected boolean running = false;

    public AbstractNativeDbHolder(String id, String host, String adminUsr, String adminPsw) {
        Objects.requireNonNull(host, "host url must not be null");
        Objects.requireNonNull(adminUsr, "username must not be null");
        Objects.requireNonNull(adminPsw, "password must not be null");

        this.id = id;
        this.database = "cmt" + DigestUtils.md5Hex(UUID.randomUUID().toString()).substring(0, 27);
        this.adminUsr = adminUsr;
        this.adminPsw = adminPsw;

        final String[] split = host.split(":");
        this.host = split[0];
        this.port = (split.length < 2 || split[1] == null) ? defaultPort() : Integer.parseInt(split[1]);
    }

    protected abstract int defaultPort();

    @Override
    public String getId() {
        return id;
    }

    @Override
    public boolean isInitialized() {
        return initialized;
    }

    @Override
    public void setInitialized(boolean initialized) {
        this.initialized = initialized;
    }

    @Override
    public String getPassword() {
        return DEFAULT_CI_PASS;
    }

    @Override
    public boolean isRunning() {
        return this.running;
    }

    @Override
    public void create() {
        String script = setupSchemaScript();
        Properties connectionProps = new Properties();
        connectionProps.put("user", this.adminUsr);
        connectionProps.put("password", this.adminPsw);
        LOG.info("Creating schema {}", this.database);
        try (Connection conn = DriverManager.getConnection(adminJDBUrl(), connectionProps)) {
            ScriptUtils.executeSqlScript(conn, new EncodedResource(new ByteArrayResource(script.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        running = true;
    }

    protected abstract String setupSchemaScript();

    protected abstract String adminJDBUrl();

    @Override
    public void destroy() {
        String script = teardownSchemaScript();
        Properties connectionProps = new Properties();
        connectionProps.put("user", this.adminUsr);
        connectionProps.put("password", this.adminPsw);
        LOG.info("Dropping schema {}", this.database);
        try (Connection conn = DriverManager.getConnection(adminJDBUrl(), connectionProps)) {
            ScriptUtils.executeSqlScript(conn, new EncodedResource(new ByteArrayResource(script.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        running = false;
    }

    protected abstract String teardownSchemaScript();
}
