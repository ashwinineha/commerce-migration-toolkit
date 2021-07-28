package org.sap.move.commercemigrationtest.configuration;

import de.hybris.platform.core.Tenant;
import de.hybris.platform.core.TenantPropertiesLoader;
import org.sap.commercemigration.profile.DataSourceConfiguration;

public class TestTenantPropertiesLoader extends TenantPropertiesLoader {

    private static final String DB_URL = "db.url";
    private static final String DB_DRIVER = "db.driver";
    private static final String DB_USERNAME = "db.username";
    private static final String DB_PASSWORD = "db.password";
    private static final String DB_TABLEPREFX = "db.tableprefix";

    private Tenant tenant;
    private DataSourceConfiguration dataSourceConfiguration;

    public TestTenantPropertiesLoader(Tenant tenant, DataSourceConfiguration dataSourceConfiguration) {
        super(tenant);
        this.tenant = tenant;
        this.dataSourceConfiguration = dataSourceConfiguration;
    }

    public String getProperty(String key) {
        try {
            return getPropertyDelegate(key);
        } catch (Exception e) {
            return this.tenant.getConfig().getParameter(key);
        }
    }

    public String getProperty(String key, String defaultValue) {
        try {
            return getPropertyDelegate(key);
        } catch (Exception e) {
            return this.tenant.getConfig().getString(key, defaultValue);
        }
    }

    public String getPropertyDelegate(String key) throws Exception {
        switch (key) {
            case DB_URL:
                return dataSourceConfiguration.getConnectionString();
            case DB_DRIVER:
                return dataSourceConfiguration.getDriver();
            case DB_PASSWORD:
                return dataSourceConfiguration.getPassword();
            case DB_USERNAME:
                return dataSourceConfiguration.getUserName();
            case DB_TABLEPREFX:
                return dataSourceConfiguration.getTablePrefix();
        }
        throw new Exception("unsupported key");
    }

}
