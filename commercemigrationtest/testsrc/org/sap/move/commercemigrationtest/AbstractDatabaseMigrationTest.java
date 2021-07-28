package org.sap.move.commercemigrationtest;

import com.google.common.base.Stopwatch;
import com.zaxxer.hikari.HikariDataSource;
import de.hybris.bootstrap.config.ConfigUtil;
import de.hybris.bootstrap.config.PlatformConfig;
import de.hybris.bootstrap.config.SystemConfig;
import de.hybris.bootstrap.ddl.DataSourceCreator;
import de.hybris.bootstrap.ddl.HybrisSchemaGenerator;
import de.hybris.bootstrap.ddl.PropertiesLoader;
import de.hybris.bootstrap.ddl.tools.TypeSystemHelper;
import de.hybris.platform.core.Registry;
import de.hybris.platform.core.Tenant;
import de.hybris.platform.core.TenantPropertiesLoader;
import de.hybris.platform.jalo.JaloSession;
import de.hybris.platform.servicelayer.ServicelayerBaseTest;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Index;
import org.apache.ddlutils.model.IndexColumn;
import org.apache.ddlutils.model.NonUniqueIndex;
import org.apache.ddlutils.model.TypeMap;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.sap.commercemigration.events.handlers.CopyDatabaseTableEventListener;
import org.sap.commercemigration.profile.DataSourceConfiguration;
import org.sap.commercemigration.provider.CopyItemProvider;
import org.sap.commercemigration.repository.DataRepository;
import org.sap.commercemigration.repository.impl.DataRepositoryFactory;
import org.sap.commercemigration.service.DatabaseMigrationService;
import org.sap.commercemigration.service.DatabaseSchemaDifferenceService;
import org.sap.commercemigration.service.impl.DefaultDatabaseSchemaDifferenceService;
import org.sap.move.commercemigrationtest.configuration.TestDataSourceConfiguration;
import org.sap.move.commercemigrationtest.configuration.TestTenantPropertiesLoader;
import org.sap.move.commercemigrationtest.context.TestMigrationContext;
import org.sap.move.commercemigrationtest.context.impl.DefaultTestMigrationContext;
import org.sap.move.commercemigrationtest.context.impl.DelegatingTestMigrationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.shaded.org.apache.commons.lang.StringUtils;

import javax.annotation.Resource;
import java.io.FileInputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Stream;


@RunWith(Parameterized.class)
public abstract class AbstractDatabaseMigrationTest extends ServicelayerBaseTest {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractDatabaseMigrationTest.class);

    private static final List<TestcontainerHolder.TYPE> enabledDatabases = Arrays.asList(TestcontainerHolder.TYPE.MSSQL, TestcontainerHolder.TYPE.MYSQL, TestcontainerHolder.TYPE.ORACLE, TestcontainerHolder.TYPE.HANA);
    //private static final List<TestcontainerHolder.TYPE> enabledDatabases = Arrays.asList(TestcontainerHolder.TYPE.MYSQL);
    private static final List<DatabaseHolder> registeredDatabases = new ArrayList<>();
    private static DatabaseHolder targetDatabaseParam;

    @Parameterized.Parameter()
    public String parametrizationId;
    @Parameterized.Parameter(value = 1)
    public DatabaseHolder sourceDatabase;
    @Parameterized.Parameter(value = 2)
    public DatabaseHolder targetDatabase;
    @Resource
    protected DatabaseMigrationService databaseMigrationService;
    @Resource
    protected DatabaseSchemaDifferenceService schemaDifferenceService;
    @Resource(name = "dataCopyItemProvider")
    protected CopyItemProvider dataCopyItemProvider;
    private PlatformConfig platformConfig;
    @Resource
    private DataRepositoryFactory dataRepositoryFactory;
    @Resource
    private CopyDatabaseTableEventListener copyDatabaseTableEventListener;
    @Resource
    private DelegatingTestMigrationContext migrationContext;
    private String latestMigrationId;

    static void initDatabases() {
        registeredDatabases.clear();

        ConnectionInfo info = getConnectionInfo(TestcontainerHolder.TYPE.MSSQL, null, MSSQLDatabaseHolder.DEFAULT_PORT);
        targetDatabaseParam = new MSSQLDatabaseHolder("sqlserver (target)", info.getHost(), info.getUsr(), info.getPsw());
        registerDatabase(targetDatabaseParam);

        if (enabledDatabases.contains(TestcontainerHolder.TYPE.MSSQL)) {
            registerDatabase(new MSSQLDatabaseHolder("sqlserver (source)", info.getHost(), info.getUsr(), info.getPsw()));
        }

        if (enabledDatabases.contains(TestcontainerHolder.TYPE.MYSQL)) {
            info = getConnectionInfo(TestcontainerHolder.TYPE.MYSQL, "root", MySQLDatabaseHolder.DEFAULT_PORT);
            registerDatabase(new MySQLDatabaseHolder("mysql", info.getHost(), info.getUsr(), info.getPsw()));
        }

        if (enabledDatabases.contains(TestcontainerHolder.TYPE.ORACLE)) {
            info = getConnectionInfo(TestcontainerHolder.TYPE.ORACLE, null, OracleDatabaseHolder.DEFAULT_PORT);
            registerDatabase(new OracleDatabaseHolder("oracle", info.getHost(), info.getUsr(), info.getPsw()));
        }

        if (enabledDatabases.contains(TestcontainerHolder.TYPE.HANA)) {
            info = getConnectionInfo(TestcontainerHolder.TYPE.HANA, null, HanaDatabaseHolder.DEFAULT_PORT);
            registerDatabase(new HanaDatabaseHolder("hana", info.getHost(), info.getUsr(), info.getPsw()));
        }
    }

    private static ConnectionInfo getConnectionInfo(TestcontainerHolder.TYPE type, String userName, int defaultPort) {
        String host;
        String usr;
        String psw;
        if (System.getenv("CI") == null) {
            JdbcDatabaseContainer<?> container = TestcontainerHolder.get(type);
            host = container.getHost() + ":" + container.getMappedPort(defaultPort);
            usr = userName == null ? container.getUsername() : userName;
            psw = container.getPassword();
        } else {
            host = System.getenv(String.format("%s_HOST", type.name()));
            usr = System.getenv(String.format("%s_USR", type.name()));
            psw = System.getenv(String.format("%s_PSW", type.name()));
        }

        return new ConnectionInfo(host, usr, psw);
    }

    private static void registerDatabase(DatabaseHolder databaseHolder) {
        registeredDatabases.add(databaseHolder);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<?> parameters() {
        initDatabases();
        Object[] params = registeredDatabases.stream().filter(h -> !StringUtils.equals(h.getId(), targetDatabaseParam.getId())).map(h -> new Object[]{String.format("%s->%s", h.getId(), targetDatabaseParam.getId()), h, targetDatabaseParam}).toArray();
        return Arrays.asList(params);
    }

    @BeforeClass
    public static void beforeAllTests() {
        JaloSession.getCurrentSession();
        for (DatabaseHolder databaseHolder : registeredDatabases) {
            create(databaseHolder);
        }
    }

    @AfterClass
    public static void afterAllTests() {
        if (System.getenv("CI_SKIP_DROP") == null) {
            for (DatabaseHolder databaseHolder : registeredDatabases) {
                destroy(databaseHolder);
            }
        } else {
            LOG.warn("CI_SKIP_DROP enabled; won't drop databases");
        }
    }

    private static void create(DatabaseHolder databaseHolder) {
        if (!databaseHolder.isRunning()) {
            LOG.info("Creating DB: {}", databaseHolder.getId());
            databaseHolder.create();
            LOG.info("Created: {}", databaseHolder.getId());
        }
    }

    private static void destroy(DatabaseHolder databaseHolder) {
        if (databaseHolder.isRunning()) {
            LOG.info("Destroying DB: {}", databaseHolder.getId());
            databaseHolder.destroy();
            LOG.info("Destroyed: {}", databaseHolder.getId());
        }
    }

    // TODO REVIEW Downgrade from Java11 for compatibility
    @SafeVarargs
    public static <T> Set<T> setOf(T... objs) {
        Set<T> set = new HashSet<>();
        Collections.addAll(set, objs);
        return Collections.unmodifiableSet(set);
    }

    protected static Column createColumn(String columnName) {
        Column column = new Column();
        column.setName(columnName);
        column.setType(TypeMap.INTEGER);
        return column;
    }

    protected static Index createIndex(String indexName, Column... columns) {
        Index index = new NonUniqueIndex();
        index.setName(indexName);
        Stream.of(columns).forEach(c -> {
            IndexColumn indexColumn = new IndexColumn();
            indexColumn.setColumn(c);
            index.addColumn(indexColumn);
        });

        return index;
    }

    @Before
    public void beforeTests() throws Exception {
        this.migrationContext.setContext(createMigrationContext(getSourceDatabase(), getTargetDatabase()));
        this.copyDatabaseTableEventListener.setMigrationContext(migrationContext);
        Path platformHome = ConfigUtil.getPlatformConfig(Tenant.class).getPlatformHome().toPath();
        platformConfig = configForPlatformHome(platformHome);
        initializeRepositories();
    }

    @After
    public void cleanupConnections() {
        final HikariDataSource sourceDataSource = (HikariDataSource) this.migrationContext.getContext().getDataSourceRepository().getDataSource();
        final HikariDataSource targetDataSource = (HikariDataSource) this.migrationContext.getContext().getDataTargetRepository().getDataSource();
        sourceDataSource.close();
        targetDataSource.close();
    }

    protected void initializeRepositories() throws Exception {
        initializeInternal(getSourceDatabase(), this.migrationContext.getDataSourceRepository(), false);
        initializeInternal(getTargetDatabase(), this.migrationContext.getDataTargetRepository(), true);
    }

    protected TestMigrationContext createMigrationContext(DatabaseHolder sourceDatabase, DatabaseHolder targetDatabase) throws Exception {
        DataSourceConfiguration sourceDataSourceConfiguration = createDataSourceConfiguration(sourceDatabase, "source");
        DataSourceConfiguration targetDataSourceConfiguration = createDataSourceConfiguration(targetDatabase, "target");

        return new DefaultTestMigrationContext(sourceDataSourceConfiguration, targetDataSourceConfiguration, getDataRepositoryFactory());
    }

    private void initializeInternal(DatabaseHolder container, DataRepository repo, boolean target) throws Exception {
        if (!container.isInitialized()) {
            initializeRepository(repo, target);
            container.setInitialized(true);
        }
    }

    protected void initializeRepository(DataRepository repository, boolean target) throws Exception {
        Tenant tenant = Registry.getMasterTenant();
        TenantPropertiesLoader tenantPropertiesLoader = new TestTenantPropertiesLoader(tenant, repository.getDataSourceConfiguration());
        HybrisSchemaGenerator generator = createSchemaGenerator(getPlatformConfig(), tenantPropertiesLoader, getDataSourceCreator(repository), false);
        //HybrisSchemaGenerator generator = new HybrisSchemaGenerator(getPlatformConfig(), tenantPropertiesLoader, getDataSourceCreator(repository), false);
        LOG.info("Running init scripts for: {}", repository.getDataSourceConfiguration().getConnectionString());
        Stopwatch sw = Stopwatch.createStarted();
        generator.initialize();
        LOG.info("Init scripts finished for: {} | {}", repository.getDataSourceConfiguration().getConnectionString(), sw.stop());
    }

    private HybrisSchemaGenerator createSchemaGenerator(PlatformConfig platformConfig, TenantPropertiesLoader tenantPropertiesLoader, DataSourceCreator dataSourceCreator, boolean b) {
        /*
         *   TODO this is quick fix for the hybris api change that was released with the latest version.
         *    Since the constructor we were using was kindly removed, this dirty fix is used for the time being
         *    to keep compatibility across versions. To be cleaned up in the future.
         */
        try {
            Constructor<HybrisSchemaGenerator> constructor = HybrisSchemaGenerator.class.getConstructor(PlatformConfig.class, PropertiesLoader.class, DataSourceCreator.class, boolean.class);
            return constructor.newInstance(platformConfig, tenantPropertiesLoader, dataSourceCreator, b);
        } catch (Exception e1) {
            e1.printStackTrace();
            try {
                Constructor<HybrisSchemaGenerator> constructor = HybrisSchemaGenerator.class.getConstructor(PlatformConfig.class, PropertiesLoader.class, DataSourceCreator.class, boolean.class, String.class);
                return constructor.newInstance(platformConfig, tenantPropertiesLoader, dataSourceCreator, b, "master");
            } catch (Exception e2) {
                e2.printStackTrace();
                throw new RuntimeException("Cannot create SchemaGenerator");
            }
        }
    }

    protected void createTypeSystem(DataRepository repository, String typeSystemName) {
        Tenant tenant = Registry.getMasterTenant();
        TenantPropertiesLoader tenantPropertiesLoader = new TestTenantPropertiesLoader(tenant, repository.getDataSourceConfiguration());
        LOG.info("Creating new typesystem based on current: {}", typeSystemName);
        TypeSystemHelper.createTypeSystemBasedOnCurrentTypeSystem(getDataSourceCreator(repository), tenantPropertiesLoader, typeSystemName);
    }

    private DataSourceCreator getDataSourceCreator(DataRepository repository) {
        return databaseSettings -> repository.getDataSource();
    }

    private SystemConfig loadSystemConfig(Path platformHome) throws Exception {
        Properties properties = new Properties();
        properties.load(new FileInputStream(platformHome.resolve("env.properties").toFile()));
        String configDirOriginal = (String) System.getProperties().get(SystemConfig.PROPERTY_CONFIG_DIR);
        try {
            System.getProperties().put(SystemConfig.PROPERTY_CONFIG_DIR, new ClassPathResource("/testconfig").getFile().getAbsolutePath());
            properties.setProperty("platformhome", platformHome.toFile().getCanonicalPath());
            if (!properties.containsKey("HYBRIS_BOOTSTRAP_BIN_DIR")) {
                properties.put("HYBRIS_BOOTSTRAP_BIN_DIR", "${platformhome}/bootstrap/bin");
            }
            // avoid singleton caching
            Field singleton = SystemConfig.class.getDeclaredField("singleton");
            singleton.setAccessible(true);
            singleton.set(null, null);
            return SystemConfig.getInstanceByProps(properties);
        } finally {
            System.getProperties().put(SystemConfig.PROPERTY_CONFIG_DIR, configDirOriginal);
        }
    }

    private PlatformConfig configForPlatformHome(Path path) {
        try {
            // avoid instance caching
            Field instance = PlatformConfig.class.getDeclaredField("instance");
            instance.setAccessible(true);
            instance.set(null, null);
            SystemConfig systemConfig = loadSystemConfig(path);
            return PlatformConfig.getInstance(systemConfig);
        } catch (Exception e) {
            throw new RuntimeException("Could not build PlatformConfig", e);
        }
    }

    private DataSourceConfiguration createDataSourceConfiguration(DatabaseHolder databaseHolder, String profile) {
        TestDataSourceConfiguration dataSourceConfiguration = new TestDataSourceConfiguration(profile);
        dataSourceConfiguration.setDriver(databaseHolder.getDriverClassName());
        dataSourceConfiguration.setConnectionString(databaseHolder.getConnectionString());
        dataSourceConfiguration.setUserName(databaseHolder.getUserName());
        dataSourceConfiguration.setPassword(databaseHolder.getPassword());
        dataSourceConfiguration.setSchema(databaseHolder.getSchemaName());
        dataSourceConfiguration.setTypeSystemName("DEFAULT");
        dataSourceConfiguration.setTypeSystemSuffix("");
        dataSourceConfiguration.setTablePrefix("");
        return dataSourceConfiguration;
    }

    protected void startMigrationAndWaitForFinish(DatabaseMigrationService databaseMigrationService) throws Exception {
        latestMigrationId = databaseMigrationService.startMigration(getMigrationContext());
        databaseMigrationService.waitForFinish(getMigrationContext(), latestMigrationId);
    }

    protected DefaultDatabaseSchemaDifferenceService.SchemaDifferenceResult getDifference() throws Exception {
        return schemaDifferenceService.getDifference(getMigrationContext());
    }

    public DatabaseHolder getSourceDatabase() {
        return sourceDatabase;
    }

    public DatabaseHolder getTargetDatabase() {
        return targetDatabase;
    }

    public TestMigrationContext getMigrationContext() {
        return migrationContext;
    }

    public DataRepositoryFactory getDataRepositoryFactory() {
        return dataRepositoryFactory;
    }

    public PlatformConfig getPlatformConfig() {
        return platformConfig;
    }

    protected long getTableCountForInitialization() {
        return 375;
    }

    public String getLatestMigrationId() {
        return latestMigrationId;
    }

    private static class ConnectionInfo {
        private String host;
        private String usr;
        private String psw;

        public ConnectionInfo(String host, String usr, String psw) {
            this.host = host;
            this.usr = usr;
            this.psw = psw;
        }

        public String getHost() {
            return host;
        }

        public String getUsr() {
            return usr;
        }

        public String getPsw() {
            return psw;
        }
    }

}
