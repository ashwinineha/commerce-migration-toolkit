package org.sap.move.commercemigrationtest.integration;

import de.hybris.bootstrap.annotations.IntegrationTest;
import de.hybris.bootstrap.ddl.DataBaseProvider;
import de.hybris.platform.servicelayer.config.ConfigurationService;
import org.junit.Test;
import org.sap.commercemigration.constants.CommercemigrationConstants;
import org.sap.commercemigration.context.CopyContext;
import org.sap.commercemigration.profile.DataSourceConfiguration;
import org.sap.commercemigration.provider.CopyItemProvider;
import org.sap.commercemigration.repository.DataRepository;
import org.sap.commercemigration.service.DatabaseMigrationService;
import org.sap.commercemigration.service.DatabaseMigrationSynonymService;
import org.sap.commercemigration.service.DatabaseSchemaDifferenceService;
import org.sap.move.commercemigrationtest.AbstractDatabaseMigrationTest;
import org.sap.move.commercemigrationtest.DatabaseHolder;
import org.sap.move.commercemigrationtest.ccv2.Ccv2TypeSystemHelper;
import org.sap.move.commercemigrationtest.configuration.TestDataSourceConfiguration;
import org.sap.move.commercemigrationtest.context.TestMigrationContext;

import javax.annotation.Resource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

@IntegrationTest
public class CCv2PrefixedDatabaseMigrationIntegrationTest extends AbstractDatabaseMigrationTest {

    private static final String SOURCE_TABLE_PREFIX = "srcpfx_";
    private static final String TARGET_TABLE_PREFIX = "tgtpfx_";
    private static final String TARGET_MIGRATION_TABLE_PREFIX = "cmt_";
    @Resource
    private DatabaseMigrationService databaseMigrationService;
    @Resource(name = "dataCopyItemProvider")
    private CopyItemProvider dataCopyItemProvider;
    @Resource(name = "schemaDifferenceService")
    private DatabaseSchemaDifferenceService databaseSchemaDifferenceService;
    @Resource
    private ConfigurationService configurationService;
    @Resource
    private DatabaseMigrationSynonymService databaseMigrationSynonymService;


    @Override
    protected TestMigrationContext createMigrationContext(DatabaseHolder sourceDatabase, DatabaseHolder targetDatabase) throws Exception {
        TestMigrationContext migrationContext = super.createMigrationContext(sourceDatabase, targetDatabase);
        setConfigPrefix(migrationContext.getDataSourceRepository(), SOURCE_TABLE_PREFIX);
        setConfigPrefix(migrationContext.getDataTargetRepository(), TARGET_TABLE_PREFIX);
        return migrationContext;
    }

    @Override
    protected void initializeRepository(DataRepository repository, boolean target) throws Exception {
        super.initializeRepository(repository, target);
        if (target) {
            /*
            String prefixProp = "db.tableprefix";
            String actualPrefix = configurationService.getConfiguration().getString(prefixProp);
            configurationService.getConfiguration().setProperty(prefixProp, TARGET_TABLE_PREFIX);
            MigrationSystemSetup setup = new MigrationSystemSetup(getMigrationContext(), configurationService, databaseMigrationSynonymService);
            setup.createEssentialData(null);
            configurationService.getConfiguration().setProperty(prefixProp, actualPrefix);
             */
            // create the ccv2 tables using the helper tool
            new Ccv2TypeSystemHelper(repository).loadRecords();

        }
    }

    private void setConfigPrefix(DataRepository repository, String prefixValue) {
        DataSourceConfiguration dataSourceConfiguration = repository.getDataSourceConfiguration();
        assertThat(dataSourceConfiguration).isInstanceOf(TestDataSourceConfiguration.class);
        ((TestDataSourceConfiguration) dataSourceConfiguration).setTablePrefix(prefixValue);
    }


    @Test
    public void testCCv2PrefixedFlow() throws Exception {
        //cleanup migration tables if exists (to ensure clean state for each db param iteration)
        cleanupMigrationTables(getMigrationContext().getDataTargetRepository());

        //let's check first that the ccv2 meta table was created for main prefix
        assertThat(getMigrationContext().getDataTargetRepository().getAllTableNames()).contains(TARGET_TABLE_PREFIX + "CCV2_TYPESYSTEM_MIGRATIONS");

        //target was initialized with specific prefix, now set different prefix for migration
        setConfigPrefix(getMigrationContext().getDataTargetRepository(), TARGET_MIGRATION_TABLE_PREFIX);

        /*
            target was initialized with prefix, migration uses different prefix so none of the target tables can exist (and were cleaned up before) at this point in time.
            We use the schema diff service to create the tables in target.
         */
        getMigrationContext().setAddMissingTablesToSchemaEnabled(true);
        databaseSchemaDifferenceService.executeSchemaDifferences(getMigrationContext());

        /*
            make sure the newly created tables have the correct data type mappings
         */
        assertCustomTableDataTypesAreCorrectlyMapped();

        /*
         * After schema migration, all tables having the migration prefix should be created.
         * Let's check that the provider returns an item set that contains all initialized candidates
         */
        Set<CopyContext.DataCopyItem> dataCopyItems = dataCopyItemProvider.get(getMigrationContext());
        assertThat(getTableCountForInitialization()).isEqualTo(dataCopyItems.size());

        // Let's make sure that item set contains the respective prefixed ydeployment tables
        String sourceCheck = SOURCE_TABLE_PREFIX + CommercemigrationConstants.DEPLOYMENTS_TABLE;
        String targetCheck = TARGET_MIGRATION_TABLE_PREFIX + CommercemigrationConstants.DEPLOYMENTS_TABLE;
        assertThat(dataCopyItems.stream())
                .filteredOn(item -> item.getSourceItem().equalsIgnoreCase(sourceCheck) && item.getTargetItem().equalsIgnoreCase(targetCheck))
                .isNotEmpty();

        //run migration
        startMigrationAndWaitForFinish(databaseMigrationService);

        for (CopyContext.DataCopyItem copyItem : dataCopyItems) {
            long sourceRowCount = getMigrationContext().getDataSourceRepository().getRowCount(copyItem.getSourceItem());
            long targetRowCount = getMigrationContext().getDataTargetRepository().getRowCount(copyItem.getTargetItem());
            assertThat(sourceRowCount).isEqualTo(targetRowCount);
        }
        String ccv2TypeSystemName = new Ccv2TypeSystemHelper(getMigrationContext().getDataTargetRepository()).currentTypesystem();
        String typeSystemName = getMigrationContext().getDataTargetRepository().getDataSourceConfiguration().getTypeSystemName();
        assertThat(ccv2TypeSystemName).isEqualTo(typeSystemName);

        //reset config prefix
        setConfigPrefix(getMigrationContext().getDataTargetRepository(), TARGET_TABLE_PREFIX);
    }

    private void cleanupMigrationTables(DataRepository dataTargetRepository) throws Exception {
        dataTargetRepository.getAllTableNames().stream().filter(n -> n.startsWith(TARGET_MIGRATION_TABLE_PREFIX)).forEach(n -> {
            try {
                dataTargetRepository.executeUpdateAndCommit(String.format("DROP TABLE IF EXISTS %s", n));
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    private void assertCustomTableDataTypesAreCorrectlyMapped() throws Exception {
        Map<String, DtSpec> dtSpecs = createDtSpec();
        if (getMigrationContext().getDataSourceRepository().getDatabaseProvider() == DataBaseProvider.ORACLE) {
            dtSpecs = createOracleDtSpec();
        } else if (getMigrationContext().getDataSourceRepository().getDatabaseProvider() == DataBaseProvider.HANA) {
            dtSpecs = createHanaDtSpec();
        }
        try (Connection conn = getMigrationContext().getDataTargetRepository().getConnection();
             Statement statement = conn.createStatement();
             ResultSet rs = statement.executeQuery(String.format("select * from %sMyCustomItem", TARGET_MIGRATION_TABLE_PREFIX));) {

            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                String columnName = metaData.getColumnName(i);
                String columnTypeName = metaData.getColumnTypeName(i);
                int precision = metaData.getPrecision(i);
                int scale = metaData.getScale(i);
                if (dtSpecs.containsKey(columnName)) {
                    DtSpec dtSpec = dtSpecs.get(columnName);
                    assertThat(columnTypeName).isEqualToIgnoringCase(dtSpec.getColumnTypeName());
                    if (dtSpec.getPrecision() > 0) {
                        assertThat(precision).isEqualTo(dtSpec.getPrecision());
                    }
                    if (dtSpec.getScale() > 0) {
                        assertThat(scale).isEqualTo(dtSpec.getScale());
                    }
                }
            }
        } catch (Exception e) {
            throw e;
        }
    }

    private Map<String, DtSpec> createDtSpec() {
        Map<String, DtSpec> map = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        map.put("p_stringfield", new DtSpec("nvarchar", 255, 0));
        map.put("p_pstringfield", new DtSpec("nvarchar", 255, 0));
        map.put("p_floatfield", new DtSpec("float"));
        map.put("p_doublefield", new DtSpec("float"));
        map.put("p_bytefield", new DtSpec("int"));
        //map.put("p_characterfield", new DtSpec("char", 4, 0));
        map.put("p_shortfield", new DtSpec("int"));
        map.put("p_booleanfield", new DtSpec("tinyint"));
        map.put("p_longfield", new DtSpec("bigint"));
        map.put("p_integerfield", new DtSpec("int"));
        map.put("p_pfloatfield", new DtSpec("float"));
        map.put("p_pdoublefield", new DtSpec("float"));
        map.put("p_pbytefield", new DtSpec("int"));
        //map.put("p_pcharfield", new DtSpec("char", 4, 0));
        map.put("p_pshortfield", new DtSpec("int"));
        map.put("p_pbooleanfield", new DtSpec("tinyint"));
        map.put("p_plongfield", new DtSpec("bigint"));
        map.put("p_pintfield", new DtSpec("int"));
        map.put("p_datefield", new DtSpec("datetime2"));
        map.put("p_bigdecimalfield", new DtSpec("decimal", 30, 8));
        map.put("p_serializablefield", new DtSpec("varbinary", DtSpec.BINARY_MAX_LENGTH, 0));
        map.put("p_longstringfield", new DtSpec("nvarchar", DtSpec.CHAR_MAX_LENGTH, 0));
        map.put("p_jsonfield", new DtSpec("nvarchar", DtSpec.CHAR_MAX_LENGTH, 0));
        map.put("p_commaseparatedpksfield", new DtSpec("nvarchar", DtSpec.CHAR_MAX_LENGTH, 0));
        map.put("p_pkfield", new DtSpec("bigint"));
        map.put("p_customdecimalfield", new DtSpec("decimal", 12, 3));
        map.put("p_customvarbinary1000field", new DtSpec("varbinary", 1000, 0));
        map.put("p_customnvarchar2000field", new DtSpec("nvarchar", 2000, 0));
        return map;
    }

    /*
     * There is a compatibility "mismatch" with how hana DT are defined ootb
     */
    private Map<String, DtSpec> createHanaDtSpec() {
        Map<String, DtSpec> map = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        map.put("p_stringfield", new DtSpec("nvarchar", 255, 0));
        map.put("p_pstringfield", new DtSpec("nvarchar", 255, 0));
        map.put("p_floatfield", new DtSpec("decimal", 20, 5)); // decimal(20,5) -> float
        map.put("p_doublefield", new DtSpec("decimal", 30, 8)); // decimal(30,8) -> float
        map.put("p_bytefield", new DtSpec("int"));
        map.put("p_characterfield", new DtSpec("int")); // smallint -> integer
        map.put("p_shortfield", new DtSpec("int"));
        map.put("p_booleanfield", new DtSpec("decimal", 1, 0)); // decimal(1,0) -> tinyint
        map.put("p_longfield", new DtSpec("bigint"));
        map.put("p_integerfield", new DtSpec("bigint")); // bigint -> integer
        map.put("p_floatfield", new DtSpec("decimal", 20, 5)); // decimal(20,5) -> float
        map.put("p_doublefield", new DtSpec("decimal", 30, 8)); // decimal(30,8) -> float
        map.put("p_pbytefield", new DtSpec("int"));
        map.put("p_pcharfield", new DtSpec("int")); // integer -> char(4)
        map.put("p_pshortfield", new DtSpec("int"));
        map.put("p_pbooleanfield", new DtSpec("decimal", 1, 0)); // decimal(1,0) -> tinyint
        map.put("p_plongfield", new DtSpec("bigint"));
        map.put("p_pintfield", new DtSpec("bigint")); // bigint -> integer
        map.put("p_datefield", new DtSpec("datetime2"));
        map.put("p_bigdecimalfield", new DtSpec("decimal", 30, 8));
        map.put("p_serializablefield", new DtSpec("varbinary", DtSpec.BINARY_MAX_LENGTH, 0));
        map.put("p_longstringfield", new DtSpec("nvarchar", DtSpec.CHAR_MAX_LENGTH, 0));
        map.put("p_jsonfield", new DtSpec("nvarchar", DtSpec.CHAR_MAX_LENGTH, 0));
        map.put("p_commaseparatedpksfield", new DtSpec("nvarchar", DtSpec.CHAR_MAX_LENGTH, 0));
        map.put("p_pkfield", new DtSpec("bigint"));
        map.put("p_customdecimalfield", new DtSpec("decimal", 12, 3));
        map.put("p_customvarbinary1000field", new DtSpec("varbinary", 1000, 0));
        map.put("p_customnvarchar2000field", new DtSpec("nvarchar", 2000, 0));
        return map;
    }

    /*
     * There is a compatibility "mismatch" with how oracle DT are defined ootb
     */
    private Map<String, DtSpec> createOracleDtSpec() {
        Map<String, DtSpec> map = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        map.put("p_stringfield", new DtSpec("nvarchar", 255, 0));
        map.put("p_pstringfield", new DtSpec("nvarchar", 255, 0));
        map.put("p_floatfield", new DtSpec("decimal", 20, 5)); // number(20,5) -> float
        map.put("p_doublefield", new DtSpec("decimal", 30, 8)); // number(30,8) -> float
        map.put("p_bytefield", new DtSpec("decimal", 10, 0)); // number(10,0) -> integer
        map.put("p_characterfield", new DtSpec("decimal", 10, 0)); // number(10,0) -> char(4)
        map.put("p_shortfield", new DtSpec("decimal", 10, 0)); // number(10,0) -> integer
        map.put("p_booleanfield", new DtSpec("tinyint"));
        map.put("p_longfield", new DtSpec("decimal", 20, 0)); // number(20,0) -> bigint
        map.put("p_integerfield", new DtSpec("decimal", 20, 0)); // number(20,0) -> int
        map.put("p_pfloatfield", new DtSpec("decimal", 20, 5)); // number(20,5) -> float
        map.put("p_pdoublefield", new DtSpec("decimal", 30, 8)); // number(30,8) -> float
        map.put("p_pbytefield", new DtSpec("decimal", 10, 0)); // number(10,0) -> integer
        map.put("p_pcharfield", new DtSpec("decimal", 10, 0)); // number(10,0) -> char(4)
        map.put("p_pshortfield", new DtSpec("decimal", 10, 0)); // number(10,0) -> integer
        map.put("p_pbooleanfield", new DtSpec("tinyint"));
        map.put("p_plongfield", new DtSpec("decimal", 20, 0)); // number(20,0) -> int
        map.put("p_pintfield", new DtSpec("decimal", 20, 0)); // number(20,0) -> int
        map.put("p_datefield", new DtSpec("datetime2"));
        map.put("p_bigdecimalfield", new DtSpec("decimal", 30, 8));
        map.put("p_serializablefield", new DtSpec("varbinary", DtSpec.BINARY_MAX_LENGTH, 0));
        map.put("p_longstringfield", new DtSpec("nvarchar", 4000, 0));
        map.put("p_jsonfield", new DtSpec("nvarchar", DtSpec.CHAR_MAX_LENGTH, 0));
        map.put("p_commaseparatedpksfield", new DtSpec("nvarchar", 4000, 0));
        map.put("p_pkfield", new DtSpec("decimal", 20, 0)); // number(20,0) -> bigint
        map.put("p_customdecimalfield", new DtSpec("decimal", 12, 3));
        map.put("p_customvarbinary1000field", new DtSpec("varbinary", 1000, 0));
        map.put("p_customnvarchar2000field", new DtSpec("nvarchar", 2000, 0));
        return map;
    }


    private static class DtSpec {

        private static int BINARY_MAX_LENGTH = 2147483647;
        private static int CHAR_MAX_LENGTH = 1073741823;

        private String columnTypeName;
        private int precision;
        private int scale;

        public DtSpec(String columnTypeName, int precision, int scale) {
            this.columnTypeName = columnTypeName;
            this.precision = precision;
            this.scale = scale;
        }

        public DtSpec(String columnTypeName) {
            this(columnTypeName, 0, 0);
        }

        public String getColumnTypeName() {
            return columnTypeName;
        }

        public int getPrecision() {
            return precision;
        }

        public int getScale() {
            return scale;
        }
    }

}
