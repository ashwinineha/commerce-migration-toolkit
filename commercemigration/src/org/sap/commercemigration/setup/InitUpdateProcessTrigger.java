package org.sap.commercemigration.setup;

import de.hybris.platform.media.services.MediaStorageInitializer;
import org.sap.commercemigration.context.MigrationContext;
import org.sap.commercemigration.service.DatabaseMigrationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InitUpdateProcessTrigger implements MediaStorageInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(InitUpdateProcessTrigger.class);

    private MigrationContext migrationContext;
    private DatabaseMigrationService databaseMigrationService;
    private boolean failOnError = false;

    public InitUpdateProcessTrigger(MigrationContext migrationContext, DatabaseMigrationService databaseMigrationService) {
        this.migrationContext = migrationContext;
        this.databaseMigrationService = databaseMigrationService;
    }

    @Override
    public void onInitialize() {
        //Do nothing
    }

    @Override
    public void onUpdate() {
        try {
            if (migrationContext.isMigrationTriggeredByUpdateProcess()) {
                LOG.info("Starting data migration ...");
                String migrationId = databaseMigrationService.startMigration(migrationContext);
                databaseMigrationService.waitForFinish(migrationContext, migrationId);
                //note: further update activities not stopped here -> should we?
            }
        } catch (Exception e) {
            failOnError = migrationContext.isFailOnErrorEnabled();
            if (failOnError) {
                throw new Error(e);
            }
        }
    }

    @Override
    public boolean failOnInitUpdateError() {
        return failOnError;
    }

}