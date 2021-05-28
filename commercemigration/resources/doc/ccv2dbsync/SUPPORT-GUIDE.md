# CMT - Azure Database Sync

It allows you to synchronize the data you select single-directionally (from CCV2) across multiple MS databases, both on-premises and in the cloud.
It has the following properties:

* The Sync Schema describes which data(table/items) is being synchronized.
* The Sync Direction is only single-directional, which is from CCV2 to Onprem or another cloud MS Database.
* The Sync Interval describes how often synchronization occurs.

## When to use

It is useful in cases where data needs to be kept updated across databases in Azure SQL Database or SQL Server.

 It's beneficial to separate different workloads across different databases. For example, if you have a large production database, but you also need to run a reporting or analytics workload on this data, it's helpful to have a second database for this additional workload.

## CMT changes to support data sync

Following high-level changes in CMT for Data Synchronization:

* Cronjob for full migration
* Cronjob for incremental migration
* Commerce Azure [read-only DB](https://docs.microsoft.com/en-us/azure/azure-sql/database/read-scale-out) as Source data source (it will not impact performance of the primary db)
* Reporting DB as Target Data

## Methodology for Data Sync

* Identify the tables you would like to sync (limit to the minimum that is required, avoid large table when possible (ex: do not sync task logs!)
* Remove/Add indexes that are not necessary in the target db
* Create indexes on last modified timestamp for tables that supports incremental
* Full data migration with all tables
* Run incremental regularly (example every hour)
* Reconfigure full data migration cronjob to sync tables with no last modified timestamp
* Run full data migration cronjob regularly and during low activity (e.g. every day, 3PM)
* Ensure data migration cronjobs are running on read-only

## Limitations with Data Sync

The following limitations should be considered when implementing CMT for data sync:
* Not all tables contain last modified timestamp
   * Some master data tables (LP tables, Audit…) will not contain timestamp column 
   * Should not be an issue, most of the time
   * You should ensure these tables are not too large
* No support for delete in the data source
   * Should not be a issue for most of the cases
   * In case, it is needed, you should implement you own strategy (e.g. interceptor on delete model service, new status to flag the row as deleted)
* Incremental update may create data integrity issue between two runs
   * During the cronjob execution, you may have some updates or creations that are not sync
   * There is no guaranty that some tables with relations may be partially sync, example Orders vs Orders items,
   * This should be acceptable for reporting or analytics, should be take into consideration for the application using the destination DB)
* Sync of master data (tables without last modified timestamp) may be delayed (e.g. 24 hours)
* Performance should be tested to tune batch size and number threads (memory and CPU on the application server)

## Configuration reference Data Sync

[Configuration Reference](../configuration/CONFIGURATION-REFERENCE.md) To get a high-level overview of the configurable properties by CMT.

Properties require to reconfigure or readjusted for Data Sync.

| Property                                               | Mandatory | Default                                                                                                                                                                      | Description                                                                                              |
|--------------------------------------------------------|-----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------|
| migration.ds.source.db.url                             | yes       |                                                                                                                                                                              | DB url for source connection , default value should be **${db.url};ApplicationIntent=ReadOnly** ApplicationIntent can be adjusted or removed for local testing                                                                            |
| migration.ds.source.db.schema                          | no       |  dbo                                                                                                                                                                          | DB schema for source connection                                                                          |
| migration.ds.target.db.driver                          | yes       | ${db.driver}                                                                                                                                                                 | DB driver class for target connection                                                                    |
| migration.ds.target.db.username                        | yes       |                                                                                                                                                                              | DB username for target connection                                                                        |
| migration.ds.target.db.password                        | yes       |                                                                                                                                                                              | DB password for target connection                                                                        |
| migration.ds.target.db.tableprefix                     | no        | ${db.tableprefix}                                                                                                                                                            | DB table prefix for target connection                                                                    |
| migration.ds.target.db.schema                          | no        | dbo                                                                                                                                                                          | DB schema for target connection                                                                          |
| migration.data.tables.included                         | no        |                                                                                                                                                                              | Tables to be included in the migration. It is recommended to set this parameter during the first load of selective table sync, which will allow you to sync directly from HAC along with Schema. Eventually you can do very similar with full migration cron jobs by adjusting the list of tables.  |
| migration.data.report.connectionstring                        | yes        |  ${media.globalSettings.cloudAzureBlobStorageStrategy.connection}                                                                                      | target blob storage for the report generation, although you can replace with Hotfolder Blob storage ${azure.hotfolder.storage.account.connection-string} |
| migration.data.workers.retryattempts                       | no        | 0                                                                                                                                                                         | retry attempts if a batch (read or write) failed.                                                           |

## CronJob Configuration reference Data Sync

DataSync is managed by Cronjobs, which allow you to trigger full and interval Sync based on sync interval.

Following High-level details for the Cronjobs,
#### FullMigrationCronJob
It allows you to sync the full based on the list provided in CronJob settings.
List of attributes/properties can set during Full migration

| attributes                                               | Mandatory | Default                                                                                                                                                                      | Description                                                                                              |
|--------------------------------------------------------|-----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------|
| migrationItems                                         | yes       |                                                                                                                                                                              | Initially it can be set through impex file, and later it Adjusted through either Backoffice or Impex. You can set list of table with required full sync during initials, later adjust based on business case.                                                                               |
| schemaAutotrigger                                      | no        |  false                                                                                                                                                                       | Adjust this value if you have any Data model changes, it can be changed to true, but it will add delay in every sync.                                                                             |
| truncateEnabled                                        | yes       |  false                                                                                                                                                                       | Allow truncating the target table before writing data which is mandatory for the Full Sync, set **true** for full Sync                                                                        |
| cronExpression                                        | yes       |   0 0/1 * * * ?                                                                                                                                                                | Set via impex file                                                                      |

#### IncrementalMigrationCronJob
It allows you to sync the delta based on modifiedTS hence tables must have the following columns: modifiedTS, PK. Furthermore, this is an incremental approach... only modified and inserted rows are taken into account. Deletions on the source side are not handled.

List of attributes/properties can set during incremental migration

| attributes                                               | Mandatory | Default                                                                                                                                                                      | Description                                                                                              |
|--------------------------------------------------------|-----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------|
| migrationItems                                         | yes       |                                                                                                                                                                              | Initially it can be set through impex file, and later it Adjusted through either Backoffice or Impex.   |
| schemaAutotrigger                                      | no        |  false                                                                                                                                                                       | Adjust this value if you have any Data model changes, it can be changed to true, but it will add delay in every sync.                                                                             |
| truncateEnabled                                        | yes       |  false                                                                                                                                                                       | Set **false** for incremental sync                                                                          |
| cronExpression                                         | yes       |   0 0 0 * * ?                                                                                                                                                                | Set via impex file                                                                      |
| lastStartTime                                         | yes       |   0 0 0 * * ?                                                                                                                                                                 | Its updated based last triggered timestamp. Update manually for longer window.                                                                     |


#### Default Impex file
```
INSERT_UPDATE ServicelayerJob;code[unique=true];springId[unique=true]
;incrementalMigrationJob;incrementalMigrationJob
;fullMigrationJob;fullMigrationJob

# Update details for incremental migration
INSERT_UPDATE IncrementalMigrationCronJob;code[unique=true];active;job(code)[default=incrementalMigrationJob];sessionLanguage(isoCode)[default=en]
;incrementalMigrationJob;true;

INSERT_UPDATE IncrementalMigrationCronJob;code[unique=true];migrationItems
;incrementalMigrationJob;PAYMENTMODES,ADDRESSES,users,CAT2PRODREL,CONSIGNMENTS,ORDERS

INSERT_UPDATE FullMigrationCronJob;code[unique=true];active;job(code)[default=fullMigrationJob];sessionLanguage(isoCode)[default=en]
;fullMigrationJob;true;

INSERT_UPDATE FullMigrationCronJob;code[unique=true];truncateEnabled;migrationItems
;fullMigrationJob;true;PAYMENTMODES,products

INSERT_UPDATE Trigger;cronjob(code)[unique=true];cronExpression
#% afterEach: impex.getLastImportedItem().setActivationTime(new Date());
;incrementalMigrationJob; 0 0/1 * * *
;fullMigrationJob; 0 0 0 * * ?

```

