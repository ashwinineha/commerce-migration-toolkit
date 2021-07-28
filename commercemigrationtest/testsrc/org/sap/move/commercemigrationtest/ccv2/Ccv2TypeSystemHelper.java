/*
 * [y] hybris Platform
 *
 * Copyright (c) 2018 SAP SE or an SAP affiliate company. All rights reserved.
 *
 * This software is the confidential and proprietary information of SAP
 * ("Confidential Information"). You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms of the
 * license agreement you entered into with SAP.
 */
package org.sap.move.commercemigrationtest.ccv2;

import org.sap.commercemigration.repository.DataRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;


public class Ccv2TypeSystemHelper {

    private static final Logger LOG = LoggerFactory.getLogger(Ccv2TypeSystemHelper.class);
    /**
     * current - the typesystem being used to serve the storefront
     * String states enumerating the possible typesystem states.
     * There must be exactly 1 typesystem in this state
     */
    private static final String STATE_CURRENT = "current";
    /**
     * retired - a typesystem that once was used to serve the storefront,
     * but no longer is in use.
     * There may be any number of typesystems in this state
     */
    private static final String STATE_OLD = "retired";
    /**
     * next - a typesystem that should next be used to serve the storefront
     * but no longer is in use.
     * There may be 0 or 1 typesystems in this state.
     */
    private static final String STATE_NEXT = "next";
    /**
     * skipped - a typesystem that was created but never used due to migration failure
     * or some other cause.
     * There may be any number of typesystems in this state.
     */
    private static final String STATE_SKIPPED = "skipped";
    /**
     * Basic date format for putting created and modified dates in the database.
     * We use varchar for dates since it is more human readable and cross-platform
     * compatible for databases.
     */
    private final DateFormat isoFormat;
    private final DateFormat typesystemFormat;
    private final DataRepository dataRepository;
    /**
     * The universally known default typesystem.
     */
    private final String DEFAULT_TYPESYSTEM_NAME = "DEFAULT";

    private final String tablePrefix;

    public Ccv2TypeSystemHelper(DataRepository repository) {
        final TimeZone tz = TimeZone.getTimeZone("UTC");
        isoFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'"); // Quoted "Z" to indicate UTC, no timezone offset
        isoFormat.setTimeZone(tz);
        typesystemFormat = new SimpleDateFormat("yyyyMMdd'T'HHmmss");
        typesystemFormat.setTimeZone(tz);
        dataRepository = repository;
        tablePrefix = repository.getDataSourceConfiguration().getTablePrefix();
    }

    public void dropTable() throws Exception {
        dataRepository.executeUpdateAndCommit("drop table if exists " + tablePrefix + "CCV2_TYPESYSTEM_MIGRATIONS");
    }


    public void createTable() throws Exception {
        LOG.info("creating table: " + tablePrefix + "CCV2_TYPESYSTEM_MIGRATIONS");
        dataRepository.executeUpdateAndCommit("CREATE TABLE " + tablePrefix + "CCV2_TYPESYSTEM_MIGRATIONS (" +
                " name varchar(60), " +
                " state varchar(10), " +
                " createdDate varchar(20), " +
                " modifiedDate varchar(20), " +
                " comment varchar(255) " +
                ")");
    }

    public List<TypesystemRecord> loadRecordsWithoutRepair() throws TableEmptyException, TableDoesntExistException {
        final List<TypesystemRecord> records = new ArrayList<>();

        try (Statement stmt = dataRepository.getConnection().createStatement(); ResultSet rs = stmt.executeQuery("select * from " + tablePrefix + "CCV2_TYPESYSTEM_MIGRATIONS;")) {
            while (rs.next()) {
                records.add(new TypesystemRecord(
                        rs.getString("name"),
                        rs.getString("state"),
                        isoFormat.parse(rs.getString("createdDate")),
                        isoFormat.parse(rs.getString("modifiedDate")),
                        rs.getString("comment")));

            }
        } catch (SQLException e) {
            if (isTableDoesntExist(e)) {
                throw new TableDoesntExistException(e);
            } else {
                throw new UncheckedUpgradeException("Unable to query for ccv2_typesystem_migrations records", e);
            }
        } catch (ParseException e) {
            throw new UncheckedUpgradeException(e);
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (records.isEmpty()) {
            throw new TableEmptyException();
        }
        return records;
    }

    private boolean isTableDoesntExist(SQLException e) {

        return "42P01".equals(e.getSQLState()) // postgresql
                || e.getErrorCode() == 208 // sql server
                || e.getErrorCode() == -5501; //hsqldb
    }

    public String detectCurrentTypesystem() {
        final List<DetectedTypesystem> detectedTypesystems = detectTypesystems();
        if (detectedTypesystems.isEmpty()) {
            return DEFAULT_TYPESYSTEM_NAME;
        }

        detectedTypesystems.sort(Comparator.comparing(DetectedTypesystem::getUpdated).reversed());
        return detectedTypesystems.get(0).getTypesystemName();
    }

    public List<DetectedTypesystem> detectTypesystems() {

        final ArrayList<DetectedTypesystem> detectedTypesystems = new ArrayList<>();


        // Note: typecode = 87 is the code for the "AttributeDescriptor" hybris type
        try (Statement stmt = dataRepository.getConnection().createStatement(); ResultSet detectedTypesystemRs = stmt.executeQuery(
                "select typesystemname, tablename, typecode from " + tablePrefix + "ydeployments where typecode = 87")) {
            while (detectedTypesystemRs.next()) {
                detectedTypesystems.add(new DetectedTypesystem(
                        detectedTypesystemRs.getString("typesystemname"),
                        tablePrefix + detectedTypesystemRs.getString("tablename")
                ));
            }
        } catch (Exception e) {
            throw new UncheckedUpgradeException("Unable to list typesystems", e);
        }

        for (final DetectedTypesystem detectedTypesystem : detectedTypesystems) {
            try (ResultSet tsDate = dataRepository.getConnection().createStatement().executeQuery(
                    "select max(modifiedTs) as modifiedDate from " + detectedTypesystem.getAttributeTableName())) {
                if (tsDate.next()) {
                    detectedTypesystem.setUpdated(tsDate.getTimestamp("modifiedDate"));
                }
            } catch (Exception e) {
                throw new UncheckedUpgradeException("Unable to determine creation date of typesystem " + detectedTypesystem.getTypesystemName(), e);
            }
        }

        return detectedTypesystems;
    }


    public String currentTypesystem() throws Exception {
        try {
            final List<TypesystemRecord> records = loadRecords();
            final Optional<String> currentName = records.stream()
                    .filter(ts -> ts.getState().equalsIgnoreCase(STATE_CURRENT))
                    .map(ts -> ts.getTypesystemName())
                    .findFirst();
            return currentName.orElse(DEFAULT_TYPESYSTEM_NAME);
        } catch (RuntimeException e) {
            throw new UncheckedUpgradeException("Unable to determine current typesystem, Caught exception " + e, null);
        }
    }

    @SuppressWarnings({"squid:S1166"})
    public List<TypesystemRecord> loadRecords() throws Exception {
        List<TypesystemRecord> records = null;
        try {
            records = loadRecordsWithoutRepair();
        } catch (TableDoesntExistException e) {
            LOG.error("Table doesn't exist, creating and initializing.");
            createTable();
            initializeTypesystemRecords();
        } catch (TableEmptyException e) {
            LOG.error("Table is empty, detecting typesystems.");
            initializeTypesystemRecords();
        }


        if (records == null) {
            try {
                records = loadRecordsWithoutRepair();
            } catch (TableEmptyException | TableDoesntExistException e) {
                throw new UncheckedUpgradeException("Unable to get typesystems after repairing the table", e);
            }
        }

        records.sort(Comparator.comparing(TypesystemRecord::getCreatedDate).reversed());

        return records;
    }

    private void initializeTypesystemRecords() {
        final List<DetectedTypesystem> detectedTypesystems = detectTypesystems();
        if (detectedTypesystems.isEmpty()) {
            throw new IllegalStateException("No typesystems detected");
        }

        detectedTypesystems.sort(Comparator.comparing(DetectedTypesystem::getUpdated).reversed());
        final DetectedTypesystem currentDts = detectedTypesystems.get(0);

        detectedTypesystems.stream().map(dts ->
                new TypesystemRecord(dts.getTypesystemName(),
                        dts == currentDts ? STATE_CURRENT : STATE_OLD,
                        new Date(), dts.getUpdated(), "Imported from detected typesystems")
        ).forEach(ts -> createTypeSystem(ts));

    }

    private void createTypeSystem(TypesystemRecord record) {
        try (final PreparedStatement stmt = dataRepository.getConnection().prepareStatement("insert into " + tablePrefix + "CCV2_TYPESYSTEM_MIGRATIONS " +
                "(name, state, createdDate, modifiedDate, comment) " +
                "values (?,?,?,?,?)")) {
            stmt.setString(1, record.getTypesystemName());
            stmt.setString(2, record.getState());
            stmt.setString(3, isoFormat.format(record.getCreatedDate()));
            stmt.setString(4, isoFormat.format(record.getModifiedDate()));
            stmt.setString(5, record.getComment());
            stmt.execute();
        } catch (Exception e) {
            throw new UncheckedUpgradeException(e);
        }
    }

    private void updateTypeSytem(TypesystemRecord record) {
        try (final PreparedStatement stmt = dataRepository.getConnection().prepareStatement("update " + tablePrefix + "CCV2_TYPESYSTEM_MIGRATIONS set " +
                "state = ?, modifiedDate = ?, comment = ? " +
                "WHERE name = ?")) {

            stmt.setString(1, record.getState());
            stmt.setString(2, isoFormat.format(new Date()));
            stmt.setString(3, record.getComment());
            stmt.setString(4, record.getTypesystemName());
            stmt.execute();
        } catch (Exception e) {
            throw new UncheckedUpgradeException(e);
        }
    }

    public String createNextTypesystem() throws Exception {
        final String name = "ts" + typesystemFormat.format(new Date());

        // Update the state of all "new" records in the database with "skipped"
        final List<TypesystemRecord> typesystemRecords = loadRecords();
        typesystemRecords.stream()
                .filter(ts -> ts.getState().equals(STATE_NEXT))
                .map(ts -> ts.withState(STATE_SKIPPED).withComment("Skipped to create typesystem " + name))
                .forEach(this::updateTypeSytem);

        createTypeSystem(new TypesystemRecord(name, STATE_NEXT, new Date(), new Date(), "Creating new typesystem"));

        return name;
    }

    public String setCurrentTypesystem(final String name) throws Exception {

        final List<TypesystemRecord> typesystemRecords = loadRecords();
        final Optional<TypesystemRecord> oldCurrentTypesystem = typesystemRecords.stream()
                .filter(ts -> ts.getState().equals(STATE_CURRENT))
                .findFirst();

        final Optional<TypesystemRecord> newCurrentTypesystem = typesystemRecords.stream()
                .filter(ts -> ts.getState().equals(STATE_CURRENT) || ts.getState().equals(STATE_NEXT))
                .filter(ts -> ts.getTypesystemName().equals(name))
                .findFirst();

        if (!newCurrentTypesystem.isPresent()) {
            throw new IllegalArgumentException("Typesystem " + name + " does not exist");
        }

        if (oldCurrentTypesystem.isPresent()) {
            updateTypeSytem(oldCurrentTypesystem.get().withComment("Replaced by typesystem " + name).withState(STATE_OLD));
        }

        updateTypeSytem(newCurrentTypesystem.get().withState(STATE_CURRENT).withComment("Current Typesystem"));
        return DEFAULT_TYPESYSTEM_NAME;
    }


}