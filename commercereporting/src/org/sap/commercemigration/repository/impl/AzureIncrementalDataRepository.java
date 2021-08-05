package org.sap.commercemigration.repository.impl;

import com.google.common.base.Joiner;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.sap.commercemigration.MarkersQueryDefinition;
import org.sap.commercemigration.OffsetQueryDefinition;
import org.sap.commercemigration.SeekQueryDefinition;
import org.sap.commercemigration.constants.CommercereportingConstants;
import org.sap.commercemigration.dataset.DataSet;
import org.sap.commercemigration.profile.DataSourceConfiguration;
import org.sap.commercemigration.service.DatabaseMigrationDataTypeMapperService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AzureIncrementalDataRepository extends AzureDataRepository{

  private static final Logger LOG = LoggerFactory.getLogger(AzureIncrementalDataRepository.class);

  private static String  deletionTable = "itemdeletionmarkers";

  public AzureIncrementalDataRepository(
      DataSourceConfiguration dataSourceConfiguration,
      DatabaseMigrationDataTypeMapperService databaseMigrationDataTypeMapperService) {
    super(dataSourceConfiguration, databaseMigrationDataTypeMapperService);
  }
  @Override
  protected String buildOffsetBatchQuery(OffsetQueryDefinition queryDefinition, String... conditions) {

    if(!queryDefinition.isDeletionEnabled()) {
      return super.buildOffsetBatchQuery(queryDefinition,conditions);
    }
    String orderBy = Joiner.on(',').join(queryDefinition.getAllColumns());
    return String.format("SELECT * FROM %s WHERE %s ORDER BY %s OFFSET %s ROWS FETCH NEXT %s ROWS ONLY", deletionTable, expandConditions(conditions), orderBy, queryDefinition.getOffset(), queryDefinition.getBatchSize());
  }

  @Override
  protected String buildValueBatchQuery(SeekQueryDefinition queryDefinition, String... conditions) {
    if(!queryDefinition.isDeletionEnabled()) {
      return super.buildValueBatchQuery(queryDefinition,conditions);
    }
    return String.format("select top %s * from %s where %s order by %s", queryDefinition.getBatchSize(), deletionTable, expandConditions(conditions), queryDefinition.getColumn());
  }

  @Override
  protected String buildBatchMarkersQuery(MarkersQueryDefinition queryDefinition, String... conditions) {
    if(!queryDefinition.isDeletionEnabled()) {
      return super.buildBatchMarkersQuery(queryDefinition,conditions);
    }
    String column = queryDefinition.getColumn();
    return String.format("SELECT t.%s, t.rownum\n" +
        "FROM\n" +
        "(\n" +
        "    SELECT %s, (ROW_NUMBER() OVER (ORDER BY %s))-1 AS rownum\n" +
        "    FROM %s\n WHERE %s" +
        ") AS t\n" +
        "WHERE t.rownum %% %s = 0\n" +
        "ORDER BY t.%s", column, column, column, deletionTable, expandConditions(conditions), queryDefinition.getBatchSize(), column);
  }

  @Override
  public DataSet getBatchOrderedByColumn(SeekQueryDefinition queryDefinition, Instant time) throws Exception {
    if(!queryDefinition.isDeletionEnabled()) {
      return super.getBatchOrderedByColumn(queryDefinition,time);
    }

    //get batches with modifiedts >= configured time for incremental migration
    List<String> conditionsList = new ArrayList<>(3);
    if (time != null) {
      conditionsList.add("modifiedts > ?");
    }
    conditionsList.add("p_table = ?");
    if (queryDefinition.getLastColumnValue() != null) {
      conditionsList.add(String.format("%s >= %s", queryDefinition.getColumn(), queryDefinition.getLastColumnValue()));
    }
    if (queryDefinition.getNextColumnValue() != null) {
      conditionsList.add(String.format("%s < %s", queryDefinition.getColumn(), queryDefinition.getNextColumnValue()));
    }
    String[] conditions = null;
    if (conditionsList.size() > 0) {
      conditions = conditionsList.toArray(new String[conditionsList.size()]);
    }
    try (Connection connection = getConnection();
        PreparedStatement stmt = connection.prepareStatement(buildValueBatchQuery(queryDefinition, conditions))) {
      stmt.setFetchSize(Long.valueOf(queryDefinition.getBatchSize()).intValue());
      if (time != null) {
        stmt.setTimestamp(1, Timestamp.from(time));
      }
      // setting table for the deletions
      stmt.setString(2,queryDefinition.getTable());

      ResultSet resultSet = stmt.executeQuery();
      return convertToBatchDataSet(resultSet);
    }
  }

  @Override
  public DataSet getBatchWithoutIdentifier(OffsetQueryDefinition queryDefinition, Instant time) throws Exception {

    if(!queryDefinition.isDeletionEnabled()) {
      return super.getBatchWithoutIdentifier(queryDefinition,time);
    }
    //get batches with modifiedts >= configured time for incremental migration
    List<String> conditionsList = new ArrayList<>(2);

    if (time != null) {
      conditionsList.add("modifiedts > ?");
    }
    conditionsList.add("p_table = ?");
    String[] conditions = null;
    if (conditionsList.size() > 0) {
      conditions = conditionsList.toArray(new String[conditionsList.size()]);
    }
    try (Connection connection = getConnection();
        PreparedStatement stmt = connection.prepareStatement(buildOffsetBatchQuery(queryDefinition, conditions))) {
      stmt.setFetchSize(Long.valueOf(queryDefinition.getBatchSize()).intValue());
      if (time != null) {
        stmt.setTimestamp(1, Timestamp.from(time));
      }
      // setting table for the deletions
      stmt.setString(2,queryDefinition.getTable());

      ResultSet resultSet = stmt.executeQuery();
      return convertToBatchDataSet(resultSet);
    }
  }

  @Override
  public DataSet getBatchMarkersOrderedByColumn(MarkersQueryDefinition queryDefinition, Instant time) throws Exception {

    if(!queryDefinition.isDeletionEnabled()) {
      return super.getBatchMarkersOrderedByColumn(queryDefinition,time);
    }
    //get batches with modifiedts >= configured time for incremental migration
    List<String> conditionsList = new ArrayList<>(2);
    processDefaultConditions(queryDefinition.getTable(), conditionsList);
    if (time != null) {
      conditionsList.add("modifiedts > ?");
    }
    // setting table for the deletions
    conditionsList.add("p_table = ?");

    String[] conditions = null;
    if (conditionsList.size() > 0) {
      conditions = conditionsList.toArray(new String[conditionsList.size()]);
    }
    try (Connection connection = getConnection();
        PreparedStatement stmt = connection.prepareStatement(buildBatchMarkersQuery(queryDefinition, conditions))) {
      stmt.setFetchSize(Long.valueOf(queryDefinition.getBatchSize()).intValue());
      if (time != null) {
        stmt.setTimestamp(1, Timestamp.from(time));
      }
      // setting table for the deletions
      stmt.setString(2,queryDefinition.getTable());

      ResultSet resultSet = stmt.executeQuery();
      return convertToBatchDataSet(resultSet);
    }
  }

  @Override
  public long getRowCountModifiedAfter(String table, Instant time,boolean isDeletionEnabled) throws SQLException {
    if(!isDeletionEnabled) {
      return super.getRowCountModifiedAfter(table,time,false);
    }
    //
    List<String> conditionsList = new ArrayList<>(2);
    processDefaultConditions(table, conditionsList);
    // setting table for the deletions
    conditionsList.add("p_table = ?");
    String[] conditions = null;
    if (conditionsList.size() > 0) {
      conditions = conditionsList.toArray(new String[conditionsList.size()]);
    }
    try (Connection connection = getConnection()) {
      try (PreparedStatement stmt = connection.prepareStatement(String.format("select count(*) from %s where modifiedts > ? AND %s", deletionTable, expandConditions(conditions)))) {
        stmt.setTimestamp(1, Timestamp.from(time));
        // setting table for the deletions
        stmt.setString(2,table);
        ResultSet resultSet = stmt.executeQuery();
        long value = 0;
        if (resultSet.next()) {
          value = resultSet.getLong(1);
        }
        return value;
      }
    }
  }

}
