package ru.vk.competition.minbenchmark.repository;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import ru.vk.competition.minbenchmark.entity.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Repository
@Slf4j
@RequiredArgsConstructor
public class RawRepository {
  private final JdbcTemplate jdbcTemplate;

  private boolean tablesInvalid = false;
  private final ConcurrentSkipListSet<String> cachedTableNames = new ConcurrentSkipListSet<>();

  private final ConcurrentHashMap<String, Optional<DBTable>> cachedTableInfos = new ConcurrentHashMap<>();

  private final ConcurrentHashMap<Integer, TableQuery> queries = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<Integer, SingleQuery> singleQueries = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, ConcurrentSkipListSet<Integer>> tableQueries = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<Integer, Report<ColumnInfoWithSize>> reports = new ConcurrentHashMap<>();

  public boolean createTable(DBTable table) {
    if (tableExists(table.getName())) {
      log.info("Cannot create table: already in cache");
      return false;
    }
    try {
      jdbcTemplate.execute(buildCreateTableQuery(table));
      cachedTableNames.add(table.getName().toUpperCase());
      return true;
    } catch (Exception e) {
      log.info("Cannot create table '" + table.getName() + "': " + e.getMessage());
      return false;
    }
  }

  public boolean dropTable(String name) {
    if (!tableExists(name.toUpperCase())) {
      log.info("Cannot drop table: missed in cache");
      return false;
    }
    try {
      jdbcTemplate.execute("DROP TABLE " + name);
      cachedTableNames.remove(name.toUpperCase());
      cachedTableInfos.remove(name.toUpperCase());
      if (tableQueries.containsKey(name)) {
        for (var query : tableQueries.get(name)) {
          queries.remove(query);
        }
        tableQueries.remove(name);
      }
      return true;
    } catch (Exception e) {
      log.info("Cannot drop table '" + name + "': " + e.getMessage());
      return false;
    }
  }

  public Optional<DBTable> getTableInfo(String name) {
    if (!cachedTableInfos.containsKey(name.toUpperCase())) {
      Optional<DBTable> value;
      try {
        if (tableExists(name)) {
          var columnsWithKeys = jdbcTemplate.query("show columns from " + name, new ColumnMapper());
          var columns = columnsWithKeys.stream().map(it -> it.info).collect(Collectors.toList());
          var keyColumn = columnsWithKeys.stream().filter(it -> it.isPrimary).findFirst().map(c -> c.info.getName()).orElse("");
          value = Optional.of(new DBTable(name, columns.size(), keyColumn.toLowerCase(), columns));
        } else {
          value = Optional.empty();
        }
      } catch (Exception e) {
        log.error(e.getMessage());
        value = Optional.empty();
      }
      cachedTableInfos.put(name.toUpperCase(), value);
    }
    return cachedTableInfos.get(name.toUpperCase());
  }

  AtomicInteger kek = new AtomicInteger();

  public Optional<List<ColumnInfo>> mapColumnTypes(List<ColumnInfo> infos) {
    if (infos.isEmpty()) {
      return Optional.of(infos);
    }
    var tableName = "ZZZ_FAKE_TABLE_ZZZ_" + kek.getAndIncrement();
    if (!createTable(new DBTable(tableName, infos.size(), infos.get(0).getName(), infos))) {
      return Optional.empty();
    }
    var mapped = getTableInfo(tableName).get().getColumnInfos();
    dropTable(tableName);
    return Optional.of(mapped);
  }

  public boolean newTableQuery(TableQuery query) {
    String tableName = query.getTableName();
    if (!tableExists(tableName)) {
      log.error("Cannot add new table query: missed table '" + tableName + "'");
      return false;
    }

    int id = query.getId();
    if (queries.containsKey(id)) {
      log.error("Cannot add new table query: id already exists " + id);
      return false;
    }

    if (query.getQuery().length() > 120) {
      log.error("Cannot add new table query: too big query");
      return false;
    }

    queries.put(id, query);
    tableQueries.putIfAbsent(tableName, new ConcurrentSkipListSet<>());
    tableQueries.get(tableName).add(id);
    return true;
  }

  public boolean updateTableQuery(TableQuery query) {
    int id = query.getId();
    if (!queries.containsKey(id)) {
      log.error("Cannot update table query: id not exists " + id);
      return false;
    }

    if (query.getQuery().length() > 120) {
      log.error("Cannot update table query: query too big " + query.getQuery().length());
      queries.get(id).setQuery("");
      return false;
    }

    String tableName = query.getTableName();
    String oldTableName = queries.get(id).getTableName();
    if (!oldTableName.equals(tableName)) {
      if (!tableExists(tableName)) {
        log.error("Cannot update table query: table not exists " + tableName);
        queries.get(id).setQuery("");
        return false;
      }
      tableQueries.get(oldTableName).remove(id);
      tableQueries.putIfAbsent(tableName, new ConcurrentSkipListSet<>());
      tableQueries.get(tableName).add(id);
    }
    queries.put(id, query);
    return true;
  }

  public boolean deleteTableQuery(int id) {
    if (!queries.containsKey(id)) {
      log.error("Cannot delete table query: id not exists " + id);
      return false;
    }

    var tableName = queries.remove(id).getTableName();
    tableQueries.get(tableName).remove(id);
    return true;
  }

  public boolean executeTableQuery(int id) {
    var query = queries.get(id);
    if (query == null) {
      log.error("Cannot execute table query: id not exists " + id);
      return false;
    }

    tablesInvalid = true;
    cachedTableInfos.clear();
    try {
      jdbcTemplate.execute(query.getQuery());
      return true;
    } catch (Exception e) {
      log.error("Cannot execute table query " + id + ": " + e.getMessage());
      return false;
    }
  }

  public Optional<List<TableQuery>> getTableQueries(String name) {
    if (!tableExists(name)) {
      return Optional.empty();
    }
    if (!tableQueries.containsKey(name)) {
      return Optional.of(Collections.emptyList());
    }
    return Optional.of(tableQueries.get(name).stream().map(queries::get).collect(Collectors.toList()));
  }

  public Optional<TableQuery> getTableQueryById(int id) {
    if (!queries.containsKey(id)) {
      return Optional.empty();
    }

    return Optional.of(queries.get(id));
  }

  public Collection<TableQuery> getAllTableQueries() {
    return queries.values();
  }

  public boolean newSingleQuery(SingleQuery query) {
    int id = query.getId();
    if (singleQueries.containsKey(id)) {
      log.error("Cannot add new single query: id already exists " + id);
      return false;
    }

    if (query.getQuery().length() > 120) {
      log.error("Cannot add new single query: too big query");
      return false;
    }

    singleQueries.put(id, query);
    return true;
  }

  public boolean updateSingleQuery(SingleQuery query) {
    int id = query.getId();
    if (!singleQueries.containsKey(id)) {
      log.error("Cannot update single query: id not exists " + id);
      return false;
    }

    if (query.getQuery().length() > 120) {
      log.error("Cannot update single query: query too big " + query.getQuery().length());
      singleQueries.get(id).setQuery("");
      return false;
    }

    singleQueries.put(id, query);
    return true;
  }

  public boolean deleteSingleQuery(int id) {
    if (!singleQueries.containsKey(id)) {
      log.error("Cannot delete single query: id not exists " + id);
      return false;
    }

    singleQueries.remove(id);
    return true;
  }

  public boolean executeSingleQuery(int id) {
    var query = singleQueries.get(id);
    if (query == null) {
      log.error("Cannot execute single query: id not exists " + id);
      return false;
    }

    tablesInvalid = true;
    cachedTableInfos.clear();
    try {
      jdbcTemplate.execute(query.getQuery());
      return true;
    } catch (Exception e) {
      log.error("Cannot execute single query " + id + ": " + e.getMessage());
      return false;
    }
  }

  public Optional<SingleQuery> getSingleQueryById(int id) {
    if (!singleQueries.containsKey(id)) {
      return Optional.empty();
    }

    return Optional.of(singleQueries.get(id));
  }

  public Collection<SingleQuery> getAllSingleQueries() {
    return singleQueries.values();
  }

  public boolean createReport(Report<ColumnInfo> report) {
    int id = report.getId();
    if (reports.containsKey(id)) {
      log.error("Cannot create report: id already exists " + id);
      return false;
    }
    var mapped = report.getTables().stream().map(this::withSize).collect(Collectors.toList());
    reports.put(id, new Report<>(id, report.getTableAmount(), mapped));
    return true;
  }

  public Optional<Report<ColumnInfoWithSize>> getReportById(int id) {
    if (!reports.containsKey(id)) {
      log.error("Cannot get report: id not exists " + id);
      return Optional.empty();
    }
    var report = reports.get(id);
    try {
      var mapped = report.getTables().stream().map(this::updateSize).collect(Collectors.toList());
      return Optional.of(new Report<>(id, report.getTableAmount(), mapped));
    } catch (Exception e) {
      log.error("Failed to update report");
      return Optional.of(report);
    }
  }

  private boolean tableExists(String tableName) {
    if (tablesInvalid) {
      fetchTableNames();
      tablesInvalid = false;
    }
    return cachedTableNames.contains(tableName.toUpperCase());
  }

  private void fetchTableNames() {
    cachedTableNames.clear();
    try {
      cachedTableNames.addAll(jdbcTemplate.query("SHOW TABLES", new TableMapper()));
    } catch (Exception ignore) {
    }
  }

  public void clear() {
    fetchTableNames();
    reports.clear();
    queries.clear();
    singleQueries.clear();
    tableQueries.clear();
    for (var table : cachedTableNames) {
      dropTable(table);
    }
    cachedTableNames.clear();
    cachedTableInfos.clear();
  }

  private ReportTable<ColumnInfoWithSize> withSize(ReportTable<ColumnInfo> table) {
    var mapped = table.getColumns().stream().map(c -> {
      try {
        return withSize(c, table.getName());
      } catch (Exception e) {
        log.error("Failed to fetch column size: " + c.getName() + " from " + c.getType() + ": " + e);
        return new ColumnInfoWithSize(c.getName(), c.getType(), "0");
      }
    }).collect(Collectors.toList());
    return new ReportTable<>(table.getName(), mapped);
  }

  private ReportTable<ColumnInfoWithSize> updateSize(ReportTable<ColumnInfoWithSize> table) {
    var mapped = table.getColumns().stream()
      .map(c -> new ColumnInfo(c.getName(), c.getType()))
      .map(c -> withSize(c, table.getName()))
      .collect(Collectors.toList());
    return new ReportTable<>(table.getName(), mapped);
  }

  private ColumnInfoWithSize withSize(ColumnInfo column, String tableName) {
    var size = jdbcTemplate.query("select count(" + column.getName() + ") from " + tableName, new CountExtractor());
    return new ColumnInfoWithSize(column.getName(), column.getType(), size);
  }

  private String buildCreateTableQuery(DBTable table) {
    var builder = new StringBuilder("CREATE TABLE ");
    builder.append(table.getName()).append(" (");
    for (var column : table.getColumnInfos()) {
      builder.append(column.getName()).append(" ").append(column.getType());
      if (column.getName().equals(table.getPrimaryKey())) {
        builder.append(" PRIMARY KEY");
      }
      builder.append(", ");
    }
    builder.deleteCharAt(builder.length() - 2).append(");");
    return builder.toString();
  }

  private static class ColumnMapper implements RowMapper<ColumnWithKey> {

    @Override
    public ColumnWithKey mapRow(ResultSet rs, int rowNum) throws SQLException {
      ColumnInfo column = new ColumnInfo();
      column.setName(rs.getString("COLUMN_NAME"));
      column.setType(rs.getString("TYPE").replaceAll("\\(\\d+\\)", ""));
      return new ColumnWithKey(column, rs.getString("KEY").equals("PRI"));
    }
  }

  private static class TableMapper implements RowMapper<String> {

    @Override
    public String mapRow(ResultSet rs, int rowNum) throws SQLException {
      return rs.getString("TABLE_NAME");
    }
  }

  private static class CountExtractor implements ResultSetExtractor<String> {
    @Override
    public String extractData(ResultSet rs) throws SQLException, DataAccessException {
      rs.next();
      return rs.getString(1);
    }
  }

  @Data
  @AllArgsConstructor
  private static class ColumnWithKey {
    ColumnInfo info;
    boolean isPrimary;
  }
}
