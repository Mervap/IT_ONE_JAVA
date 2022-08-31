package ru.vk.competition.minbenchmark.repository;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import ru.vk.competition.minbenchmark.entity.ColumnInfo;
import ru.vk.competition.minbenchmark.entity.DBTable;
import ru.vk.competition.minbenchmark.entity.SingleQuery;
import ru.vk.competition.minbenchmark.entity.TableQuery;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;

@Repository
@Slf4j
@RequiredArgsConstructor
public class RawRepository {
  private final JdbcTemplate jdbcTemplate;
  private final ConcurrentSkipListSet<String> cachedTableNames = new ConcurrentSkipListSet<>();
  private final ConcurrentHashMap<Integer, TableQuery> queries = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<Integer, SingleQuery> singleQueries = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, ConcurrentSkipListSet<Integer>> tableQueries = new ConcurrentHashMap<>();

  public boolean createTable(DBTable table) {
    if (cachedTableNames.contains(table.getName())) {
      log.warn("Cannot create table: already in cache");
      return false;
    }
    try {
      jdbcTemplate.execute(buildCreateTableQuery(table));
      cachedTableNames.add(table.getName());
      return true;
    } catch (Exception e) {
      log.warn("Cannot create table '" + table.getName() + "': " + e.getMessage());
      return false;
    }
  }

  public boolean dropTable(String name) {
    if (!cachedTableNames.remove(name)) {
      log.warn("Cannot drop table: missed in cache");
      return false;
    }
    try {
      jdbcTemplate.execute("DROP TABLE " + name);
      return true;
    } catch (Exception e) {
      log.warn("Cannot drop table '" + name + "': " + e.getMessage());
      return false;
    }
  }

  public Optional<DBTable> getTableInfo(String name) {
    try {
      var columnsWithKeys = jdbcTemplate.query("show columns from " + name, new ColumnMapper());
      var columns = columnsWithKeys.stream().map(it -> it.info).collect(Collectors.toList());
      var keyColumn = columnsWithKeys.stream().filter(it -> it.isPrimary).findFirst().get().info.getName();
      return Optional.of(new DBTable(name, columns.size(), keyColumn.toLowerCase(), columns));
    } catch (Exception e) {
      log.error(e.getMessage());
      return Optional.empty();
    }
  }

  public boolean newTableQuery(TableQuery query) {
    String tableName = query.getTableName();
    if (!cachedTableNames.contains(tableName)) {
      log.error("Cannot add new table query: missed table '" + tableName + "'");
      return false;
    }

    int id = query.getId();
    if (queries.containsKey(id) || singleQueries.containsKey(id)) {
      log.error("Cannot add new table query: id already exists " + id);
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

    String tableName = query.getTableName();
    if (!queries.get(id).getTableName().equals(tableName)) {
      log.error("Cannot update table query: query related to another table " + id);
      return false;
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

    try {
      jdbcTemplate.execute(query.getQuery());
      return true;
    } catch (Exception e) {
      log.warn("Cannot execute table query " + id + ": " + e.getMessage());
      return false;
    }
  }

  public Optional<List<TableQuery>> getTableQueries(String name) {
    if (!cachedTableNames.contains(name)) {
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
    if (singleQueries.containsKey(id) || queries.containsKey(id)) {
      log.error("Cannot add new single query: id already exists " + id);
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

    try {
      jdbcTemplate.execute(query.getQuery());
      return true;
    } catch (Exception e) {
      log.warn("Cannot execute single query " + id + ": " + e.getMessage());
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

  public void clear() {
    queries.clear();
    singleQueries.clear();
    tableQueries.clear();
    for (var table : cachedTableNames) {
      dropTable(table);
    }
    cachedTableNames.clear();
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

  @Data
  @AllArgsConstructor
  private static class ColumnWithKey {
    ColumnInfo info;
    boolean isPrimary;
  }
}
