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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.stream.Collectors;

@Repository
@Slf4j
@RequiredArgsConstructor
public class RawRepository {
  private final JdbcTemplate jdbcTemplate;

  public boolean createTable(DBTable table) {
    try {
      jdbcTemplate.execute(buildCreateTableQuery(table));
      return true;
    } catch (Exception e) {
      log.error("Cannot create table '" + table.getName() + "': " + e.getMessage());
      return false;
    }
  }

  public boolean dropTable(String name) {
    try {
      jdbcTemplate.execute("DROP TABLE " + name);
      return true;
    } catch (Exception e) {
      log.error("Cannot drop table '" + name + "': " + e.getMessage());
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
