package ru.vk.competition.minbenchmark.service;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.util.Pair;
import reactor.test.StepVerifier;
import ru.vk.competition.minbenchmark.entity.ColumnInfo;
import ru.vk.competition.minbenchmark.entity.DBTable;

import java.util.ArrayList;

import static ru.vk.competition.minbenchmark.util.TableSchemaUtil.CHARACTER_TYPE;
import static ru.vk.competition.minbenchmark.util.TableSchemaUtil.INTEGER_TYPE;

@SpringBootTest
public class TableServiceTest {

  @Autowired
  private TableService tableService;

  @AfterEach
  void cleanDB() {
    tableService.dropTable(TEST_TABLE).block();
  }

  @Test
  void createAndGetCorrectTable() {
    var dbs = createTestTable(TEST_TABLE, "id",
      ID_COLUMN,
      new TestColumnInfo("name", "VARCHAR(40)", CHARACTER_TYPE),
      new TestColumnInfo("description", "varchar", CHARACTER_TYPE)
    );
    var requestDB = dbs.getFirst();
    var verifyDB = dbs.getSecond();

    StepVerifier.create(tableService.createTable(requestDB)).expectNext(true).verifyComplete();
    StepVerifier.create(tableService.getTableByName(TEST_TABLE)).expectNext(verifyDB).verifyComplete();
  }

  @Test
  void createBadTableName() {
    var db = createTestTable("TestЛОЛ", "id", ID_COLUMN).getFirst();

    StepVerifier.create(tableService.createTable(db)).expectNext(false).verifyComplete();
  }

  @Test
  void createBadColumnName() {
    var db = createTestTable(
      TEST_TABLE, "id",
      new TestColumnInfo("id🥺", "int4", INTEGER_TYPE)
    ).getFirst();

    StepVerifier.create(tableService.createTable(db)).expectNext(false).verifyComplete();
  }

  @Test
  void createBadTypeName() {
    var db = createTestTable(
      TEST_TABLE, "id",
      new TestColumnInfo("id", "mamabama", INTEGER_TYPE)
    ).getFirst();

    StepVerifier.create(tableService.createTable(db)).expectNext(false).verifyComplete();
  }

  @Test
  void createTwice() {
    var db = createTestTable(TEST_TABLE, "id", ID_COLUMN).getFirst();

    StepVerifier.create(tableService.createTable(db)).expectNext(true).verifyComplete();
    StepVerifier.create(tableService.createTable(db)).expectNext(false).verifyComplete();
  }

  @Test
  void createBadAmount() {
    var db = createTestTable(TEST_TABLE, 33, "id", ID_COLUMN).getFirst();

    StepVerifier.create(tableService.createTable(db)).expectNext(false).verifyComplete();
  }

  @Test
  void getNonExisting() {
    var db = createTestTable(TEST_TABLE, "id", ID_COLUMN).getFirst();

    StepVerifier.create(tableService.createTable(db)).expectNext(true).verifyComplete();
    StepVerifier.create(tableService.getTableByName("Wat")).verifyComplete();
  }

  @Test
  void dropExisting() {
    var db = createTestTable(TEST_TABLE, "id", ID_COLUMN);
    var createDb = db.getFirst();
    var verifyDb = db.getSecond();

    StepVerifier.create(tableService.createTable(createDb)).expectNext(true).verifyComplete();
    StepVerifier.create(tableService.getTableByName(TEST_TABLE)).expectNext(verifyDb).verifyComplete();
    StepVerifier.create(tableService.dropTable(TEST_TABLE)).expectNext(true).verifyComplete();
    StepVerifier.create(tableService.getTableByName(TEST_TABLE)).verifyComplete();
  }

  @Test
  void dropNonExisting() {
    var db = createTestTable(TEST_TABLE, "id", ID_COLUMN);
    var createDb = db.getFirst();
    var verifyDb = db.getSecond();

    StepVerifier.create(tableService.createTable(createDb)).expectNext(true).verifyComplete();
    StepVerifier.create(tableService.getTableByName(TEST_TABLE)).expectNext(verifyDb).verifyComplete();
    StepVerifier.create(tableService.dropTable("Wat")).expectNext(false).verifyComplete();
    StepVerifier.create(tableService.getTableByName(TEST_TABLE)).expectNext(verifyDb).verifyComplete();
  }

  private static Pair<DBTable, DBTable> createTestTable(String name, String primaryKey, TestColumnInfo... columns) {
    return createTestTable(name, columns.length, primaryKey, columns);
  }

  private static Pair<DBTable, DBTable> createTestTable(String name, int columnsAmount, String primaryKey, TestColumnInfo... columns) {
    var testColumns = new ArrayList<ColumnInfo>();
    var verifyColumns = new ArrayList<ColumnInfo>();
    for (var column : columns) {
      testColumns.add(new ColumnInfo(column.name, column.type));
      verifyColumns.add(new ColumnInfo(column.name.toUpperCase(), column.normalizerType));
    }
    return Pair.of(
      new DBTable(name, columnsAmount, primaryKey, testColumns),
      new DBTable(name, verifyColumns.size(), primaryKey.toLowerCase(), verifyColumns)
    );
  }

  @Data
  @AllArgsConstructor
  private static class TestColumnInfo {
    String name;
    String type;
    String normalizerType;
  }

  private static final String TEST_TABLE = "Test";
  private static final TestColumnInfo ID_COLUMN = new TestColumnInfo("id", "int4", INTEGER_TYPE);
}

