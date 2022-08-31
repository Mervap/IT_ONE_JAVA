package ru.vk.competition.minbenchmark.service;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.test.StepVerifier;
import ru.vk.competition.minbenchmark.entity.ColumnInfo;
import ru.vk.competition.minbenchmark.entity.DBTable;
import ru.vk.competition.minbenchmark.entity.TableQuery;

import java.util.Arrays;
import java.util.Collections;


@SpringBootTest
public class TableQueryServiceTest {

  @Autowired
  private TableService tableService;

  @Autowired
  private TableQueryService queryService;

  @BeforeEach
  void createDB() {
    tableService.createTable(new DBTable("Test", 1, "id", Arrays.asList(
      new ColumnInfo("id", "int4")
    ))).block();
    tableService.createTable(new DBTable("Test1", 1, "id", Arrays.asList(
      new ColumnInfo("id", "int4")
    ))).block();
  }

  @AfterEach
  void clear() {
    queryService.clear();
  }

  @Test
  void addNewQuery() {
    var query = new TableQuery(1, "Test", "select * from Test");
    StepVerifier.create(queryService.addNewTableQuery(query)).expectNext(true).verifyComplete();
  }

  @Test
  void addNewQueryBadId() {
    var query = new TableQuery(1, "Test", "select * from Test");
    StepVerifier.create(queryService.addNewTableQuery(query)).expectNext(true).verifyComplete();
    StepVerifier.create(queryService.addNewTableQuery(query)).expectNext(false).verifyComplete();
  }

  @Test
  void addNewQueryBadTable() {
    var query = new TableQuery(1, "Lol", "select * from Test");
    StepVerifier.create(queryService.addNewTableQuery(query)).expectNext(false).verifyComplete();
  }

  @Test
  void addNewQueryTooBig() {
    var query = new TableQuery(1, "Test", "select * from Test" + "r".repeat(140));
    StepVerifier.create(queryService.addNewTableQuery(query)).expectNext(false).verifyComplete();
  }

  @Test
  void updateQuery() {
    var query1 = new TableQuery(1, "Test", "select id from Test");
    var query2 = new TableQuery(1, "Test", "select id, name from Test");
    StepVerifier.create(queryService.addNewTableQuery(query1)).expectNext(true).verifyComplete();
    StepVerifier.create(queryService.updateTableQuery(query2)).expectNext(true).verifyComplete();
  }

  @Test
  void updateQueryTooBig() {
    var query1 = new TableQuery(1, "Test", "select * from Test");
    var query2 = new TableQuery(1, "Test", "select * from Test" + "r".repeat(140));
    StepVerifier.create(queryService.addNewTableQuery(query1)).expectNext(true).verifyComplete();
    StepVerifier.create(queryService.updateTableQuery(query2)).expectNext(false).verifyComplete();
  }

  @Test
  void updateQueryChangeTable() {
    var query1 = new TableQuery(1, "Test", "select * from Test");
    var query2 = new TableQuery(1, "Test1", "select id, name from Test");

    StepVerifier.create(queryService.addNewTableQuery(query1)).expectNext(true).verifyComplete();
    StepVerifier.create(queryService.getTableQueries("Test")).expectNext(Collections.singletonList(query1)).verifyComplete();
    StepVerifier.create(queryService.getTableQueries("Test1")).expectNext(Collections.emptyList()).verifyComplete();

    StepVerifier.create(queryService.updateTableQuery(query2)).expectNext(true).verifyComplete();
    StepVerifier.create(queryService.getTableQueries("Test")).expectNext(Collections.emptyList()).verifyComplete();
    StepVerifier.create(queryService.getTableQueries("Test1")).expectNext(Collections.singletonList(query2)).verifyComplete();
  }

  void updateQueryChangeTableNotExists() {
    var query1 = new TableQuery(1, "Test", "select * from Test");
    var query2 = new TableQuery(1, "Test2", "select id, name from Test2");

    StepVerifier.create(queryService.addNewTableQuery(query1)).expectNext(true).verifyComplete();
    StepVerifier.create(queryService.getTableQueries("Test")).expectNext(Collections.singletonList(query1)).verifyComplete();
    StepVerifier.create(queryService.getTableQueries("Test2")).verifyComplete();

    StepVerifier.create(queryService.updateTableQuery(query2)).expectNext(false).verifyComplete();
  }

  @Test
  void updateQueryBadTable() {
    var query = new TableQuery(1, "Lol", "select * from Test");
    StepVerifier.create(queryService.updateTableQuery(query)).expectNext(false).verifyComplete();
  }

  @Test
  void updateQueryBadId() {
    var query1 = new TableQuery(1, "Test", "select * from Test");
    var query2 = new TableQuery(1, "Test", "select * from Test");
    var query3 = new TableQuery(2, "Test", "select * from Test");
    StepVerifier.create(queryService.addNewTableQuery(query1)).expectNext(true).verifyComplete();
    StepVerifier.create(queryService.updateTableQuery(query2)).expectNext(true).verifyComplete();
    StepVerifier.create(queryService.updateTableQuery(query3)).expectNext(false).verifyComplete();
  }

  @Test
  void deleteQuery() {
    var query = new TableQuery(1, "Test", "select * from Test");
    StepVerifier.create(queryService.addNewTableQuery(query)).expectNext(true).verifyComplete();
    StepVerifier.create(queryService.deleteTableQuery(query.getId())).expectNext(true).verifyComplete();
  }

  @Test
  void deleteBadQuery() {
    StepVerifier.create(queryService.deleteTableQuery(1)).expectNext(false).verifyComplete();
  }

  @Test
  void executeQuery() {
    var query = new TableQuery(1, "Test", "select * from Test");
    StepVerifier.create(queryService.addNewTableQuery(query)).expectNext(true).verifyComplete();
    StepVerifier.create(queryService.executeTableQuery(query.getId())).expectNext(true).verifyComplete();
  }

  @Test
  void executeBadQuery() {
    var query = new TableQuery(1, "Test", "azaza");
    StepVerifier.create(queryService.addNewTableQuery(query)).expectNext(true).verifyComplete();
    StepVerifier.create(queryService.executeTableQuery(query.getId())).expectNext(false).verifyComplete();
  }

  @Test
  void executeBadQueryId() {
    StepVerifier.create(queryService.executeTableQuery(1)).expectNext(false).verifyComplete();
  }

  @Test
  void getAllQueries() {
    var query1 = new TableQuery(1, "Test", "select * from Test");
    var query2 = new TableQuery(2, "Test1", "select * from Test1");
    StepVerifier.create(queryService.addNewTableQuery(query1)).expectNext(true).verifyComplete();
    StepVerifier.create(queryService.addNewTableQuery(query2)).expectNext(true).verifyComplete();
    StepVerifier.create(queryService.getTableQueries("Test")).expectNext(Collections.singletonList(query1)).verifyComplete();
  }

  @Test
  void getAllQueriesBadTable() {
    StepVerifier.create(queryService.getTableQueries("Test+++")).verifyComplete();
  }

  @Test
  void getAllQueriesEmptyTable() {
    StepVerifier.create(queryService.getTableQueries("Test")).expectNext(Collections.emptyList()).verifyComplete();
  }

  @Test
  void getAllQueriesAfterDelete() {
    var query1 = new TableQuery(1, "Test", "select * from Test");
    var query2 = new TableQuery(2, "Test", "select id, name from Test");
    StepVerifier.create(queryService.addNewTableQuery(query1)).expectNext(true).verifyComplete();
    StepVerifier.create(queryService.addNewTableQuery(query2)).expectNext(true).verifyComplete();
    StepVerifier.create(queryService.getTableQueries("Test")).expectNext(Arrays.asList(query1, query2)).verifyComplete();
    StepVerifier.create(queryService.deleteTableQuery(1)).expectNext(true).verifyComplete();
    StepVerifier.create(queryService.getTableQueries("Test")).expectNext(Collections.singletonList(query2)).verifyComplete();
  }

  @Test
  void getById() {
    var query = new TableQuery(1, "Test", "select * from Test");
    StepVerifier.create(queryService.addNewTableQuery(query)).expectNext(true).verifyComplete();
    StepVerifier.create(queryService.getQueryById(1)).expectNext(query).verifyComplete();
  }

  @Test
  void getByIdBadId() {
    StepVerifier.create(queryService.getQueryById(1)).verifyComplete();
  }

  @Test
  void getAllTableQueries() {
    var query1 = new TableQuery(1, "Test", "select * from Test");
    var query2 = new TableQuery(2, "Test1", "select * from Test");
    StepVerifier.create(queryService.addNewTableQuery(query1)).expectNext(true).verifyComplete();
    StepVerifier.create(queryService.addNewTableQuery(query2)).expectNext(true).verifyComplete();
    StepVerifier.create(queryService.getAllTableQueries()).expectNext(Arrays.asList(query1, query2)).verifyComplete();
  }

  @Test
  void getAllTableQueriesEmpty() {
    StepVerifier.create(queryService.getAllTableQueries()).expectNext(Collections.emptyList()).verifyComplete();
  }
}
