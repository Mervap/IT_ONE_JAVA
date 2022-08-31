package ru.vk.competition.minbenchmark.service;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.test.StepVerifier;
import ru.vk.competition.minbenchmark.entity.ColumnInfo;
import ru.vk.competition.minbenchmark.entity.DBTable;
import ru.vk.competition.minbenchmark.entity.SingleQuery;
import ru.vk.competition.minbenchmark.entity.TableQuery;

import java.util.Arrays;

@SpringBootTest
public class TableSingleLinkTest {

  @Autowired
  private TableService tableService;

  @Autowired
  private TableQueryService tableQueryService;

  @Autowired
  private SingleQueryService singleQueryService;

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
    tableQueryService.clear();
    singleQueryService.clear();
  }

  @Test
  void getAllDifferent() {
    var query1 = new TableQuery(1, "Test", "select * from Test");
    var query2 = new TableQuery(2, "Test", "select * from Test");
    var queryList1 = Arrays.asList(query1, query2);

    var query3 = new SingleQuery(1, "select * from Test");
    var query4 = new SingleQuery(2, "select * from Test");
    var queryList2 = Arrays.asList(query3, query4);

    StepVerifier.create(tableQueryService.addNewTableQuery(query1)).expectNext(true).verifyComplete();
    StepVerifier.create(tableQueryService.addNewTableQuery(query2)).expectNext(true).verifyComplete();
    StepVerifier.create(singleQueryService.addNewQuery(query3)).expectNext(true).verifyComplete();
    StepVerifier.create(singleQueryService.addNewQuery(query4)).expectNext(true).verifyComplete();

    StepVerifier.create(tableQueryService.getAllTableQueries()).expectNext(queryList1).verifyComplete();
    StepVerifier.create(singleQueryService.getAllQueries()).expectNext(queryList2).verifyComplete();
  }
}
