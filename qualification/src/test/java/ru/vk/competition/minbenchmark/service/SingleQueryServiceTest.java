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
import java.util.Collections;


@SpringBootTest
public class SingleQueryServiceTest {

  @Autowired
  private SingleQueryService queryService;

  @AfterEach
  void clear() {
    queryService.clear();
  }

  @Test
  void addNewQuery() {
    var query = new SingleQuery(1, "select * from Test");
    StepVerifier.create(queryService.addNewQuery(query)).expectNext(true).verifyComplete();
  }

  @Test
  void addNewQueryBadId() {
    var query = new SingleQuery(1, "select * from Test");
    StepVerifier.create(queryService.addNewQuery(query)).expectNext(true).verifyComplete();
    StepVerifier.create(queryService.addNewQuery(query)).expectNext(false).verifyComplete();
  }

  @Test
  void updateQuery() {
    var query1 = new SingleQuery(1, "select id from Test");
    var query2 = new SingleQuery(1, "select id, name from Test");
    StepVerifier.create(queryService.addNewQuery(query1)).expectNext(true).verifyComplete();
    StepVerifier.create(queryService.updateQuery(query2)).expectNext(true).verifyComplete();
  }

  @Test
  void updateQueryBadId() {
    var query1 = new SingleQuery(1, "select * from Test");
    var query2 = new SingleQuery(1, "select * from Test");
    var query3 = new SingleQuery(2, "select * from Test");
    StepVerifier.create(queryService.addNewQuery(query1)).expectNext(true).verifyComplete();
    StepVerifier.create(queryService.updateQuery(query2)).expectNext(true).verifyComplete();
    StepVerifier.create(queryService.updateQuery(query3)).expectNext(false).verifyComplete();
  }

  @Test
  void deleteQuery() {
    var query = new SingleQuery(1, "select * from Test");
    StepVerifier.create(queryService.addNewQuery(query)).expectNext(true).verifyComplete();
    StepVerifier.create(queryService.deleteQuery(query.getId())).expectNext(true).verifyComplete();
  }

  @Test
  void deleteBadQuery() {
    StepVerifier.create(queryService.deleteQuery(1)).expectNext(false).verifyComplete();
  }

  @Test
  void executeQuery() {
    var query = new SingleQuery(1, "show tables;");
    StepVerifier.create(queryService.addNewQuery(query)).expectNext(true).verifyComplete();
    StepVerifier.create(queryService.executeQuery(query.getId())).expectNext(true).verifyComplete();
  }

  @Test
  void executeBadQuery() {
    var query = new SingleQuery(1, "azaza");
    StepVerifier.create(queryService.addNewQuery(query)).expectNext(true).verifyComplete();
    StepVerifier.create(queryService.executeQuery(query.getId())).expectNext(false).verifyComplete();
  }

  @Test
  void executeBadQueryId() {
    StepVerifier.create(queryService.executeQuery(1)).expectNext(false).verifyComplete();
  }

  @Test
  void getById() {
    var query = new SingleQuery(1, "select * from Test");
    StepVerifier.create(queryService.addNewQuery(query)).expectNext(true).verifyComplete();
    StepVerifier.create(queryService.getQueryById(1)).expectNext(query).verifyComplete();
  }

  @Test
  void getByIdBadId() {
    StepVerifier.create(queryService.getQueryById(1)).verifyComplete();
  }

  @Test
  void getAllQueries() {
    var query1 = new SingleQuery(1, "select * from Test");
    var query2 = new SingleQuery(2, "select * from Test");
    StepVerifier.create(queryService.addNewQuery(query1)).expectNext(true).verifyComplete();
    StepVerifier.create(queryService.addNewQuery(query2)).expectNext(true).verifyComplete();
    StepVerifier.create(queryService.getAllQueries()).expectNext(Arrays.asList(query1, query2)).verifyComplete();
  }

  @Test
  void getAllQueriesEmpty() {
    StepVerifier.create(queryService.getAllQueries()).expectNext(Collections.emptyList()).verifyComplete();
  }
}

