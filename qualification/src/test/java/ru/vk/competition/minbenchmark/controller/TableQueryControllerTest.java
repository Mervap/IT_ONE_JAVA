package ru.vk.competition.minbenchmark.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;
import ru.vk.competition.minbenchmark.controller.query.TableQueryController;
import ru.vk.competition.minbenchmark.controller.query.TableQueryController500;
import ru.vk.competition.minbenchmark.entity.TableQuery;
import ru.vk.competition.minbenchmark.service.TableQueryService;

import java.util.Arrays;
import java.util.Collections;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.BDDMockito.given;

@ExtendWith(SpringExtension.class)
@WebFluxTest(controllers = {TableQueryController.class, TableQueryController500.class})
class TableQueryControllerTest {
  @Autowired
  private WebTestClient webClient;

  @Autowired
  private ObjectMapper objectMapper;

  @MockBean
  private TableQueryService queryService;

  @Test
  void addNewOk() {
    given(queryService.addNewTableQuery(any())).willReturn(Mono.just(true));
    addOrUpdateQuery(webClient.post(), "/api/table-query/add-new-query-to-table")
      .expectStatus().isCreated();
  }

  @Test
  void addNewFail() {
    given(queryService.addNewTableQuery(any())).willReturn(Mono.just(false));
    addOrUpdateQuery(webClient.post(), "/api/table-query/add-new-query-to-table")
      .expectStatus().isEqualTo(HttpStatus.NOT_ACCEPTABLE);
  }

  @Test
  void addNewBadQueryId() {
    addOrUpdateBadQuery(webClient.post(), "/api/table-query/add-new-query-to-table")
      .expectStatus().isEqualTo(HttpStatus.NOT_ACCEPTABLE);
  }

  @Test
  void updateOk() {
    given(queryService.updateTableQuery(any())).willReturn(Mono.just(true));
    addOrUpdateQuery(webClient.put(), "/api/table-query/modify-query-in-table")
      .expectStatus().isOk();
  }

  @Test
  void updateFail() {
    given(queryService.updateTableQuery(any())).willReturn(Mono.just(false));
    addOrUpdateQuery(webClient.put(), "/api/table-query/modify-query-in-table")
      .expectStatus().isEqualTo(HttpStatus.NOT_ACCEPTABLE);
  }

  @Test
  void updateBadQueryId() {
    addOrUpdateBadQuery(webClient.put(), "/api/table-query/modify-query-in-table")
      .expectStatus().isEqualTo(HttpStatus.NOT_ACCEPTABLE);
  }

  @Test
  void deleteOk() {
    given(queryService.deleteTableQuery(anyInt())).willReturn(Mono.just(true));
    webClient.delete()
      .uri("/api/table-query/delete-table-query-by-id/1").exchange()
      .expectStatus().isAccepted();
  }

  @Test
  void deleteFail() {
    given(queryService.deleteTableQuery(anyInt())).willReturn(Mono.just(false));
    webClient.delete()
      .uri("/api/table-query/delete-table-query-by-id/1").exchange()
      .expectStatus().isEqualTo(HttpStatus.NOT_ACCEPTABLE);
  }

  @Test
  void deleteBadQueryId() {
    webClient.delete()
      .uri("/api/table-query/delete-table-query-by-id/azaza").exchange()
      .expectStatus().isEqualTo(HttpStatus.NOT_ACCEPTABLE);
  }

  @Test
  void executeOk() {
    given(queryService.executeTableQuery(anyInt())).willReturn(Mono.just(true));
    webClient.get()
      .uri("/api/table-query/execute-table-query-by-id/1").exchange()
      .expectStatus().isCreated();
  }

  @Test
  void executeFail() {
    given(queryService.executeTableQuery(anyInt())).willReturn(Mono.just(false));
    webClient.get()
      .uri("/api/table-query/execute-table-query-by-id/1").exchange()
      .expectStatus().isEqualTo(HttpStatus.NOT_ACCEPTABLE);
  }

  @Test
  void executeBadQueryId() {
    webClient.get()
      .uri("/api/table-query/execute-table-query-by-id/azaza").exchange()
      .expectStatus().isEqualTo(HttpStatus.NOT_ACCEPTABLE);
  }

  @Test
  void getAllOk() throws JsonProcessingException {
    var query1 = new TableQuery(1, "Test", "select * from Test");
    var query2 = new TableQuery(2, "Test", "select id, name from Test");
    var queryList = Arrays.asList(query1, query2);
    var queriesJson = objectMapper.writeValueAsString(queryList);

    given(queryService.getTableQueries(anyString())).willReturn(Mono.just(queryList));
    webClient.get()
      .uri("/api/table-query/get-all-queries-by-table-name/Test").exchange()
      .expectAll(
        spec -> spec.expectStatus().isOk(),
        spec -> spec.expectBody().json(queriesJson)
      );
  }

  @Test
  void getAllFail() {
    given(queryService.getTableQueries(anyString())).willReturn(Mono.empty());
    webClient.get()
      .uri("/api/table-query/get-all-queries-by-table-name/lol1+1").exchange()
      .expectAll(
        spec -> spec.expectStatus().isOk(),
        spec -> spec.expectBody().isEmpty()
      );
  }

  @Test
  void getByIdOk() throws JsonProcessingException {
    var query1 = new TableQuery(1, "Test", "select * from Test");
    var queryJson = objectMapper.writeValueAsString(query1);

    given(queryService.getQueryById(anyInt())).willReturn(Mono.just(query1));
    webClient.get()
      .uri("/api/table-query/get-table-query-by-id/1").exchange()
      .expectAll(
        spec -> spec.expectStatus().isOk(),
        spec -> spec.expectBody().json(queryJson)
      );
  }

  @Test
  void getByIdFail() {
    given(queryService.getQueryById(anyInt())).willReturn(Mono.empty());
    webClient.get()
      .uri("/api/table-query/get-table-query-by-id/1").exchange()
      .expectStatus().isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
  }

  @Test
  void getByIdBadId() {
    webClient.get()
      .uri("/api/table-query/get-table-query-by-id/azaza").exchange()
      .expectStatus().isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
  }

  @Test
  void getAllTableQueries() throws JsonProcessingException {
    var query1 = new TableQuery(1, "Test", "select * from Test");
    var query2 = new TableQuery(2, "Test", "select id, name from Test");
    var queryList = Arrays.asList(query1, query2);
    var queriesJson = objectMapper.writeValueAsString(queryList);

    given(queryService.getAllTableQueries()).willReturn(Mono.just(queryList));
    webClient.get()
      .uri("/api/table-query/get-all-table-queries").exchange()
      .expectAll(
        spec -> spec.expectStatus().isOk(),
        spec -> spec.expectBody().json(queriesJson)
      );
  }

  @Test
  void getAllTableQueriesEmpty() {
    given(queryService.getAllTableQueries()).willReturn(Mono.just(Collections.emptyList()));
    webClient.get()
      .uri("/api/table-query/get-all-table-queries").exchange()
      .expectAll(
        spec -> spec.expectStatus().isOk(),
        spec -> spec.expectBody().json("[]")
      );
  }

  private WebTestClient.ResponseSpec addOrUpdateBadQuery(WebTestClient.RequestBodyUriSpec spec, String uri) {
    return spec.uri(uri)
      .contentType(MediaType.APPLICATION_JSON)
      .accept(MediaType.APPLICATION_JSON)
      .bodyValue("{ \"queryId\": abs, \"tableName\": \"Artists\", \"query\": \"hello\" }")
      .exchange();
  }

  private WebTestClient.ResponseSpec addOrUpdateQuery(WebTestClient.RequestBodyUriSpec spec, String uri) {
    return spec.uri(uri)
      .contentType(MediaType.APPLICATION_JSON)
      .accept(MediaType.APPLICATION_JSON)
      .body(Mono.just(new TableQuery(1, "Vasya", "Hello")), TableQuery.class)
      .exchange();
  }
}