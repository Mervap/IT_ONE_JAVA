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
import ru.vk.competition.minbenchmark.controller.query.*;
import ru.vk.competition.minbenchmark.entity.SingleQuery;
import ru.vk.competition.minbenchmark.entity.TableQuery;
import ru.vk.competition.minbenchmark.service.SingleQueryService;
import ru.vk.competition.minbenchmark.service.TableQueryService;

import java.util.Arrays;
import java.util.Collections;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.BDDMockito.given;

@ExtendWith(SpringExtension.class)
@WebFluxTest(controllers = { SingleQueryController.class, SingleQueryController400.class, SingleQueryController500.class })
class SingleQueryControllerTest {
  @Autowired
  private WebTestClient webClient;

  @Autowired
  private ObjectMapper objectMapper;

  @MockBean
  private SingleQueryService queryService;

  @Test
  void addNewOk() {
    given(queryService.addNewQuery(any())).willReturn(Mono.just(true));
    addOrUpdateQuery(webClient.post(), "/api/single-query/add-new-query")
      .expectStatus().isCreated();
  }

  @Test
  void addNewFail() {
    given(queryService.addNewQuery(any())).willReturn(Mono.just(false));
    addOrUpdateQuery(webClient.post(), "/api/single-query/add-new-query")
      .expectStatus().isEqualTo(HttpStatus.BAD_REQUEST);
  }

  @Test
  void addNewBadQueryId() {
    addOrUpdateBadQuery(webClient.post(), "/api/single-query/add-new-query")
      .expectStatus().isEqualTo(HttpStatus.BAD_REQUEST);
  }

  @Test
  void updateOk() {
    given(queryService.updateQuery(any())).willReturn(Mono.just(true));
    addOrUpdateQuery(webClient.put(), "/api/single-query/modify-single-query")
      .expectStatus().isOk();
  }

  @Test
  void updateFail() {
    given(queryService.updateQuery(any())).willReturn(Mono.just(false));
    addOrUpdateQuery(webClient.put(), "/api/single-query/modify-single-query")
      .expectStatus().isEqualTo(HttpStatus.NOT_ACCEPTABLE);
  }

  @Test
  void updateBadQueryId() {
    addOrUpdateBadQuery(webClient.put(), "/api/single-query/modify-single-query")
      .expectStatus().isEqualTo(HttpStatus.NOT_ACCEPTABLE);
  }

  @Test
  void deleteOk() {
    given(queryService.deleteQuery(anyInt())).willReturn(Mono.just(true));
    webClient.delete()
      .uri("/api/single-query/delete-single-query-by-id/1").exchange()
      .expectStatus().isAccepted();
  }

  @Test
  void deleteFail() {
    given(queryService.deleteQuery(anyInt())).willReturn(Mono.just(false));
    webClient.delete()
      .uri("/api/single-query/delete-single-query-by-id/1").exchange()
      .expectStatus().isEqualTo(HttpStatus.NOT_ACCEPTABLE);
  }

  @Test
  void deleteBadQueryId() {
    webClient.delete()
      .uri("/api/single-query/delete-single-query-by-id/azaza").exchange()
      .expectStatus().isEqualTo(HttpStatus.NOT_ACCEPTABLE);
  }

  @Test
  void executeOk() {
    given(queryService.executeQuery(anyInt())).willReturn(Mono.just(true));
    webClient.get()
      .uri("/api/single-query/execute-single-query-by-id/1").exchange()
      .expectStatus().isCreated();
  }

  @Test
  void executeFail() {
    given(queryService.executeQuery(anyInt())).willReturn(Mono.just(false));
    webClient.get()
      .uri("/api/single-query/execute-single-query-by-id/1").exchange()
      .expectStatus().isEqualTo(HttpStatus.NOT_ACCEPTABLE);
  }

  @Test
  void executeBadQueryId() {
    webClient.get()
      .uri("/api/single-query/execute-single-query-by-id/azaza").exchange()
      .expectStatus().isEqualTo(HttpStatus.NOT_ACCEPTABLE);
  }

  @Test
  void getByIdOk() throws JsonProcessingException {
    var query1 = new SingleQuery(1, "select * from Test");
    var queryJson = objectMapper.writeValueAsString(query1);

    given(queryService.getQueryById(anyInt())).willReturn(Mono.just(query1));
    webClient.get()
      .uri("/api/single-query/get-single-query-by-id/1").exchange()
      .expectAll(
        spec -> spec.expectStatus().isOk(),
        spec -> spec.expectBody().json(queryJson)
      );
  }

  @Test
  void getByIdFail() {
    given(queryService.getQueryById(anyInt())).willReturn(Mono.empty());
    webClient.get()
      .uri("/api/single-query/get-single-query-by-id/1").exchange()
      .expectStatus().isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
  }

  @Test
  void getByIdBadId() {
    webClient.get()
      .uri("/api/single-query/get-single-query-by-id/azaza").exchange()
      .expectStatus().isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
  }
  @Test
  void getAllQueries() throws JsonProcessingException {
    var query1 = new SingleQuery(1, "select * from Test");
    var query2 = new SingleQuery(2, "select id, name from Test");
    var queryList = Arrays.asList(query1, query2);
    var queriesJson = objectMapper.writeValueAsString(queryList);

    given(queryService.getAllQueries()).willReturn(Mono.just(queryList));
    webClient.get()
      .uri("/api/single-query/get-all-single-queries").exchange()
      .expectAll(
        spec -> spec.expectStatus().isOk(),
        spec -> spec.expectBody().json(queriesJson)
      );
  }

  @Test
  void getAllTableQueriesEmpty() {
    given(queryService.getAllQueries()).willReturn(Mono.just(Collections.emptyList()));
    webClient.get()
      .uri("/api/single-query/get-all-single-queries").exchange()
      .expectAll(
        spec -> spec.expectStatus().isOk(),
        spec -> spec.expectBody().json("[]")
      );
  }

  private WebTestClient.ResponseSpec addOrUpdateBadQuery(WebTestClient.RequestBodyUriSpec spec, String uri) {
    return spec.uri(uri)
      .contentType(MediaType.APPLICATION_JSON)
      .accept(MediaType.APPLICATION_JSON)
      .bodyValue("{ \"queryId\": abs, \"query\": \"hello\" }")
      .exchange();
  }

  private WebTestClient.ResponseSpec addOrUpdateQuery(WebTestClient.RequestBodyUriSpec spec, String uri) {
    return spec.uri(uri)
      .contentType(MediaType.APPLICATION_JSON)
      .accept(MediaType.APPLICATION_JSON)
      .body(Mono.just(new SingleQuery(1, "select * from Test")), SingleQuery.class)
      .exchange();
  }
}