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
import ru.vk.competition.minbenchmark.entity.ColumnInfo;
import ru.vk.competition.minbenchmark.entity.DBTable;
import ru.vk.competition.minbenchmark.service.TableService;

import java.util.Arrays;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static ru.vk.competition.minbenchmark.util.TableSchemaUtil.CHARACTER_TYPE;
import static ru.vk.competition.minbenchmark.util.TableSchemaUtil.INTEGER_TYPE;

@ExtendWith(SpringExtension.class)
@WebFluxTest(controllers = TableController.class)
class TableControllerTest {
  @Autowired
  private WebTestClient webClient;

  @Autowired
  private ObjectMapper objectMapper;

  @MockBean
  private TableService tableService;

  @Test
  void ifCreatedReturn201() {
    given(tableService.createTable(any())).willReturn(Mono.just(true));
    webClient.post()
      .uri("/api/table/create-table")
      .contentType(MediaType.APPLICATION_JSON)
      .accept(MediaType.APPLICATION_JSON)
      .body(Mono.just(TABLE), DBTable.class)
      .exchange().expectStatus()
      .isCreated();
  }

  @Test
  void ifNotCreatedReturn406() {
    given(tableService.createTable(any())).willReturn(Mono.just(false));
    webClient.post()
      .uri("/api/table/create-table")
      .contentType(MediaType.APPLICATION_JSON)
      .accept(MediaType.APPLICATION_JSON)
      .body(Mono.just(TABLE), DBTable.class)
      .exchange().expectStatus()
      .isEqualTo(HttpStatus.NOT_ACCEPTABLE);
  }

  @Test
  void ifExistsReturnTable() throws JsonProcessingException {
    var tableJson = objectMapper.writeValueAsString(TABLE);
    given(tableService.getTableByName(TABLE.getName())).willReturn(Mono.just(TABLE));
    webClient.get()
      .uri("/api/table/get-table-by-name/" + TABLE.getName())
      .exchange()
      .expectAll(
        response -> response.expectStatus().isOk(),
        response -> response.expectBody().json(tableJson)
      );
  }

  @Test
  void ifNotExistsReturnEmpty() {
    given(tableService.getTableByName(TABLE.getName())).willReturn(Mono.empty());
    webClient.get()
      .uri("/api/table/get-table-by-name/" + TABLE.getName())
      .exchange()
      .expectAll(
        response -> response.expectStatus().isOk(),
        response -> response.expectBody().isEmpty()
      );
  }

  @Test
  void ifDeleteReturn201() {
    given(tableService.dropTable(TABLE.getName())).willReturn(Mono.just(true));
    webClient.delete()
      .uri("/api/table/drop-table/" + TABLE.getName())
      .exchange().expectStatus()
      .isCreated();
  }

  @Test
  void ifDeleteNotExistsReturn406() {
    given(tableService.dropTable(TABLE.getName())).willReturn(Mono.just(false));
    webClient.delete()
      .uri("/api/table/drop-table/" + TABLE.getName())
      .exchange().expectStatus()
      .isEqualTo(HttpStatus.NOT_ACCEPTABLE);
  }

  private final static DBTable TABLE = new DBTable(
    "Test",
    2, "ID",
    Arrays.asList(
      new ColumnInfo("ID", INTEGER_TYPE),
      new ColumnInfo("DATA", CHARACTER_TYPE)
    )
  );
}