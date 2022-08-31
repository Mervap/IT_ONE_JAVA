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
import ru.vk.competition.minbenchmark.entity.ColumnInfoWithSize;
import ru.vk.competition.minbenchmark.entity.Report;
import ru.vk.competition.minbenchmark.entity.ReportTable;
import ru.vk.competition.minbenchmark.service.ReportService;

import java.util.Arrays;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.BDDMockito.given;
import static ru.vk.competition.minbenchmark.util.TableSchemaUtil.CHARACTER_TYPE;
import static ru.vk.competition.minbenchmark.util.TableSchemaUtil.INTEGER_TYPE;

@ExtendWith(SpringExtension.class)
@WebFluxTest(controllers = ReportController.class)
class ReportControllerTest {
  @Autowired
  private WebTestClient webClient;

  @Autowired
  private ObjectMapper objectMapper;

  @MockBean
  private ReportService reportService;


  @Test
  void createReport() {
    given(reportService.createReport(any())).willReturn(Mono.just(true));
    webClient.post()
      .uri("/api/report/create-report")
      .contentType(MediaType.APPLICATION_JSON)
      .accept(MediaType.APPLICATION_JSON)
      .body(Mono.just(REPORT), Report.class)
      .exchange().expectStatus()
      .isCreated();
  }

  @Test
  void createReportFail() {
    given(reportService.createReport(any())).willReturn(Mono.just(false));
    webClient.post()
      .uri("/api/report/create-report")
      .contentType(MediaType.APPLICATION_JSON)
      .accept(MediaType.APPLICATION_JSON)
      .body(Mono.just(REPORT), Report.class)
      .exchange().expectStatus()
      .isEqualTo(HttpStatus.NOT_ACCEPTABLE);
  }

  @Test
  void getReport() throws JsonProcessingException {
    var json = objectMapper.writeValueAsString(REPORT1);
    given(reportService.getReportById(anyInt())).willReturn(Mono.just(REPORT1));
    webClient.get()
      .uri("/api/report/get-report-by-id/1")
      .exchange().expectAll(
        spec -> spec.expectStatus().isCreated(),
        spec -> spec.expectBody().json(json)
      );
  }

  @Test
  void getReportFail() {
    given(reportService.getReportById(anyInt())).willReturn(Mono.empty());
    webClient.get()
      .uri("/api/report/get-report-by-id/1")
      .exchange().expectAll(
        spec -> spec.expectStatus().isEqualTo(HttpStatus.NOT_ACCEPTABLE),
        spec -> spec.expectBody().isEmpty()
      );
  }

  @Test
  void getReportBadId() {
    webClient.get()
      .uri("/api/report/get-report-by-id/eheheheh")
      .exchange().expectAll(
        spec -> spec.expectStatus().isEqualTo(HttpStatus.NOT_ACCEPTABLE)
      );
  }

  private final static ReportTable<ColumnInfo> TABLE1 = new ReportTable<>(
    "Test",
    Arrays.asList(
      new ColumnInfo("ID", INTEGER_TYPE),
      new ColumnInfo("DATA", CHARACTER_TYPE)
    )
  );

  private final static ReportTable<ColumnInfo> TABLE2 = new ReportTable<>(
    "Test2",
    Arrays.asList(
      new ColumnInfo("NAME", CHARACTER_TYPE),
      new ColumnInfo("DESC", CHARACTER_TYPE)
    )
  );

  private final static Report<ColumnInfo> REPORT = new Report<>(1, 2, Arrays.asList(TABLE1, TABLE2));

  private final static ReportTable<ColumnInfoWithSize> TABLE3 = new ReportTable<>(
    "Test",
    Arrays.asList(
      new ColumnInfoWithSize("ID", INTEGER_TYPE, "1"),
      new ColumnInfoWithSize("DATA", CHARACTER_TYPE, "2")
    )
  );

  private final static ReportTable<ColumnInfoWithSize> TABLE4 = new ReportTable<>(
    "Test2",
    Arrays.asList(
      new ColumnInfoWithSize("NAME", CHARACTER_TYPE, "3"),
      new ColumnInfoWithSize("DESC", CHARACTER_TYPE, "3")
    )
  );

  private final static Report<ColumnInfoWithSize> REPORT1 = new Report<>(1, 2, Arrays.asList(TABLE3, TABLE4));
}