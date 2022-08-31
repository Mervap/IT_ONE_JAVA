package ru.vk.competition.minbenchmark.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.server.ServerWebInputException;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import ru.vk.competition.minbenchmark.entity.ColumnInfo;
import ru.vk.competition.minbenchmark.entity.ColumnInfoWithSize;
import ru.vk.competition.minbenchmark.entity.Report;
import ru.vk.competition.minbenchmark.service.ReportService;

@Slf4j
@RestController
@RequestMapping("/api/report")
@RequiredArgsConstructor
public class ReportController extends ControllerWithCounter {

  private final ReportService reportService;

  @GetMapping("/get-report-by-id/{id}")
  public Mono<ResponseEntity<Report<ColumnInfoWithSize>>> getReportById(@PathVariable Integer id) {
    var queryId = nextId();
    log.info(withId(queryId, "Get report: " + id));
    return reportService.getReportById(id).map(it -> {
        log.info(withId(queryId, "Get report succeeded: " + it.toString()));
        return it;
      }).publishOn(Schedulers.boundedElastic())
      .map(body -> new ResponseEntity<>(body, HttpStatus.CREATED))
      .defaultIfEmpty(new ResponseEntity<>(HttpStatus.NOT_ACCEPTABLE));
  }

  @PostMapping("/create-report")
  public Mono<ResponseEntity<Void>> createReport(@RequestBody Report<ColumnInfo> report) {
    var id = nextId();
    log.info(withId(id, "Create report " + report));
    return toHttpStatus(reportService.createReport(report).publishOn(Schedulers.boundedElastic())).map(it -> {
      log.info(withId(id, "Create report result: " + it.getStatusCodeValue()));
      return it;
    });
  }

  @ExceptionHandler(ServerWebInputException.class)
  ResponseEntity<Void> badQuery(ServerWebInputException ex) {
    throw new ResponseStatusException(HttpStatus.NOT_ACCEPTABLE, ex.getReason(), ex.getCause());
  }

  protected Mono<ResponseEntity<Void>> toHttpStatus(Mono<Boolean> res) {
    return toHttpStatus(res, HttpStatus.CREATED, HttpStatus.NOT_ACCEPTABLE);
  }
}