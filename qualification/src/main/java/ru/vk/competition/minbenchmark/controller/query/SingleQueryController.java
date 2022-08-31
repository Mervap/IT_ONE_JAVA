package ru.vk.competition.minbenchmark.controller.query;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.server.ServerWebInputException;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import ru.vk.competition.minbenchmark.controller.ControllerWithCounter;
import ru.vk.competition.minbenchmark.entity.SingleQuery;
import ru.vk.competition.minbenchmark.service.SingleQueryService;

import java.util.List;

@Slf4j
@RestController
@RequestMapping("/api/single-query")
@RequiredArgsConstructor
public class SingleQueryController extends ControllerWithCounter {

  private final SingleQueryService queryService;

  @PutMapping("/modify-single-query")
  public Mono<ResponseEntity<Void>> updateQuery(@RequestBody SingleQuery query) {
    var id = nextId();
    log.info(withId(id, "Update single query: " + query.toString()));
    return toHttpStatus(queryService.updateQuery(query).publishOn(Schedulers.boundedElastic()), HttpStatus.OK).map(it -> {
      log.info(withId(id, "Update single query result: " + it.getStatusCodeValue()));
      return it;
    });
  }

  @DeleteMapping("/delete-single-query-by-id/{id}")
  public Mono<ResponseEntity<Void>> deleteQuery(@PathVariable Integer id) {
    var queryId = nextId();
    log.info(withId(queryId, "Delete single query: " + id));
    return toHttpStatus(queryService.deleteQuery(id).publishOn(Schedulers.boundedElastic()), HttpStatus.ACCEPTED).map(it -> {
      log.info(withId(queryId, "Delete single query result: " + it.getStatusCodeValue()));
      return it;
    });
  }

  @GetMapping("/execute-single-query-by-id/{id}")
  public Mono<ResponseEntity<Void>> executeQuery(@PathVariable Integer id) {
    var queryId = nextId();
    log.info(withId(queryId, "Execute single query: " + id));
    return toHttpStatus(queryService.executeQuery(id).publishOn(Schedulers.boundedElastic()), HttpStatus.CREATED).map(it -> {
      log.info(withId(queryId, "Execute single query result: " + it.getStatusCodeValue()));
      return it;
    });
  }


  @GetMapping("/get-all-single-queries")
  public Mono<List<SingleQuery>> getAllQueries() {
    var queryId = nextId();
    log.info(withId(queryId, "Get all single queries"));
    return queryService.getAllQueries().publishOn(Schedulers.boundedElastic()).map(it -> {
      log.info(withId(queryId, "Get all single queries result: " + it.toString()));
      return it;
    });
  }


  @ExceptionHandler(ServerWebInputException.class)
  ResponseEntity<Void> badQuery(ServerWebInputException ex) {
    log.info("Handle bad single query: " + ex.getMethodParameter() + " " + ex);
    throw new ResponseStatusException(HttpStatus.NOT_ACCEPTABLE, ex.getReason(), ex.getCause());
  }

  protected Mono<ResponseEntity<Void>> toHttpStatus(Mono<Boolean> res, HttpStatus ok) {
    return toHttpStatus(res, ok, HttpStatus.NOT_ACCEPTABLE);
  }
}