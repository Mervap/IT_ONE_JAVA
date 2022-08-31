package ru.vk.competition.minbenchmark.controller.query;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import ru.vk.competition.minbenchmark.controller.ControllerWithCounter;
import ru.vk.competition.minbenchmark.entity.TableQuery;
import ru.vk.competition.minbenchmark.service.TableQueryService;

import java.util.List;

@Slf4j
@RestController
@RequestMapping("/api/table-query")
@RequiredArgsConstructor
public class TableQueryController extends ControllerWithCounter {

  private final TableQueryService queryService;

  @PostMapping("/add-new-query-to-table")
  public Mono<ResponseEntity<Void>> addNewTableQuery(@RequestBody TableQuery query) {
    var id = nextId();
    log.info(withId(id, "Add table query: " + query.toString()));
    return toHttpStatus(queryService.addNewTableQuery(query).publishOn(Schedulers.boundedElastic())).map(it -> {
      log.info(withId(id, "Add table query result: " + it.getStatusCodeValue()));
      return it;
    });
  }

  @PutMapping("/modify-query-in-table")
  public Mono<ResponseEntity<Void>> updateTableQuery(@RequestBody TableQuery query) {
    var id = nextId();
    log.info(withId(id, "Update table query: " + query.toString()));
    return toHttpStatus(queryService.updateTableQuery(query).publishOn(Schedulers.boundedElastic()), HttpStatus.OK).map(it -> {
      log.info(withId(id, "Update table query result: " + it.getStatusCodeValue()));
      return it;
    });
  }

  @DeleteMapping("/delete-table-query-by-id/{id}")
  public Mono<ResponseEntity<Void>> deleteTableQuery(@PathVariable Integer id) {
    var queryId = nextId();
    log.info(withId(queryId, "Delete table query: " + id));
    return toHttpStatus(queryService.deleteTableQuery(id).publishOn(Schedulers.boundedElastic()), HttpStatus.ACCEPTED).map(it -> {
      log.info(withId(queryId, "Delete table query result: " + it.getStatusCodeValue()));
      return it;
    });
  }

  @GetMapping("/execute-table-query-by-id/{id}")
  public Mono<ResponseEntity<Void>> executeTableQuery(@PathVariable Integer id) {
    var queryId = nextId();
    log.info(withId(queryId, "Execute table query: " + id));
    return toHttpStatus(queryService.executeTableQuery(id).publishOn(Schedulers.boundedElastic())).map(it -> {
      log.info(withId(queryId, "Execute table query result: " + it.getStatusCodeValue()));
      return it;
    });
  }

  @GetMapping("/get-all-queries-by-table-name/{name}")
  public Mono<List<TableQuery>> getTableQueries(@PathVariable String name) {
    var queryId = nextId();
    log.info(withId(queryId, "Get table query: " + name));
    return queryService.getTableQueries(name).publishOn(Schedulers.boundedElastic()).map(it -> {
      log.info(withId(queryId, "Get table query result: " + it.toString()));
      return it;
    });
  }

  @GetMapping("/get-all-table-queries")
  public Mono<List<TableQuery>> getAllTableQueries() {
    var queryId = nextId();
    log.info(withId(queryId, "Get all table queries"));
    return queryService.getAllTableQueries().publishOn(Schedulers.boundedElastic()).map(it -> {
      log.info(withId(queryId, "Get all table queries result: " + it.toString()));
      return it;
    });
  }
}