package ru.vk.competition.minbenchmark.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import ru.vk.competition.minbenchmark.entity.DBTable;
import ru.vk.competition.minbenchmark.service.TableService;

@Slf4j
@RestController
@RequestMapping("/api/table")
@RequiredArgsConstructor
public class TableController extends ControllerWithCounter {

  private final TableService tableService;
  @GetMapping("/get-table-by-name/{name}")
  public Mono<DBTable> getTableByName(@PathVariable String name) {
    var id = nextId();
    log.warn(withId(id,  "Get table: name = " + name));
    return tableService.getTableByName(name).map(it -> {
      log.warn(withId(id, "Get table succeeded: " + it.toString()));
      return it;
    }).publishOn(Schedulers.boundedElastic());
  }

  @PostMapping("/create-table")
  public Mono<ResponseEntity<Void>> createTable(@RequestBody DBTable table) {
    var id = nextId();
    log.warn(withId(id, "Create table: name = " + table.toString()));
    return toHttpStatus(tableService.createTable(table).publishOn(Schedulers.boundedElastic())).map(it -> {
      log.warn(withId(id, "Create table result: " + it.getStatusCodeValue()));
      return it;
    });
  }

  @DeleteMapping("/drop-table/{name}")
  public Mono<ResponseEntity<Void>> dropTable(@PathVariable String name) {
    var id = nextId();
    log.warn(withId(id, "Drop table: name = " + name));
    return toHttpStatus(tableService.dropTable(name).publishOn(Schedulers.boundedElastic())).map(it -> {
      log.warn(withId(id, "Drop table result: " + it.getStatusCodeValue()));
      return it;
    });
  }
}