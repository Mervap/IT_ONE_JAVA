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
    log.info(withId(id, "Get table: name = " + name));
    return tableService.getTableByName(name).map(it -> {
      log.info(withId(id, "Get table succeeded: " + it.toString()));
      return it;
    }).publishOn(Schedulers.boundedElastic());
  }

  @PostMapping("/create-table")
  public Mono<ResponseEntity<Void>> createTable(@RequestBody DBTable table) {
    var id = nextId();
    log.info(withId(id, "Create table: name = " + table.toString()));
    return toHttpStatus(tableService.createTable(table).publishOn(Schedulers.boundedElastic())).map(it -> {
      if (it.getStatusCodeValue() == 201) {
        log.info(withId(id, "Create table " + table.getName()));
      }
      log.info(withId(id, "Create table result: " + it.getStatusCodeValue()));
      return it;
    });
  }

  @DeleteMapping("/drop-table/{name}")
  public Mono<ResponseEntity<Void>> dropTable(@PathVariable String name) {
    var id = nextId();
    log.info(withId(id, "Drop table: name = " + name));
    return toHttpStatus(tableService.dropTable(name).publishOn(Schedulers.boundedElastic())).map(it -> {
      if (it.getStatusCodeValue() == 201) {
        log.info(withId(id, "Drop table " + name));
      }
      log.info(withId(id, "Drop table result: " + it.getStatusCodeValue()));
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