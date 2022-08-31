package ru.vk.competition.minbenchmark.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import ru.vk.competition.minbenchmark.entity.DBTable;
import ru.vk.competition.minbenchmark.service.TableService;

import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@RestController
@RequestMapping("/api/table")
@RequiredArgsConstructor
public class TableController {

  private final TableService tableService;
  private final AtomicInteger counter = new AtomicInteger();

  @GetMapping("/get-table-by-name/{name}")
  public Mono<DBTable> getTableByName(@PathVariable String name) {
    var id = counter.getAndIncrement();
    log.info(id + ") Get table: name = " + name);
    return tableService.getTableByName(name).map(it -> {
      log.info(id + ") Get table succeeded: " + it.toString());
      return it;
    }).publishOn(Schedulers.boundedElastic());
  }

  @PostMapping("/create-table")
  public Mono<ResponseEntity<Void>> createTable(@RequestBody DBTable table) {
    var id = counter.getAndIncrement();
    log.info(id + ") Create table: name = " + table.toString());
    return toHttpStatus(tableService.createTable(table).publishOn(Schedulers.boundedElastic())).map(it -> {
      log.info(id + ") Create table result: " + it.getStatusCodeValue());
      return it;
    });
  }

  @DeleteMapping("/drop-table/{name}")
  public Mono<ResponseEntity<Void>> dropTable(@PathVariable String name) {
    var id = counter.getAndIncrement();
    log.info(id + ") Drop table: name = " + name);
    return toHttpStatus(tableService.dropTable(name).publishOn(Schedulers.boundedElastic())).map(it -> {
      log.info(id + ") Drop table result: " + it.getStatusCodeValue());
      return it;
    });
  }

  private Mono<ResponseEntity<Void>> toHttpStatus(Mono<Boolean> res) {
    return res.map(isOk -> {
      if (isOk) {
        return new ResponseEntity<>(HttpStatus.CREATED);
      } else {
        return new ResponseEntity<>(HttpStatus.NOT_ACCEPTABLE);
      }
    });
  }
}