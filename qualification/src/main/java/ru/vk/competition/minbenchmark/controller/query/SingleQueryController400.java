package ru.vk.competition.minbenchmark.controller.query;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import ru.vk.competition.minbenchmark.controller.ControllerWithCounter;
import ru.vk.competition.minbenchmark.entity.SingleQuery;
import ru.vk.competition.minbenchmark.service.SingleQueryService;

@Slf4j
@RestController
@RequestMapping("/api/single-query")
@RequiredArgsConstructor
public class SingleQueryController400 extends ControllerWithCounter {

  private final SingleQueryService queryService;

  @PostMapping("/add-new-query")
  public Mono<ResponseEntity<Void>> addNewQuery(@RequestBody SingleQuery query) {
    var id = nextId();
    log.info(withId(id, "Add single query: " + query.toString()));
    return toHttpStatus(queryService.addNewQuery(query).publishOn(Schedulers.boundedElastic())).map(it -> {
      log.info(withId(id, "Add single query result: " + it.getStatusCodeValue()));
      return it;
    });
  }

  protected Mono<ResponseEntity<Void>> toHttpStatus(Mono<Boolean> res) {
    return toHttpStatus(res, HttpStatus.CREATED, HttpStatus.BAD_REQUEST);
  }
}