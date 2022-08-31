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

@Slf4j
@RestController
@RequestMapping("/api/single-query")
@RequiredArgsConstructor
public class SingleQueryController500 extends ControllerWithCounter {

  private final SingleQueryService queryService;

  @GetMapping("/get-single-query-by-id/{id}")
  public Mono<SingleQuery> getQueryById(@PathVariable Integer id) {
    var queryId = nextId();
    log.info(withId(queryId, "Get single query by id: " + id));
    return queryService.getQueryById(id)
      .switchIfEmpty(Mono.error(new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Bad query")))
      .publishOn(Schedulers.boundedElastic()).map(it -> {
        log.info(withId(queryId, "Get single query by id result: " + it.toString()));
        return it;
      });
  }

  @ExceptionHandler(ServerWebInputException.class)
  ResponseEntity<Void> badQuery(ServerWebInputException ex) {
    log.info("Handle bad single query500: " + ex.getMethodParameter() + " " + ex);
    throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, ex.getReason(), ex.getCause());
  }

  protected Mono<ResponseEntity<Void>> toHttpStatus(Mono<Boolean> res) {
    return toHttpStatus(res, HttpStatus.CREATED, HttpStatus.BAD_REQUEST);
  }
}