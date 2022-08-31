package ru.vk.competition.minbenchmark.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebExceptionHandler;
import reactor.core.publisher.Mono;

@Slf4j
@Component
@Order(-2)
class RestWebExceptionHandler implements WebExceptionHandler {

  @Override
  public Mono<Void> handle(ServerWebExchange exchange, Throwable ex) {
    if (ex instanceof ResponseStatusException && ((ResponseStatusException) ex).getStatus().value() == 404) {
      var path = exchange.getAttribute("org.springframework.web.reactive.HandlerMapping.pathWithinHandlerMapping");
      if (path != null) {
        log.info("Incoming request: " + path);
      }
    }
    return Mono.error(ex);
  }
}