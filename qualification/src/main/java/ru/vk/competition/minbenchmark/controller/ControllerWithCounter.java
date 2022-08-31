package ru.vk.competition.minbenchmark.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicInteger;

public class ControllerWithCounter {
  private final AtomicInteger counter = new AtomicInteger();

  protected int nextId() {
    return counter.getAndIncrement();
  }

  protected String withId(int id, String s) {
    return id + ") " + s;
  }


  protected Mono<ResponseEntity<Void>> toHttpStatus(Mono<Boolean> res, HttpStatus ok, HttpStatus error) {
    return res.map(isOk -> {
      if (isOk) {
        return new ResponseEntity<>(ok);
      } else {
        throw new ResponseStatusException(error, "Bad query");
      }
    });
  }
}
