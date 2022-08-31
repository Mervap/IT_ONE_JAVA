package ru.vk.competition.minbenchmark.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import ru.vk.competition.minbenchmark.entity.SingleQuery;
import ru.vk.competition.minbenchmark.repository.RawRepository;

import java.util.List;

@Service
@Slf4j
@RequiredArgsConstructor
public class SingleQueryService {

  private final RawRepository repo;

  public Mono<Boolean> addNewQuery(SingleQuery query) {
    return Mono.fromCallable(() -> repo.newSingleQuery(query));
  }

  public Mono<Boolean> updateQuery(SingleQuery query) {
    return Mono.fromCallable(() -> repo.updateSingleQuery(query));
  }

  public Mono<Boolean> deleteQuery(int id) {
    return Mono.fromCallable(() -> repo.deleteSingleQuery(id));
  }

  public Mono<Boolean> executeQuery(int id) {
    return Mono.fromCallable(() -> repo.executeSingleQuery(id));
  }

  public Mono<SingleQuery> getQueryById(int id) {
    return Mono.fromCallable(() -> repo.getSingleQueryById(id)).flatMap(Mono::justOrEmpty);
  }

  public Mono<List<SingleQuery>> getAllQueries() {
    return Mono.fromCallable(() -> repo.getAllSingleQueries().stream().toList());
  }

  public void clear() {
    repo.clear();
  }
}