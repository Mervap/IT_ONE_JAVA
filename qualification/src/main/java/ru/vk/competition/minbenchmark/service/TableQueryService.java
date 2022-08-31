package ru.vk.competition.minbenchmark.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import ru.vk.competition.minbenchmark.entity.TableQuery;
import ru.vk.competition.minbenchmark.repository.RawRepository;

import java.util.List;

@Service
@Slf4j
@RequiredArgsConstructor
public class TableQueryService {

  private final RawRepository repo;

  public Mono<Boolean> addNewTableQuery(TableQuery query) {
    return Mono.fromCallable(() -> repo.newTableQuery(query));
  }

  public Mono<Boolean> updateTableQuery(TableQuery query) {
    return Mono.fromCallable(() -> repo.updateTableQuery(query));
  }

  public Mono<Boolean> deleteTableQuery(int id) {
    return Mono.fromCallable(() -> repo.deleteTableQuery(id));
  }

  public Mono<Boolean> executeTableQuery(int id) {
    return Mono.fromCallable(() -> repo.executeTableQuery(id));
  }

  public Mono<List<TableQuery>> getTableQueries(String name) {
    return Mono.fromCallable(() -> repo.getTableQueries(name)).flatMap(Mono::justOrEmpty);
  }

  public Mono<TableQuery> getQueryById(int id) {
    return Mono.fromCallable(() -> repo.getQueryById(id)).flatMap(Mono::justOrEmpty);
  }

  public Mono<List<TableQuery>> getAllTableQueries() {
    return Mono.fromCallable(() -> repo.getAllTableQueries().stream().toList());
  }

  public void clear() {
    repo.clear();
  }
}