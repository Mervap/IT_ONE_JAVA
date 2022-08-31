package ru.vk.competition.minbenchmark.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import ru.vk.competition.minbenchmark.entity.DBTable;
import ru.vk.competition.minbenchmark.repository.RawRepository;

@Service
@Slf4j
@RequiredArgsConstructor
public class TableService {

  private final RawRepository repo;

  public Mono<DBTable> getTableByName(String name) {
    return Mono.fromCallable(() -> repo.getTableInfo(name)).flatMap(Mono::justOrEmpty);
  }

  public Mono<Boolean> createTable(DBTable table) {
    if (table.getColumnsAmount() != table.getColumnInfos().size()) {
      log.info("Bad columns amount");
      return Mono.just(false);
    }

    if (isNotAsciiOnly(table.getName())) {
      log.info("Bad table name");
      return Mono.just(false);
    }

    if (table.getColumnInfos().stream().anyMatch(it -> isNotAsciiOnly(it.getName()))) {
      log.info("Bad column name");
      return Mono.just(false);
    }

    if (table.getColumnInfos().stream().noneMatch(it -> it.getName().equalsIgnoreCase(table.getPrimaryKey()))) {
      log.info("Bad primary key");
      return Mono.just(false);
    }

    return Mono.fromCallable(() -> repo.createTable(table));
  }

  public Mono<Boolean> dropTable(String name) {
    return Mono.fromCallable(() -> repo.dropTable(name));
  }

  private boolean isNotAsciiOnly(String s) {
    return s.chars().anyMatch(c -> c > 127);
  }
}