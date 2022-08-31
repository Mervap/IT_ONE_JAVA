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
      log.warn("Fail to create table: bad columns amount: expected  " + table.getColumnsAmount() + ", actual: " + table.getColumnInfos().size());
      return Mono.just(false);
    }

    if (isNotAsciiOnly(table.getName())) {
      log.warn("Fail to create table: bad table name '" + table.getName() + "'");
      return Mono.just(false);
    }

    for (var columnInfo : table.getColumnInfos()) {
      if (isNotAsciiOnly(columnInfo.getName())) {
        log.warn("Fail to create table: bad column name '" + columnInfo.getName() + "'");
        return Mono.just(false);
      }
    }

    if (table.getColumnInfos().stream().noneMatch(it -> it.getName().equalsIgnoreCase(table.getPrimaryKey()))) {
      log.warn("Fail to create table: bad primary key '" + table.getPrimaryKey() + "'");
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