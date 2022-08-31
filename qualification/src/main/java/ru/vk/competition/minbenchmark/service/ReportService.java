package ru.vk.competition.minbenchmark.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import ru.vk.competition.minbenchmark.entity.ColumnInfo;
import ru.vk.competition.minbenchmark.entity.ColumnInfoWithSize;
import ru.vk.competition.minbenchmark.entity.Report;
import ru.vk.competition.minbenchmark.repository.RawRepository;

@Service
@Slf4j
@RequiredArgsConstructor
public class ReportService {

  private final RawRepository repo;

  public Mono<Report<ColumnInfoWithSize>> getReportById(int id) {
    return Mono.fromCallable(() -> repo.getReportById(id)).flatMap(Mono::justOrEmpty);
  }

  public Mono<Boolean> createReport(Report<ColumnInfo> report) {
    if (report.getTableAmount() != report.getTables().size()) {
      log.info("Fail to create report: bad tables amount: expected  " + report.getTableAmount() + ", actual: " + report.getTables().size());
      return Mono.just(false);
    }

    return Mono.fromCallable(() -> {
      for (var table : report.getTables()) {
        String tableName = table.getName();
        var info = repo.getTableInfo(tableName);
        if (info.isEmpty()) {
          log.info("Fail to create report: bad table name '" + tableName + "'");
          return false;
        }

        var thisColumnsOptional = repo.mapColumnTypes(table.getColumns());
        if (thisColumnsOptional.isEmpty()) {
          log.info("Fail to map columns '" + tableName + "'");
          return false;
        }

        var thisColumns = thisColumnsOptional.get();
        var realColumns = info.get().getColumnInfos();
        for (var column : thisColumns) {
          String columnName = column.getName();
          String columnType = column.getType();

          boolean founded = false;
          for (var realColumn : realColumns) {
            if (realColumn.getName().equals(columnName)) {
              if (!realColumn.getType().equals(columnType)) {
                log.info("Fail to create report: bad column type '" + columnType + "' (expected '" + realColumn.getType() + "')");
                return false;
              }
              founded = true;
              break;
            }
          }

          if (!founded) {
            log.info("Fail to create report: bad column name '" + columnName + "'");
            return false;
          }
        }
      }

      return repo.createReport(report);
    });
  }

  public void clear() {
    repo.clear();
  }
}