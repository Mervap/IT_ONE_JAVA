package ru.vk.competition.minchecker.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.vk.competition.minchecker.entity.*;
import ru.vk.competition.minchecker.utils.ApiRequests;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

@Service
@Slf4j
@RequiredArgsConstructor
public class StartService {

  private final AtomicInteger counter = new AtomicInteger();

  public void onStartMission() {
    var cnt = counter.incrementAndGet();
    var proxy = new Proxy(cnt, counter::get);
    try {
      proxy.createTable(fakeTable("report"), 201, false);
      proxy.createTable(fakeTable("reports"), 201, false);
      for (int i = 1; i <= 6; i += 1) {
        proxy.getTableByName(fakeTableNormal("report"), false);
      }
      // Table: 22; Single: 20; Query: 20; Report: 30

      TableQuery tableQuery = new TableQuery(2, "report", "insert into Report (id, description, data, other, image) values (1, 'P', CURRENT_TIMESTAMP(), null, null)");
      proxy.addTableQuery(tableQuery, 201, false);
      proxy.executeTableQuery(tableQuery.getId(), 201, false);
      // Table: 22; Single: 20; Query: 18; Report: 30

      proxy.createReport(new Report<>(1, 2, Arrays.asList(
        reportTable("report"),
        reportTable("reports")
      )), 201, false);

      for (int i = 1; i <= 29; i += 1) {
        proxy.getReportStat(new Report<>(1, 2, Arrays.asList(
          reportTable("report", new String[]{"1", "1", "1", "0", "0"}),
          reportTable("reports", new String[]{"0", "0", "0", "0", "0"})
        )), 201, false);
      }
      // Table: 22; Single: 20; Query: 18; Report: 0

      SingleQuery singleQuery = new SingleQuery(1, "create table single_query;");
      proxy.addSingleQuery(singleQuery, 201, false);
      // Table: 22; Single: 19; Query: 18; Report: 0

      proxy.dropTable("single_query", 406, false);
      proxy.dropTable("table_query", 406, false);
      proxy.dropTable("table_queries", 406, false);
      // Table: 19; Single: 19; Query: 18; Report: 0

      proxy.executeSingleQuery(1, 201, false);
      proxy.dropTable("single_query", 201, false);
      // Table: 18; Single: 18; Query: 18; Report: 0

      var isTableOk = false;
      var isTableModifyOk = false;
      var isTableAddOk = false;

      var dropIsOk = false;
      var isSingleModifyOk = false;
      var isSingleAddOk = false;
      for (int i = 1; i <= 18; i += 1) {
        if (!isTableOk) {
          isTableOk = proxy.getTableQueryById(tableQuery, 200, false);
        }
        else if (!isTableModifyOk) {
          var badQuery = new TableQuery(2, "report", "A".repeat(200));
          isTableModifyOk = proxy.modifyTableQuery(badQuery, 406, false);
        }
        else if (!isTableAddOk) {
          var badQuery = new TableQuery(3, "report", "A".repeat(200));
          isTableAddOk = proxy.addTableQuery(badQuery, 406, false);
        }
        if (!dropIsOk) {
          dropIsOk = proxy.executeSingleQuery(1, 201, false);
          dropIsOk &= proxy.dropTable("single_query", 201, false);
        }
        else {
          proxy.getTableByName(fakeTableNormal("report"), false);
          if (!isSingleModifyOk) {
            var badQuery = new SingleQuery(1, "A".repeat(200));
            isSingleModifyOk = proxy.modifySingleQuery(badQuery, 406, false);
          }
          else if (!isSingleAddOk) {
            var badQuery = new SingleQuery(2, "A".repeat(200));
            isSingleAddOk = proxy.addSingleQuery(badQuery, 400, false);
          }
        }
      }
      // Table: 0; Single: 0; Query: 0; Report: 0

      System.out.println(cnt + ": " + proxy.getStat());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private DBTable fakeTable(String name) {
    return new DBTable(
      name, 5, "ID", Arrays.asList(
        new ColumnInfo("id", "tId"),
        new ColumnInfo("description", "national CHARACTER LaRgE OBJECT"),
        new ColumnInfo("data", "DateTIME2"),
        new ColumnInfo("other", "OtheR"),
        new ColumnInfo("image", "ImagE")
      )
    );
  }

  private DBTable fakeTableNormal(String name) {
    return new DBTable(
      name, 5, "id", Arrays.asList(
        new ColumnInfo("ID", "CHARACTER VARYING"),
        new ColumnInfo("DESCRIPTION", "CHARACTER LARGE OBJECT"),
        new ColumnInfo("DATA", "TIMESTAMP"),
        new ColumnInfo("OTHER", "JAVA_OBJECT"),
        new ColumnInfo("IMAGE", "BINARY LARGE OBJECT")
      )
    );
  }

  private ReportTable<ColumnInfo> reportTable(String tableName) {
    return new ReportTable<>(tableName, Arrays.asList(
      new ColumnInfo("id", "tId"),
      new ColumnInfo("description", "national CHARACTER LaRgE OBJECT"),
      new ColumnInfo("data", "DateTIME2"),
      new ColumnInfo("other", "OtheR"),
      new ColumnInfo("image", "ImagE")
    ));
  }

  private ReportTable<ColumnInfoWithSize> reportTable(String tableName, String[] sizes) {
    return new ReportTable<>(tableName, Arrays.asList(
      new ColumnInfoWithSize("id", "tId", sizes[0]),
      new ColumnInfoWithSize("description", "national CHARACTER LaRgE OBJECT", sizes[1]),
      new ColumnInfoWithSize("data", "DateTIME2", sizes[2]),
      new ColumnInfoWithSize("other", "OtheR", sizes[3]),
      new ColumnInfoWithSize("image", "ImagE", sizes[4])
    ));
  }
}

@RequiredArgsConstructor
class Proxy {
  private final int iteration;
  private final Supplier<Integer> currentIteration;

  private Integer tableStat = 0;
  private Integer singleQueryStat = 0;
  private Integer tableQueryStat = 0;
  private Integer reportStat = 0;
  private Integer total = 0;

  public String getStat() {
    return "Total: " + total + "; Table: " + tableStat + "; Single: " + singleQueryStat + "; Query: " + tableQueryStat + "; Report: " + reportStat;
  }

  public boolean addSingleQuery(SingleQuery query, int expected, boolean isLog) {
    if (iteration != currentIteration.get()) return false;
    total += 1;
    var isSucc = ApiRequests.addSingleQuery(query, expected, isLog);
    if (!isSucc) {
      System.out.println("Add single: " + query);
      singleQueryStat += 1;
    }
    return isSucc;
  }

  public boolean modifySingleQuery(SingleQuery query, int expected, boolean isLog) {
    if (iteration != currentIteration.get()) return false;
    total += 1;
    var isSucc = ApiRequests.modifySingleQuery(query, expected, isLog);
    if (!isSucc) {
      System.out.println("Modify single: " + query);
      singleQueryStat += 1;
    }
    return isSucc;
  }

  public boolean deleteSingleQuery(int queryId, int expected, boolean isLog) {
    if (iteration != currentIteration.get()) return false;
    total += 1;
    var isSucc = ApiRequests.deleteSingleQuery(queryId, expected, isLog);
    if (!isSucc) {
      System.out.println("Delete single: " + queryId);
      singleQueryStat += 1;
    }
    return isSucc;
  }

  public boolean executeSingleQuery(int queryId, int expected, boolean isLog) {
    if (iteration != currentIteration.get()) return false;
    total += 1;
    var isSucc = ApiRequests.executeSingleQuery(queryId, expected, isLog);
    if (!isSucc) {
      System.out.println("Executw single: " + queryId);
      singleQueryStat += 1;
    }
    return isSucc;
  }

  public boolean getSingleQueryById(SingleQuery query, int expected, boolean isLog) {
    if (iteration != currentIteration.get()) return false;
    total += 1;
    var isSucc = ApiRequests.getSingleQueryById(query, expected, isLog);
    if (!isSucc) {
      singleQueryStat += 1;
    }
    return isSucc;
  }

  public boolean addTableQuery(TableQuery query, int expected, boolean isLog) {
    if (iteration != currentIteration.get()) return false;
    total += 1;
    var isSucc = ApiRequests.addTableQuery(query, expected, isLog);
    if (!isSucc) {
      tableQueryStat += 1;
    }
    return isSucc;
  }

  public boolean modifyTableQuery(TableQuery query, int expected, boolean isLog) {
    if (iteration != currentIteration.get()) return false;
    total += 1;
    var isSucc = ApiRequests.modifyTableQuery(query, expected, isLog);
    if (!isSucc) {
      tableQueryStat += 1;
    }
    return isSucc;
  }

  public boolean allTableQueryByName(String tableName, int expected, List<TableQuery> queries, boolean isLog) {
    if (iteration != currentIteration.get()) return false;
    total += 1;
    var isSucc = ApiRequests.allTableQueryByName(tableName, expected, queries, isLog);
    if (!isSucc) {
      tableQueryStat += 1;
    }
    return isSucc;
  }

  public boolean getTableQueryById(TableQuery query, int expected, boolean isLog) {
    if (iteration != currentIteration.get()) return false;
    total += 1;
    var isSucc = ApiRequests.getTableQueryById(query, expected, isLog);
    if (!isSucc) {
      tableQueryStat += 1;
    }
    return isSucc;
  }

  public boolean executeTableQuery(int queryId, int expected, boolean isLog) {
    if (iteration != currentIteration.get()) return false;
    total += 1;
    var isSucc = ApiRequests.executeTableQuery(queryId, expected, isLog);
    if (!isSucc) {
      tableQueryStat += 1;
    }
    return isSucc;
  }

  public boolean deleteTableQuery(int queryId, int expected, boolean isLog) {
    if (iteration != currentIteration.get()) return false;
    total += 1;
    var isSucc = ApiRequests.deleteTableQuery(queryId, expected, isLog);
    if (!isSucc) {
      tableQueryStat += 1;
    }
    return isSucc;
  }

  public boolean createTable(DBTable table, int expected, boolean isLog) {
    if (iteration != currentIteration.get()) return false;
    total += 1;
    var isSucc = ApiRequests.createTable(table, expected, isLog);
    if (!isSucc) {
      System.out.println("Create table: " + table);
      tableStat += 1;
    }
    return isSucc;
  }

  public boolean dropTable(String tableName, int expected, boolean isLog) {
    if (iteration != currentIteration.get()) return false;
    total += 1;
    var isSucc = ApiRequests.dropTable(tableName, expected, isLog);
    if (!isSucc) {
      System.out.println("Drop table: " + tableName + " " + expected);
      tableStat += 1;
    }
    return isSucc;
  }

  public boolean getTableByName(DBTable table, boolean isLog) {
    if (iteration != currentIteration.get()) return false;
    total += 1;
    var isSucc = ApiRequests.getTableByName(table, isLog);
    if (!isSucc) {
      System.out.println("Get table: " + table);
      tableStat += 1;
    }
    return isSucc;
  }

  public boolean createReport(Report<ColumnInfo> report, int expected, boolean isLog) {
    if (iteration != currentIteration.get()) return false;
    total += 1;
    var isSucc = ApiRequests.createReport(report, expected, isLog);
    if (!isSucc) {
      reportStat += 1;
    }
    return isSucc;
  }

  public boolean getReportStat(Report<ColumnInfoWithSize> report, int expected, boolean isLog) {
    if (iteration != currentIteration.get()) return false;
    total += 1;
    var isSucc = ApiRequests.getReport(report, expected, isLog);
    if (!isSucc) {
      reportStat += 1;
    }
    return isSucc;
  }
}