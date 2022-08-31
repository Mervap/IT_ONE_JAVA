package ru.vk.competition.minchecker.utils;

import lombok.extern.slf4j.Slf4j;
import ru.vk.competition.minchecker.entity.*;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

@Slf4j(topic = "API")
public class ApiRequests {

  private static final AtomicInteger counter = new AtomicInteger();

  public static boolean addSingleQuery(SingleQuery query, int expected, boolean isLog) {
    assert(expected == 400 || expected == 201);
    return makeRequest(
      "/single-query/add-new-query-result", expected,
      id -> OkHttp.post("/single-query/add-new-query", id, query, isLog),
      false
    );
  }

  public static boolean modifySingleQuery(SingleQuery query, int expected, boolean isLog) {
    assert(expected == 406 || expected == 200);
    return makeRequest(
      "/single-query/add-modify-result", expected,
      id -> OkHttp.put("/single-query/modify-single-query", id, query, isLog),
      false
    );
  }

  public static boolean deleteSingleQuery(int queryId, int expected, boolean isLog) {
    assert(expected == 406 || expected == 202);
    return makeRequest(
      "/single-query/add-delete-result", expected,
      id -> OkHttp.delete("/single-query/delete-single-query-by-id/" + queryId, id, isLog),
      false
    );
  }

  public static boolean executeSingleQuery(int queryId, int expected, boolean isLog) {
    assert(expected == 406 || expected == 201);
    return makeRequest(
      "/single-query/add-execute-result", expected,
      id -> OkHttp.get("/single-query/execute-single-query-by-id/" + queryId, id, isLog),
      false
    );
  }

  public static boolean getSingleQueryById(SingleQuery query, int expected, boolean isLog) {
    assert(expected == 500 || expected == 200);
    return makeRequest(
      "/single-query/add-get-single-query-by-id-result",
      id -> new GetSingleQueryResult(id, expected, query.getId(), query.getQuery()),
      GetSingleQueryResult::getCode,
      id -> OkHttp.get("/single-query/get-single-query-by-id/" + query.getId(), id, isLog),
      false
    );
  }

  public static boolean addTableQuery(TableQuery query, int expected, boolean isLog) {
    assert(expected == 406 || expected == 201);
    return makeRequest(
      "/table-query/add-new-query-to-table-result",
      id -> new TableQueryResult(id, expected, query),
      TableQueryResult::getCode,
      id -> OkHttp.post("/table-query/add-new-query-to-table", id, query, isLog),
      false
    );
  }

  public static boolean modifyTableQuery(TableQuery query, int expected, boolean isLog) {
    assert(expected == 406 || expected == 201);
    return makeRequest(
      "/table-query/modify-query-in-table-result", expected,
      id -> OkHttp.put("/table-query/modify-query-in-table", id, query, isLog),
      false
    );
  }

  public static boolean allTableQueryByName(String tableName, int expected, List<TableQuery> queries, boolean isLog) {
    assert(expected == 406 || expected == 201);
    return makeRequest(
      "/table-query/get-all-queries-by-table-name-result",
      id -> new GetAllTableQueriesResult(id, expected, queries),
      GetAllTableQueriesResult::getCode,
      id -> OkHttp.get("/table-query/get-all-queries-by-table-name/" + tableName, id, isLog),
      false
    );
  }

  public static boolean getTableQueryById(TableQuery query, int expected, boolean isLog) {
    assert(expected == 500 || expected == 200);
    return makeRequest(
      "/table-query/get-table-query-by-id-result",
      id -> new TableQueryResult(id, expected, query),
      TableQueryResult::getCode,
      id -> OkHttp.get("/table-query/get-table-query-by-id/" + query.getId(), id, isLog),
      false
    );
  }

  public static boolean executeTableQuery(int queryId, int expected, boolean isLog) {
    assert(expected == 406 || expected == 201);
    return makeRequest(
      "/table-query/execute-table-query-by-id-result", expected,
      id -> OkHttp.get("/table-query/execute-table-query-by-id/" + queryId, id, isLog),
      false
    );
  }

  public static boolean deleteTableQuery(int queryId, int expected, boolean isLog) {
    assert(expected == 406 || expected == 201);
    return makeRequest(
      "/table-query/delete-table-query-by-id-result", expected,
      id -> OkHttp.delete("/table-query/delete-table-query-by-id/" + queryId, id, isLog),
      false
    );
  }

  public static boolean createTable(DBTable table, int expected, boolean isLog) {
    assert(expected == 406 || expected == 201);
    return makeRequest(
      "/table/add-create-table-result",
      expected, id -> OkHttp.post("/table/create-table", id, table, isLog),
      false
    );
  }

  public static boolean dropTable(String tableName, int expected, boolean isLog) {
    assert(expected == 406 || expected == 201);
    return makeRequest(
      "/table/add-drop-table-result", expected,
      id -> OkHttp.delete("/table/drop-table/" + tableName, id, isLog),
      false
    );
  }

  public static boolean getTableByName(DBTable table, boolean isLog) {
    return makeRequest(
      "/table/add-get-table-by-name-result",
      id -> new GetTableResult(id, 200, table),
      GetTableResult::getCode,
      id -> OkHttp.get("/table/get-table-by-name/" + table.getName(), id, isLog),
      false
    );
  }

  public static boolean createReport(Report<ColumnInfo> report, int expected, boolean isLog) {
    assert(expected == 406 || expected == 201);
    return makeRequest(
      "/report/add-create-report-result", expected,
      id -> OkHttp.post("/report/create-report", id, report, isLog),
      false
    );
  }

  public static boolean getReport(Report<ColumnInfoWithSize> report, int expected, boolean isLog) {
    assert(expected == 406 || expected == 201);
    return makeRequest(
      "/report/add-get-report-by-id-result",
      id -> new GetReportResult(id, expected, report),
      GetReportResult::getCode,
      id -> OkHttp.get("/report/get-report-by-id/" + report.getId(), id, isLog),
      false
    );
  }

  private static boolean makeRequest(String addRequestUrl, int expected, Function<Integer, Integer> main, boolean isLog) {
    var id = counter.getAndIncrement();
    OkHttp.post(addRequestUrl, new SimpleResult(id, expected), isLog);
    var code = main.apply(id);
    return code == expected;
  }

  private static <T> boolean makeRequest(String addRequestUrl, Function<Integer, T> query, Function<T, Integer> codeGetter, Function<Integer, Integer> main, boolean isLog) {
    var id = counter.getAndIncrement();
    T result = query.apply(id);
    OkHttp.post(addRequestUrl, result, isLog);
    var code = main.apply(id);
    return code.equals(codeGetter.apply(result));
  }
}
