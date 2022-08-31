package ru.vk.competition.minbenchmark.service;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.test.StepVerifier;
import ru.vk.competition.minbenchmark.entity.*;

import java.util.Arrays;
import java.util.Collections;

import static ru.vk.competition.minbenchmark.util.TableSchemaUtil.CHARACTER_TYPE;

@SpringBootTest
public class ReportServiceTest {

  @Autowired
  private TableService tableService;

  @Autowired
  private ReportService reportService;

  @Autowired
  private TableQueryService tableQueryService;

  @Autowired
  private SingleQueryService singleQueryService;

  @BeforeEach
  void createDB() {
    DBTable TABLE1 = new DBTable(
      "Test",
      2, "ID",
      Arrays.asList(
        new ColumnInfo("ID", "int4"),
        new ColumnInfo("data", "varchar(40)")
      )
    );

    DBTable TABLE2 = new DBTable(
      "Test1",
      2, "NAME",
      Arrays.asList(
        new ColumnInfo("NAME", CHARACTER_TYPE),
        new ColumnInfo("DESC", CHARACTER_TYPE)
      )
    );

    tableService.createTable(TABLE1).block();
    tableService.createTable(TABLE2).block();
  }

  @AfterEach
  void clear() {
    reportService.clear();
  }

  @Test
  void createReport() {
    StepVerifier.create(reportService.createReport(REPORT)).expectNext(true).verifyComplete();
    StepVerifier.create(reportService.createReport(REPORT)).expectNext(false).verifyComplete();
  }

  @Test
  void createReportBadAmount() {
    var report = new Report<>(1, 1, Arrays.asList(TABLE1, TABLE2));
    StepVerifier.create(reportService.createReport(report)).expectNext(false).verifyComplete();
  }

  @Test
  void createReportBadTable() {
    var table = new ReportTable<>(
      "Test3",
      Arrays.asList(
        new ColumnInfo("NAME", CHARACTER_TYPE),
        new ColumnInfo("DESC", CHARACTER_TYPE)
      )
    );

    var report = new Report<>(1, 2, Arrays.asList(TABLE1, table));
    StepVerifier.create(reportService.createReport(report)).expectNext(false).verifyComplete();
  }

  @Test
  void createReportBadTableName() {
    var table = new ReportTable<>(
      "Test2",
      Arrays.asList(
        new ColumnInfo("NAME", CHARACTER_TYPE),
        new ColumnInfo("lol", CHARACTER_TYPE)
      )
    );

    var report = new Report<>(1, 2, Arrays.asList(TABLE1, table));
    StepVerifier.create(reportService.createReport(report)).expectNext(false).verifyComplete();
  }

  @Test
  void createReportBadTypeName() {
    var table = new ReportTable<>(
      "Test2",
      Arrays.asList(
        new ColumnInfo("NAME", CHARACTER_TYPE),
        new ColumnInfo("DESC", "lol")
      )
    );

    var report = new Report<>(1, 2, Arrays.asList(TABLE1, table));
    StepVerifier.create(reportService.createReport(report)).expectNext(false).verifyComplete();
  }

  @Test
  void getReport() {
    StepVerifier.create(reportService.createReport(REPORT)).expectNext(true).verifyComplete();

    var query = new TableQuery(1, "Test", "insert into Test (ID, data) values (1, 'Hello!'), (2, null);");
    StepVerifier.create(tableQueryService.addNewTableQuery(query)).expectNext(true).verifyComplete();
    StepVerifier.create(tableQueryService.executeTableQuery(1)).expectNext(true).verifyComplete();

    var query1 = new TableQuery(2, "Test1", "insert into Test1 (NAME, desc) values ('Vasya', 'Hello!')");
    StepVerifier.create(tableQueryService.addNewTableQuery(query1)).expectNext(true).verifyComplete();
    StepVerifier.create(tableQueryService.executeTableQuery(2)).expectNext(true).verifyComplete();

    StepVerifier.create(reportService.getReportById(1)).expectNext(REPORT1).verifyComplete();
  }

  @Test
  void getReportBadId() {
    StepVerifier.create(reportService.getReportById(1)).verifyComplete();
  }

  @Test
  void getReportAfterChange() {
    var query1 = new SingleQuery(1, "create table ReportTable (id int4, data varchar);");
    var query2 = new SingleQuery(2, "insert into ReportTable (ID, data) values (1, 'Hello!'), (2, null);");

    StepVerifier.create(singleQueryService.addNewQuery(query1)).expectNext(true).verifyComplete();
    StepVerifier.create(singleQueryService.addNewQuery(query2)).expectNext(true).verifyComplete();
    StepVerifier.create(singleQueryService.executeQuery(1)).expectNext(true).verifyComplete();
    StepVerifier.create(singleQueryService.executeQuery(2)).expectNext(true).verifyComplete();

    var report = new Report<>(1, 1, Collections.singletonList(
      new ReportTable<>("reporttable", Arrays.asList(
        new ColumnInfo("id", "int4"),
        new ColumnInfo("data", "varchar")
      ))
    ));

    StepVerifier.create(reportService.createReport(report)).expectNext(true).verifyComplete();

    var query3 = new SingleQuery(3, "drop table REPORTTABLE;");
    StepVerifier.create(singleQueryService.addNewQuery(query3)).expectNext(true).verifyComplete();
    StepVerifier.create(singleQueryService.executeQuery(3)).expectNext(true).verifyComplete();

    var reportRes = new Report<>(1, 1, Collections.singletonList(
      new ReportTable<>("reporttable", Arrays.asList(
        new ColumnInfoWithSize("id", "int4", "2"),
        new ColumnInfoWithSize("data", "varchar", "1")
      ))
    ));

    StepVerifier.create(reportService.getReportById(1)).expectNext(reportRes).verifyComplete();
  }

  private final static ReportTable<ColumnInfo> TABLE1 = new ReportTable<>(
    "Test",
    Arrays.asList(
      new ColumnInfo("ID", "int4"),
      new ColumnInfo("data", "varchar(40)")
    )
  );

  private final static ReportTable<ColumnInfo> TABLE2 = new ReportTable<>(
    "Test1",
    Arrays.asList(
      new ColumnInfo("NAME", CHARACTER_TYPE),
      new ColumnInfo("DESC", CHARACTER_TYPE)
    )
  );

  private final static Report<ColumnInfo> REPORT = new Report<>(1, 2, Arrays.asList(TABLE1, TABLE2));

  private final static ReportTable<ColumnInfoWithSize> TABLE3 = new ReportTable<>(
    "Test",
    Arrays.asList(
      new ColumnInfoWithSize("ID", "int4", "2"),
      new ColumnInfoWithSize("data", "varchar(40)", "1")
    )
  );

  private final static ReportTable<ColumnInfoWithSize> TABLE4 = new ReportTable<>(
    "Test1",
    Arrays.asList(
      new ColumnInfoWithSize("NAME", CHARACTER_TYPE, "1"),
      new ColumnInfoWithSize("DESC", CHARACTER_TYPE, "1")
    )
  );

  private final static Report<ColumnInfoWithSize> REPORT1 = new Report<>(1, 2, Arrays.asList(TABLE3, TABLE4));
}


