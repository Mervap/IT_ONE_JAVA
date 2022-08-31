package ru.vk.competition.minchecker.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class GetReportResult {
  private int resultId;
  private int code;
  private Report<ColumnInfoWithSize> getReport;
}

