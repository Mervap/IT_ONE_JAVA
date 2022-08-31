package ru.vk.competition.minchecker.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TableQueryResult {
  private int resultId;
  private int code;
  private TableQuery tableQuery;
}

