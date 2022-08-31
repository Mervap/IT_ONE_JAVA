package ru.vk.competition.minchecker.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class GetAllTableQueriesResult {
  private int resultId;
  private int code;
  private List<TableQuery> tableQueries;
}

