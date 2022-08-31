package ru.vk.competition.minbenchmark.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DBTable {
  @JsonProperty("tableName")
  private String name;
  private Integer columnsAmount;
  private String primaryKey;
  private List<ColumnInfo> columnInfos;
}