package ru.vk.competition.minbenchmark.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DBTable {
  @NonNull
  @JsonProperty("tableName")
  private String name;
  private int columnsAmount;
  @NonNull
  private String primaryKey;
  @NonNull
  private List<ColumnInfo> columnInfos;
}