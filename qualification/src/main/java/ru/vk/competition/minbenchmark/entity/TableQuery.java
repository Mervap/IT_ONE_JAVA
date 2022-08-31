package ru.vk.competition.minbenchmark.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

import javax.persistence.Id;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TableQuery {
  @Id
  @JsonProperty("queryId")
  int id;
  @NonNull
  String tableName;
  @NonNull
  String query;
}
