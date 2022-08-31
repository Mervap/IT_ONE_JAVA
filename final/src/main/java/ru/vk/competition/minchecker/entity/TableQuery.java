package ru.vk.competition.minchecker.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TableQuery {
  @JsonProperty("queryId")
  int id;
  @NonNull
  String tableName;
  @NonNull
  String query;
}
