package ru.vk.competition.minbenchmark.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

import javax.persistence.Id;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ColumnInfo {
  @Id
  @NonNull
  @JsonProperty("title")
  private String name;
  @NonNull
  private String type;
}