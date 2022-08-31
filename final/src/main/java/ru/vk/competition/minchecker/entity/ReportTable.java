package ru.vk.competition.minchecker.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ReportTable<T> {
  @NonNull
  @JsonProperty("tableName")
  private String name;
  @NonNull
  private List<T> columns;
}