package ru.vk.competition.minbenchmark.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Report<T> {
  @NonNull
  @JsonProperty("reportId")
  private int id;
  private int tableAmount;
  @NonNull
  private List<ReportTable<T>> tables;
}