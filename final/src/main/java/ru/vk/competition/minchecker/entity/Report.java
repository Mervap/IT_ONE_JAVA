package ru.vk.competition.minchecker.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Report<T> {
  @JsonProperty("reportId")
  private int id;
  private int tableAmount;
  @NonNull
  private List<ReportTable<T>> tables;
}