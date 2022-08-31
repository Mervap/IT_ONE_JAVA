package ru.vk.competition.minchecker.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ColumnInfoWithSize {
  @NonNull
  @JsonProperty("title")
  private String name;
  @NonNull
  private String type;
  @NonNull
  private String size;
}