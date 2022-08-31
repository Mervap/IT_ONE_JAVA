package ru.vk.competition.minchecker.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SingleQuery {

  @JsonProperty("queryId")
  private int id;
  @NonNull
  private String query;
}
