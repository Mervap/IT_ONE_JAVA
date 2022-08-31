package ru.vk.competition.minbenchmark.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

import javax.persistence.Id;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SingleQuery {

  @Id
  @JsonProperty("queryId")
  private int id;
  @NonNull
  private String query;
}
