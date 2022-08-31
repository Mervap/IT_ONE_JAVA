package ru.vk.competition.minchecker.utils;

import lombok.NoArgsConstructor;

@NoArgsConstructor
public class SystemVariables {

  private final String api = System.getenv("rs.endpoint");

  public String api() {
    return api + "/api";
  }
}
