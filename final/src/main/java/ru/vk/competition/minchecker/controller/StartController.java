package ru.vk.competition.minchecker.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.vk.competition.minchecker.service.StartService;

@Slf4j
@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class StartController {

  private final StartService startService;

  @GetMapping("/start")
  public void onStartMission() {
    startService.onStartMission();
  }
}
