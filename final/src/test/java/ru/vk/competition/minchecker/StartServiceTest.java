package ru.vk.competition.minchecker;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import ru.vk.competition.minchecker.service.StartService;

@SpringBootTest
public class StartServiceTest {

  @Autowired
  private StartService startService;

  @Test
  void testStartMisson() {
    startService.onStartMission();
  }
}

