package com.matheus.integration;

import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.util.Map;

public class DynamoDbResource implements QuarkusTestResourceLifecycleManager {

  private DynamoDBProxyServer server;
  @Override
  public Map<String, String> start() {
    System.setProperty("sqlite4java.library.path", "native-libs");
    String port = "8000";
    try {
      server = ServerRunner.createServerFromCommandLineArgs(
          new String[]{"-inMemory", "-port", port});
      server.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return null;
  }

  @Override
  public void stop() {
    try {
      server.stop();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
