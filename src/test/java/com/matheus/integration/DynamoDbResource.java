package com.matheus.integration;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.util.Map;
import org.testcontainers.containers.GenericContainer;

public class DynamoDbResource implements QuarkusTestResourceLifecycleManager {

  private GenericContainer dynamodb;

  @Override
  public Map<String, String> start() {
    dynamodb = new GenericContainer("amazon/dynamodb-local")
        .withExposedPorts(8000)
        .withCommand("-jar DynamoDBLocal.jar -inMemory -sharedDb");

    dynamodb.start();
    return Map.of("quarkus.dynamodb.endpoint-override",
        "http://localhost:" + dynamodb.getFirstMappedPort());
  }

  @Override
  public void stop() {
    dynamodb.stop();
  }
}
