package com.matheus.integration;

import com.matheus.model.PersonEnhanced;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.QuarkusTest;
import javax.inject.Inject;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;

@QuarkusTest
@QuarkusTestResource(DynamoDbResource.class)
public class PersonSyncIntegrationTest {

  @Inject
  DynamoDbEnhancedClient dynamoDbEnhancedClient;

  @Test
  void name() {
    DynamoDbTable<PersonEnhanced> table = dynamoDbEnhancedClient.table(PersonEnhanced.TABLE_NAME,
        TableSchema.fromBean(PersonEnhanced.class));

    table.createTable(builder -> builder
        .provisionedThroughput(b -> b
            .readCapacityUnits(10L)
            .writeCapacityUnits(10L)
            .build()));
  }
}
