package com.matheus.integration;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;

import com.matheus.model.PersonEnhanced;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import javax.inject.Inject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;

@QuarkusTest
@QuarkusTestResource(DynamoDbResource.class)
class PersonSyncIntegrationTest {

  @Inject
  DynamoDbEnhancedClient dynamoDbEnhancedClient;

  @BeforeEach
  void setUp() {
    DynamoDbTable<PersonEnhanced> table = dynamoDbEnhancedClient.table(PersonEnhanced.TABLE_NAME,
        TableSchema.fromBean(PersonEnhanced.class));

    table.createTable(builder -> builder
        .provisionedThroughput(b -> b
            .readCapacityUnits(10L)
            .writeCapacityUnits(10L)
            .build()));
  }

  @AfterEach
  void tearDown() {
    DynamoDbTable<PersonEnhanced> table = dynamoDbEnhancedClient.table(PersonEnhanced.TABLE_NAME,
        TableSchema.fromBean(PersonEnhanced.class));

    table.deleteTable();
  }

  @Test
  @DisplayName("Should find all persons successfully")
  void shouldFindAllPersonsSuccessfully() {
    DynamoDbTable<PersonEnhanced> table = dynamoDbEnhancedClient.table(PersonEnhanced.TABLE_NAME,
        TableSchema.fromBean(PersonEnhanced.class));
    table.putItem(new PersonEnhanced("firstNameTest", "lastNameTest", "cpfTest"));

    given()
        .when()
        .get("/sync/person")
        .then()
        .statusCode(200)
        .body(
            is("[{\"firstName\":\"firstNameTest\",\"lastName\":\"lastNameTest\",\"cpf\":\"cpfTest\"}]"));
  }

  @Test
  @DisplayName("Should find person by firstname successfully")
  void shouldFindPersonByFirstNameSuccessfully() {
    DynamoDbTable<PersonEnhanced> table = dynamoDbEnhancedClient.table(PersonEnhanced.TABLE_NAME,
        TableSchema.fromBean(PersonEnhanced.class));
    table.putItem(new PersonEnhanced("firstNameTest", "lastNameTest", "cpfTest"));

    given()
        .when()
        .get("/sync/person/firstname/firstNameTest")
        .then()
        .statusCode(200)
        .body(is("{\"items\":[{\"firstName\":\"firstNameTest\",\"lastName\":\"lastNameTest\",\"cpf\":\"cpfTest\"}],\"size\":1,\"lastEvaluatedKey\":{}}"));
  }

  @Test
  @DisplayName("Should create person successfully")
  void shouldCreatePersonSuccessfully() {
    PersonEnhanced person = new PersonEnhanced("firstNameCreateTest", "lastNameCreateTest",
        "cpfCreateTest");

    given()
        .when()
        .body(person)
        .contentType(ContentType.JSON)
        .post("/sync/person")
        .then()
        .statusCode(200)
        .body(
            is("{\"firstName\":\"firstNameCreateTest\",\"lastName\":\"lastNameCreateTest\",\"cpf\":\"cpfCreateTest\"}"));
  }
}
