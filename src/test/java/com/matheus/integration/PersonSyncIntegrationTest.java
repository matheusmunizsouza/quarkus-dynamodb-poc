package com.matheus.integration;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;

import com.matheus.model.Person;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import java.util.List;
import javax.inject.Inject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

@QuarkusTest
@QuarkusTestResource(DynamoDbResource.class)
class PersonSyncIntegrationTest {

  @Inject
  DynamoDbClient dynamoDbClient;

  @BeforeEach
  void setUp() {
    createPersonTable();
  }

  @AfterEach
  void tearDown() {
    deletePersonTable();
  }

  @Test
  @DisplayName("Should find all persons successfully")
  void shouldFindAllPersonsSuccessfully() {
    PutItemRequest putItemRequest = PutItemRequest.builder()
        .tableName(Person.TABLE_NAME)
        .item(Person.of("firstNameTest", "lastNameTest", "cpfTest").toDynamodbAttributes())
        .build();

    dynamoDbClient.putItem(putItemRequest);

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
    PutItemRequest putItemRequest = PutItemRequest.builder()
        .tableName(Person.TABLE_NAME)
        .item(Person.of("firstNameTest", "lastNameTest", "cpfTest").toDynamodbAttributes())
        .build();

    dynamoDbClient.putItem(putItemRequest);

    given()
        .when()
        .get("/sync/person/firstname/firstNameTest")
        .then()
        .statusCode(200)
        .body(
            is("{\"items\":[{\"firstName\":\"firstNameTest\",\"lastName\":\"lastNameTest\",\"cpf\":\"cpfTest\"}],\"size\":1,\"lastEvaluatedKey\":{}}"));
  }

  @Test
  @DisplayName("Should create person successfully")
  void shouldCreatePersonSuccessfully() {
    Person person = Person.of("firstNameCreateTest", "lastNameCreateTest",
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

  private void createPersonTable() {

    CreateTableRequest createTableRequest = CreateTableRequest.builder()
        .tableName(Person.TABLE_NAME)
        .attributeDefinitions(getAttributeDefinitions())
        .keySchema(getKeys())
        .globalSecondaryIndexes(getGlobalSecondaryIndex())
        .provisionedThroughput(getProvisionedThroughput())
        .build();

    dynamoDbClient.createTable(createTableRequest);
  }

  private List<AttributeDefinition> getAttributeDefinitions() {
    AttributeDefinition firstName = AttributeDefinition.builder()
        .attributeName("firstName")
        .attributeType("S")
        .build();

    AttributeDefinition lastName = AttributeDefinition.builder()
        .attributeName("lastName")
        .attributeType("S")
        .build();

    AttributeDefinition cpf = AttributeDefinition.builder()
        .attributeName("cpf")
        .attributeType("S")
        .build();

    return List.of(firstName, lastName, cpf);
  }

  private List<KeySchemaElement> getKeys() {

    KeySchemaElement hashKey = KeySchemaElement.builder()
        .attributeName("firstName")
        .keyType(KeyType.HASH)
        .build();

    KeySchemaElement rangeKey = KeySchemaElement.builder()
        .attributeName("lastName")
        .keyType(KeyType.RANGE)
        .build();

    return List.of(hashKey, rangeKey);
  }

  private GlobalSecondaryIndex getGlobalSecondaryIndex() {
    KeySchemaElement cpfIndex = KeySchemaElement.builder()
        .attributeName("cpf")
        .keyType(KeyType.HASH)
        .build();

    return GlobalSecondaryIndex.builder()
        .indexName("cpf_index")
        .keySchema(cpfIndex)
        .provisionedThroughput(getProvisionedThroughput())
        .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
        .build();
  }

  private ProvisionedThroughput getProvisionedThroughput() {
    return ProvisionedThroughput.builder()
        .readCapacityUnits(1L)
        .writeCapacityUnits(1L)
        .build();
  }

  private void deletePersonTable() {
    DeleteTableRequest deleteTableRequest = DeleteTableRequest.builder()
        .tableName(Person.TABLE_NAME)
        .build();
    dynamoDbClient.deleteTable(deleteTableRequest);
  }
}
