package com.matheus.component;

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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvFileSource;
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
@QuarkusTestResource(DynamoDbResourceTest.class)
class PersonSyncComponentTest {

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

  @ParameterizedTest
  @DisplayName("Should find person successfully")
  @CsvFileSource(resources = "/FindPersonSyncTestData.csv", numLinesToSkip = 1, delimiter = '|')
  void shouldPersonSuccessfully(final String url, final String expectedResponse) {
    PutItemRequest putItemRequest = PutItemRequest.builder()
        .tableName(Person.TABLE_NAME)
        .item(Person.of("firstNameTest", "lastNameTest", "cpfTest").toDynamodbAttributes())
        .build();

    dynamoDbClient.putItem(putItemRequest);

    given()
        .log().ifValidationFails()
        .when()
        .get(url)
        .then()
        .log().ifValidationFails()
        .statusCode(200)
        .body(
            is(expectedResponse));
  }

  @Test
  @DisplayName("Should create person successfully")
  void shouldCreatePersonSuccessfully() {
    Person person = Person.of("firstNameCreateTest", "lastNameCreateTest",
        "cpfCreateTest");

    given()
        .log().ifValidationFails()
        .when()
        .body(person)
        .contentType(ContentType.JSON)
        .post("/sync/person")
        .then()
        .log().ifValidationFails()
        .statusCode(200)
        .body(
            is("{\"firstName\":\"firstNameCreateTest\",\"lastName\":\"lastNameCreateTest\",\"cpf\":\"cpfCreateTest\"}"));
  }

  @Test
  @DisplayName("Should delete person successfully")
  void shouldDeletePersonSuccessfully() {
    PutItemRequest putItemRequest = PutItemRequest.builder()
        .tableName(Person.TABLE_NAME)
        .item(Person.of("firstNameTest", "lastNameTest", "cpfTest").toDynamodbAttributes())
        .build();

    dynamoDbClient.putItem(putItemRequest);

    given()
        .log().ifValidationFails()
        .when()
        .delete("/sync/person/firstname/firstNameTest/lastname/lastNameTest")
        .then()
        .log().ifValidationFails()
        .statusCode(200)
        .body(
            is("{\"firstName\":\"firstNameTest\",\"lastName\":\"lastNameTest\",\"cpf\":\"cpfTest\"}"));
  }

  @Test
  @DisplayName("Should update person successfully")
  void shouldUpdateSuccessfully() {
    PutItemRequest putItemRequest = PutItemRequest.builder()
        .tableName(Person.TABLE_NAME)
        .item(Person.of("firstNameTest", "lastNameTest", "cpfTest").toDynamodbAttributes())
        .build();

    dynamoDbClient.putItem(putItemRequest);

    Person person = Person.of("firstNameTest", "lastNameTest",
        "cpfUpdatedTest");

    given()
        .log().ifValidationFails()
        .when()
        .body(person)
        .contentType(ContentType.JSON)
        .put("/sync/person")
        .then()
        .log().ifValidationFails()
        .statusCode(200)
        .body(
            is("{\"firstName\":\"firstNameTest\",\"lastName\":\"lastNameTest\",\"cpf\":\"cpfUpdatedTest\"}"));
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
