package com.matheus.component;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;

import com.matheus.component.resources.DynamoDbResourceTest;
import com.matheus.model.Person;
import com.matheus.model.PersonEnhanced;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import javax.inject.Inject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvFileSource;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.BatchWriteItemEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.CreateTableEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.EnhancedGlobalSecondaryIndex;
import software.amazon.awssdk.enhanced.dynamodb.model.WriteBatch;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;

@QuarkusTest
@QuarkusTestResource(DynamoDbResourceTest.class)
class PersonEnhancedAsyncComponentTest {

  @Inject
  DynamoDbEnhancedClient dynamoDbEnhancedClient;

  @BeforeEach
  void setUp() {
    createEnhancedPersonTable();
  }

  @AfterEach
  void tearDown() {
    DynamoDbTable<PersonEnhanced> table = dynamoDbEnhancedClient.table(
        PersonEnhanced.TABLE_NAME, TableSchema.fromBean(PersonEnhanced.class));

    table.deleteTable();
  }

  @ParameterizedTest
  @DisplayName("Should find enhanced person successfully")
  @CsvFileSource(resources = "/FindPersonTestData.csv", numLinesToSkip = 1, delimiter = '|')
  void shouldPersonSuccessfully(final String path, final String expectedResponse) {
    insertPersonsDataBase();

    given()
        .log().ifValidationFails()
        .when()
        .get("/async/enhanced" + path)
        .then()
        .log().ifValidationFails()
        .statusCode(200)
        .body(
            is(expectedResponse));
  }

  @Test
  @DisplayName("Should create person successfully")
  void shouldCreatePersonSuccessfully() {
    PersonEnhanced person = PersonEnhanced.of("firstNameCreateTest", "lastNameCreateTest",
        "cpfCreateTest");

    given()
        .log().ifValidationFails()
        .when()
        .body(person)
        .contentType(ContentType.JSON)
        .post("/async/enhanced/person")
        .then()
        .log().ifValidationFails()
        .statusCode(200)
        .body(
            is("{\"firstName\":\"firstNameCreateTest\",\"lastName\":\"lastNameCreateTest\",\"cpf\":\"cpfCreateTest\"}"));
  }

  @Test
  @DisplayName("Should delete person successfully")
  void shouldDeletePersonSuccessfully() {
    DynamoDbTable<PersonEnhanced> table = dynamoDbEnhancedClient.table(
        PersonEnhanced.TABLE_NAME, TableSchema.fromBean(PersonEnhanced.class));

    table.putItem(PersonEnhanced.of("firstNameTest", "lastNameTest", "cpfTest"));

    given()
        .log().ifValidationFails()
        .when()
        .delete("/async/enhanced/person/firstname/firstNameTest/lastname/lastNameTest")
        .then()
        .log().ifValidationFails()
        .statusCode(200)
        .body(
            is("{\"firstName\":\"firstNameTest\",\"lastName\":\"lastNameTest\",\"cpf\":\"cpfTest\"}"));
  }

  @Test
  @DisplayName("Should update person successfully")
  void shouldUpdateSuccessfully() {
    DynamoDbTable<PersonEnhanced> table = dynamoDbEnhancedClient.table(
        PersonEnhanced.TABLE_NAME, TableSchema.fromBean(PersonEnhanced.class));

    table.putItem(PersonEnhanced.of("firstNameTest", "lastNameTest", "cpfTest"));

    Person person = Person.of("firstNameTest", "lastNameTest",
        "cpfUpdatedTest");

    given()
        .log().ifValidationFails()
        .when()
        .body(person)
        .contentType(ContentType.JSON)
        .put("/async/enhanced/person")
        .then()
        .log().ifValidationFails()
        .statusCode(200)
        .body(
            is("{\"firstName\":\"firstNameTest\",\"lastName\":\"lastNameTest\",\"cpf\":\"cpfUpdatedTest\"}"));
  }

  private void createEnhancedPersonTable() {
    DynamoDbTable<PersonEnhanced> table = dynamoDbEnhancedClient.table(
        PersonEnhanced.TABLE_NAME, TableSchema.fromBean(PersonEnhanced.class));

    ProvisionedThroughput provisionedThroughput = ProvisionedThroughput.builder()
        .writeCapacityUnits(1L)
        .readCapacityUnits(1L)
        .build();

    EnhancedGlobalSecondaryIndex enhancedGlobalSecondaryIndex = EnhancedGlobalSecondaryIndex.builder()
        .provisionedThroughput(provisionedThroughput)
        .indexName(PersonEnhanced.CPF_INDEX_NAME)
        .projection(Projection.builder()
            .projectionType(ProjectionType.ALL)
            .build())
        .build();

    CreateTableEnhancedRequest createTableEnhancedRequest = CreateTableEnhancedRequest.builder()
        .provisionedThroughput(provisionedThroughput)
        .globalSecondaryIndices(enhancedGlobalSecondaryIndex)
        .build();

    table.createTable(createTableEnhancedRequest);
  }

  private void insertPersonsDataBase() {
    DynamoDbTable<PersonEnhanced> table = dynamoDbEnhancedClient.table(
        PersonEnhanced.TABLE_NAME, TableSchema.fromBean(PersonEnhanced.class));

    WriteBatch person1WriteBatch = WriteBatch.builder(PersonEnhanced.class)
        .addPutItem(PersonEnhanced.of("Person1", "lastNameTest", "86679311031"))
        .addPutItem(PersonEnhanced.of("Person2", "lastNameTest", "86679311032"))
        .addPutItem(PersonEnhanced.of("Person3", "lastNameTest", "86679311033"))
        .addPutItem(PersonEnhanced.of("Person4", "lastNameTest", "86679311034"))
        .addPutItem(PersonEnhanced.of("Person5", "lastNameTest", "86679311035"))
        .mappedTableResource(table)
        .build();

    BatchWriteItemEnhancedRequest batchWriteItemRequest = BatchWriteItemEnhancedRequest.builder()
        .addWriteBatch(person1WriteBatch)
        .build();

    dynamoDbEnhancedClient.batchWriteItem(batchWriteItemRequest);
  }
}