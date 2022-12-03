package com.matheus.service;

import com.matheus.model.Person;
import com.matheus.vo.request.PaginationRequest;
import com.matheus.vo.response.PaginationResponse;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import java.util.Map;
import javax.enterprise.context.ApplicationScoped;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ComparisonOperator;
import software.amazon.awssdk.services.dynamodb.model.Condition;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

@ApplicationScoped
public class PersonAsyncService {

  private final DynamoDbAsyncClient dynamoDbAsyncClient;

  public PersonAsyncService(DynamoDbAsyncClient dynamoDbAsyncClient) {
    this.dynamoDbAsyncClient = dynamoDbAsyncClient;
  }

  public Multi<Person> findAll() {
    return Multi.createFrom()
        .completionStage(() -> dynamoDbAsyncClient.scan(scanRequest()))
        .onItem()
        .transformToIterable(res -> res.items().stream().map(Person::from).toList());
  }

  public Uni<Person> findByFirstName(final String firstName) {
    return Uni.createFrom()
        .completionStage(() -> dynamoDbAsyncClient.getItem(getRequest(firstName)))
        .onItem()
        .transform(res -> Person.from(res.item()));
  }

  public Uni<Person> findByFirstNameAndLastName(final String firstName, final String lastName) {
    return Uni.createFrom()
        .completionStage(() -> dynamoDbAsyncClient.getItem(getRequest(firstName, lastName)))
        .onItem()
        .transform(res -> Person.from(res.item()));
  }

  public Uni<PaginationResponse<Person>> findByCpf(final String cpf,
      final PaginationRequest paginationRequest) {
    return Uni.createFrom()
        .completionStage(() -> dynamoDbAsyncClient.query(
            getQueryRequest(paginationRequest, cpf)))
        .onItem()
        .transform(res -> PaginationResponse.of(res.items().stream().map(Person::from).toList(),
            res.lastEvaluatedKey()));
  }

  public Uni<Person> add(final Person person) {
    return Uni.createFrom()
        .completionStage(() -> dynamoDbAsyncClient.putItem(putRequest(person)))
        .onItem()
        .ignore()
        .andSwitchTo(() -> findByFirstName(person.getFirstName()));
  }

  public Uni<Person> delete(final String firstName) {
    return Uni.createFrom()
        .completionStage(() -> dynamoDbAsyncClient.deleteItem(
            DeleteItemRequest.builder()
                .key(
                    Map.of(Person.FIRST_NAME_COLUMN, AttributeValue.builder().s(firstName).build()))
                .build()))
        .onItem()
        .ignore()
        .andSwitchTo(() -> findByFirstName(firstName));
  }

  public Uni<Person> update(final Person person) {
    return Uni.createFrom()
        .completionStage(() -> dynamoDbAsyncClient.updateItem(
            UpdateItemRequest.builder()
                .key(Map.of(Person.FIRST_NAME_COLUMN,
                    AttributeValue.builder().s(person.getFirstName()).build()))
                .build()))
        .onItem()
        .ignore()
        .andSwitchTo(() -> findByFirstName(person.getFirstName()));
  }

  private static ScanRequest scanRequest() {
    return ScanRequest.builder()
        .tableName(Person.TABLE_NAME)
        .build();
  }

  private GetItemRequest getRequest(final String firstName) {
    return GetItemRequest.builder()
        .tableName(Person.TABLE_NAME)
        .key(Map.of(Person.FIRST_NAME_COLUMN, AttributeValue.builder().s(firstName).build()))
        .build();
  }

  private GetItemRequest getRequest(final String firstName, final String lastName) {
    return GetItemRequest.builder()
        .tableName(Person.TABLE_NAME)
        .key(Map.of(
            Person.FIRST_NAME_COLUMN, AttributeValue.builder().s(firstName).build(),
            Person.LAST_NAME_COLUMN, AttributeValue.builder().s(lastName).build()))
        .build();
  }

  private PutItemRequest putRequest(final Person person) {
    return PutItemRequest.builder()
        .tableName(Person.TABLE_NAME)
        .item(person.toDynamodbAttributes())
        .build();
  }

  private static QueryRequest getQueryRequest(
      final PaginationRequest paginationRequest, final String cpf) {
    return QueryRequest.builder()
        .tableName(Person.TABLE_NAME)
        .keyConditions(Map.of(Person.CPF_COLUMN, getCondition(cpf)))
        .limit(paginationRequest.getLimit())
        .exclusiveStartKey(paginationRequest.getLastEvaluatedKey())
        .build();
  }

  private static Condition getCondition(final String cpf) {
    return Condition.builder()
        .comparisonOperator(ComparisonOperator.EQ)
        .attributeValueList(AttributeValue.builder().s(cpf).build())
        .build();
  }
}
