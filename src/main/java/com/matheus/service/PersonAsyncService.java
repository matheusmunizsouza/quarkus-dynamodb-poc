package com.matheus.service;

import com.matheus.model.Person;
import com.matheus.vo.request.PaginationRequest;
import com.matheus.vo.response.PaginationResponse;
import io.smallrye.mutiny.Uni;
import java.util.List;
import java.util.Map;
import javax.enterprise.context.ApplicationScoped;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ComparisonOperator;
import software.amazon.awssdk.services.dynamodb.model.Condition;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

@ApplicationScoped
public class PersonAsyncService {

  private final DynamoDbAsyncClient dynamoDbAsyncClient;

  public PersonAsyncService(DynamoDbAsyncClient dynamoDbAsyncClient) {
    this.dynamoDbAsyncClient = dynamoDbAsyncClient;
  }

  public Uni<PaginationResponse<Person>> findAll(final PaginationRequest paginationRequest) {
    return Uni.createFrom()
        .publisher(dynamoDbAsyncClient.scanPaginator(scanRequest(paginationRequest)))
        .onItem()
        .transform(res -> PaginationResponse.of(res.items().stream().map(Person::from).toList(),
            res.lastEvaluatedKey()));
  }

  public Uni<PaginationResponse<Person>> findByFirstName(final String firstName,
      final PaginationRequest paginationRequest) {

    return Uni.createFrom()
        .completionStage(dynamoDbAsyncClient.query(
            getFindByFirstNameQueryRequest(firstName, paginationRequest)))
        .onItem()
        .transform(
            response -> PaginationResponse.of(response.items().stream().map(Person::from).toList(),
                response.lastEvaluatedKey()));
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
            getFindByCpfQueryRequest(paginationRequest, cpf)))
        .onItem()
        .transform(res -> PaginationResponse.of(res.items().stream().map(Person::from).toList(),
            res.lastEvaluatedKey()));
  }

  public Uni<Person> add(final Person person) {
    return Uni.createFrom()
        .completionStage(() -> dynamoDbAsyncClient.putItem(putRequest(person)))
        .onItem()
        .transform(response -> person);
  }

  public Uni<Person> delete(final String firstName, final String lastName) {
    return Uni.createFrom()
        .completionStage(
            () -> dynamoDbAsyncClient.deleteItem(getDeleteItemRequest(firstName, lastName)))
        .onItem()
        .transform(response -> Person.from(response.attributes()));
  }

  public Uni<Person> update(final Person person) {
    return Uni.createFrom()
        .completionStage(() -> dynamoDbAsyncClient.updateItem(getUpdateItemRequest(person)))
        .onItem()
        .transform(response -> Person.from(response.attributes()));
  }

  public Uni<Void> putBatch(final List<Person> people) {
    return Uni.createFrom()
        .completionStage(dynamoDbAsyncClient.batchWriteItem(
            getBatchWriteItemRequest(getWriteRequests(people))))
        .onItem()
        .ignore()
        .andContinueWithNull();
  }

  private static ScanRequest scanRequest(PaginationRequest paginationRequest) {
    return ScanRequest.builder()
        .tableName(Person.TABLE_NAME)
        .limit(paginationRequest.getLimit())
        .exclusiveStartKey(paginationRequest.getLastEvaluatedKey())
        .build();
  }

  private QueryRequest getFindByFirstNameQueryRequest(String firstName,
      PaginationRequest paginationRequest) {
    return QueryRequest.builder()
        .tableName(Person.TABLE_NAME)
        .keyConditions(Map.of(Person.FIRST_NAME_COLUMN, getFindByFirstNameCondition(firstName)))
        .limit(paginationRequest.getLimit())
        .exclusiveStartKey(paginationRequest.getLastEvaluatedKey())
        .build();
  }

  private Condition getFindByFirstNameCondition(String firstName) {
    return Condition.builder()
        .comparisonOperator(ComparisonOperator.EQ)
        .attributeValueList(AttributeValue.builder().s(firstName).build())
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

  private static QueryRequest getFindByCpfQueryRequest(
      final PaginationRequest paginationRequest, final String cpf) {
    return QueryRequest.builder()
        .tableName(Person.TABLE_NAME)
        .keyConditions(Map.of(Person.CPF_COLUMN, getFindByCpfCondition(cpf)))
        .limit(paginationRequest.getLimit())
        .indexName(Person.CPF_INDEX)
        .exclusiveStartKey(paginationRequest.getLastEvaluatedKey())
        .build();
  }

  private static Condition getFindByCpfCondition(final String cpf) {
    return Condition.builder()
        .comparisonOperator(ComparisonOperator.EQ)
        .attributeValueList(AttributeValue.builder().s(cpf).build())
        .build();
  }

  private PutItemRequest putRequest(final Person person) {
    return PutItemRequest.builder()
        .tableName(Person.TABLE_NAME)
        .item(person.toDynamodbAttributes())
        .build();
  }

  private DeleteItemRequest getDeleteItemRequest(String firstName, String lastName) {
    return DeleteItemRequest.builder()
        .tableName(Person.TABLE_NAME)
        .key(Map.of(
            Person.FIRST_NAME_COLUMN, AttributeValue.builder().s(firstName).build(),
            Person.LAST_NAME_COLUMN, AttributeValue.builder().s(lastName).build()))
        .returnValues(ReturnValue.ALL_OLD)
        .build();
  }

  private static UpdateItemRequest getUpdateItemRequest(Person person) {
    return UpdateItemRequest.builder()
        .tableName(Person.TABLE_NAME)
        .key(Map.of(
            Person.FIRST_NAME_COLUMN, AttributeValue.builder().s(person.getFirstName()).build(),
            Person.LAST_NAME_COLUMN, AttributeValue.builder().s(person.getLastName()).build()))
        .updateExpression("SET cpf = :newValue")
        .expressionAttributeValues(
            Map.of(":newValue", AttributeValue.builder().s(person.getCpf()).build()))
        .returnValues(ReturnValue.ALL_NEW)
        .build();
  }

  private static List<WriteRequest> getWriteRequests(List<Person> people) {
    return people.stream()
        .map(person -> PutRequest.builder()
            .item(person.toDynamodbAttributes())
            .build())
        .map(putRequest -> WriteRequest.builder()
            .putRequest(putRequest)
            .build())
        .toList();
  }

  private static BatchWriteItemRequest getBatchWriteItemRequest(List<WriteRequest> writeRequests) {
    return BatchWriteItemRequest.builder()
        .requestItems(Map.of(Person.TABLE_NAME, writeRequests))
        .build();
  }
}
