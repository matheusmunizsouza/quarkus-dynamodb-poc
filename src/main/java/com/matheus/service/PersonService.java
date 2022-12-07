package com.matheus.service;

import com.matheus.model.Person;
import com.matheus.vo.request.PaginationRequest;
import com.matheus.vo.response.PaginationResponse;
import java.util.List;
import java.util.Map;
import javax.enterprise.context.ApplicationScoped;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ComparisonOperator;
import software.amazon.awssdk.services.dynamodb.model.Condition;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

@ApplicationScoped
public class PersonService {

  private final DynamoDbClient dynamoDbClient;

  public PersonService(DynamoDbClient dynamoDbClient) {
    this.dynamoDbClient = dynamoDbClient;
  }

  public List<Person> findAll() {
    ScanRequest scanRequest = ScanRequest.builder()
        .tableName(Person.TABLE_NAME)
        .build();

    return dynamoDbClient.scanPaginator(scanRequest).items().stream()
        .map(Person::from)
        .toList();
  }

  public PaginationResponse<Person> findByFirstName(final String firstName,
      final PaginationRequest paginationRequest) {
    Condition condition = Condition.builder()
        .comparisonOperator(ComparisonOperator.EQ)
        .attributeValueList(AttributeValue.builder().s(firstName).build())
        .build();

    QueryRequest queryRequest = QueryRequest.builder()
        .tableName(Person.TABLE_NAME)
        .keyConditions(Map.of(Person.FIRST_NAME_COLUMN, condition))
        .limit(paginationRequest.getLimit())
        .exclusiveStartKey(paginationRequest.getLastEvaluatedKey())
        .build();

    QueryResponse queryResponse = dynamoDbClient.query(queryRequest);

    return PaginationResponse.of(queryResponse.items().stream().map(Person::from).toList(),
        queryResponse.lastEvaluatedKey());
  }

  public Person findByFirstNameAndLastName(final String firstName, final String lastName) {
    GetItemRequest getItemRequest = GetItemRequest.builder()
        .tableName(Person.TABLE_NAME)
        .key(Map.of(
            Person.FIRST_NAME_COLUMN, AttributeValue.builder().s(firstName).build(),
            Person.LAST_NAME_COLUMN, AttributeValue.builder().s(lastName).build()))
        .build();

    return Person.from(dynamoDbClient.getItem(getItemRequest).item());
  }

  public PaginationResponse<Person> findByCpf(
      final String cpf,
      final PaginationRequest paginationRequest) {

    Condition condition = Condition.builder()
        .comparisonOperator(ComparisonOperator.EQ)
        .attributeValueList(AttributeValue.builder().s(cpf).build())
        .build();

    QueryRequest queryRequest = QueryRequest.builder()
        .tableName(Person.TABLE_NAME)
        .keyConditions(Map.of(Person.CPF_COLUMN, condition))
        .limit(paginationRequest.getLimit())
        .exclusiveStartKey(paginationRequest.getLastEvaluatedKey())
        .build();

    QueryResponse queryResponse = dynamoDbClient.query(queryRequest);

    return PaginationResponse.of(queryResponse.items().stream().map(Person::from).toList(),
        queryResponse.lastEvaluatedKey());
  }

  public Person add(final Person person) {
    PutItemRequest putItemRequest = PutItemRequest.builder()
        .tableName(Person.TABLE_NAME)
        .item(person.toDynamodbAttributes())
        .build();

    dynamoDbClient.putItem(putItemRequest);
    return person;
  }

  public Person delete(final String firstName) {
    DeleteItemRequest deleteItemRequest = DeleteItemRequest.builder()
        .key(Map.of(Person.FIRST_NAME_COLUMN, AttributeValue.builder().s(firstName).build()))
        .build();

    return Person.from(dynamoDbClient.deleteItem(deleteItemRequest).attributes());
  }

  public Person update(final Person person) {
    UpdateItemRequest updateItemRequest = UpdateItemRequest.builder()
        .key(Map.of(Person.FIRST_NAME_COLUMN,
            AttributeValue.builder().s(person.getFirstName()).build()))
        .build();

    dynamoDbClient.updateItem(updateItemRequest);
    return person;
  }
}
