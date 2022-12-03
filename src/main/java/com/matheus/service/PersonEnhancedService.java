package com.matheus.service;

import com.matheus.model.PersonEnhanced;
import com.matheus.vo.request.PaginationRequest;
import com.matheus.vo.response.PaginationResponse;
import java.util.List;
import javax.enterprise.context.ApplicationScoped;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbIndex;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.Page;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryEnhancedRequest;

@ApplicationScoped
public class PersonEnhancedService {

  private final DynamoDbEnhancedClient dynamoDbEnhancedClient;

  public PersonEnhancedService(DynamoDbEnhancedClient dynamoDbEnhancedClient) {
    this.dynamoDbEnhancedClient = dynamoDbEnhancedClient;
  }

  public List<PersonEnhanced> findAll() {
    DynamoDbTable<PersonEnhanced> table = dynamoDbEnhancedClient.table(
        PersonEnhanced.TABLE_NAME, TableSchema.fromBean(PersonEnhanced.class));
    return table.scan().items().stream().toList();
  }

  public PersonEnhanced findByFirstName(final String firstName) {

    DynamoDbTable<PersonEnhanced> table = dynamoDbEnhancedClient.table(
        PersonEnhanced.TABLE_NAME,
        TableSchema.fromBean(PersonEnhanced.class));

    return table.getItem(Key.builder().partitionValue(firstName).build());
  }

  public PersonEnhanced findByFirstNameAndLastName(final String firstName, final String lastName) {
    DynamoDbTable<PersonEnhanced> table = dynamoDbEnhancedClient.table(
        PersonEnhanced.TABLE_NAME,
        TableSchema.fromBean(PersonEnhanced.class));

    return table.getItem(Key.builder().partitionValue(firstName).sortValue(lastName).build());
  }

  public PaginationResponse<PersonEnhanced> findByCpf(
      final String cpf,
      final PaginationRequest paginationRequest) {

    DynamoDbIndex<PersonEnhanced> index = dynamoDbEnhancedClient.table(
            PersonEnhanced.TABLE_NAME,
            TableSchema.fromBean(PersonEnhanced.class))
        .index(PersonEnhanced.CPF_INDEX_NAME);

    SdkIterable<Page<PersonEnhanced>> page = index.query(QueryEnhancedRequest.builder()
        .queryConditional(QueryConditional.keyEqualTo(Key.builder().partitionValue(cpf).build()))
        .limit(paginationRequest.getLimit())
        .exclusiveStartKey(paginationRequest.getLastEvaluatedKey())
        .build());

    return PaginationResponse.from(page.iterator().next());
  }

  public PersonEnhanced add(final PersonEnhanced person) {
    DynamoDbTable<PersonEnhanced> table = dynamoDbEnhancedClient.table(
        PersonEnhanced.TABLE_NAME,
        TableSchema.fromBean(PersonEnhanced.class));
    table.putItem(person);
    return person;
  }

  public PersonEnhanced delete(final String firstName) {
    DynamoDbTable<PersonEnhanced> table = dynamoDbEnhancedClient.table(
        PersonEnhanced.TABLE_NAME,
        TableSchema.fromBean(PersonEnhanced.class));
    return table.deleteItem(Key.builder().partitionValue(firstName).build());
  }

  public PersonEnhanced update(final PersonEnhanced person) {
    DynamoDbTable<PersonEnhanced> table = dynamoDbEnhancedClient.table(
        PersonEnhanced.TABLE_NAME,
        TableSchema.fromBean(PersonEnhanced.class));
    return table.updateItem(person);
  }
}
