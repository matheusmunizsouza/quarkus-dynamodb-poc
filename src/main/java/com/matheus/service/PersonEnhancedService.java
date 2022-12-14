package com.matheus.service;

import com.matheus.model.PersonEnhanced;
import com.matheus.vo.request.DeletePeopleBatch;
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
import software.amazon.awssdk.enhanced.dynamodb.model.BatchWriteItemEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.DeleteItemEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.Page;
import software.amazon.awssdk.enhanced.dynamodb.model.PageIterable;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.ScanEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.WriteBatch;
import software.amazon.awssdk.enhanced.dynamodb.model.WriteBatch.Builder;

@ApplicationScoped
public class PersonEnhancedService {

  private final DynamoDbEnhancedClient dynamoDbEnhancedClient;

  public PersonEnhancedService(DynamoDbEnhancedClient dynamoDbEnhancedClient) {
    this.dynamoDbEnhancedClient = dynamoDbEnhancedClient;
  }

  public PaginationResponse<PersonEnhanced> findAll(PaginationRequest paginationRequest) {
    DynamoDbTable<PersonEnhanced> table = dynamoDbEnhancedClient.table(
        PersonEnhanced.TABLE_NAME, TableSchema.fromBean(PersonEnhanced.class));

    ScanEnhancedRequest scanEnhancedRequest = ScanEnhancedRequest.builder()
        .limit(paginationRequest.getLimit())
        .exclusiveStartKey(paginationRequest.getLastEvaluatedKey())
        .build();

    PageIterable<PersonEnhanced> page = table.scan(scanEnhancedRequest);

    return PaginationResponse.from(page);
  }

  public PaginationResponse<PersonEnhanced> findByFirstName(final String firstName,
      final PaginationRequest paginationRequest) {

    DynamoDbTable<PersonEnhanced> table = dynamoDbEnhancedClient.table(
        PersonEnhanced.TABLE_NAME,
        TableSchema.fromBean(PersonEnhanced.class));

    PageIterable<PersonEnhanced> pages = table.query(QueryEnhancedRequest.builder()
        .queryConditional(
            QueryConditional.keyEqualTo(Key.builder().partitionValue(firstName).build()))
        .limit(paginationRequest.getLimit())
        .exclusiveStartKey(paginationRequest.getLastEvaluatedKey())
        .build());

    return PaginationResponse.from(pages.iterator().next());
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

  public PersonEnhanced delete(final String firstName, final String lastName) {
    DynamoDbTable<PersonEnhanced> table = dynamoDbEnhancedClient.table(
        PersonEnhanced.TABLE_NAME,
        TableSchema.fromBean(PersonEnhanced.class));

    DeleteItemEnhancedRequest deleteItemEnhancedRequest = DeleteItemEnhancedRequest.builder()
        .key(builder -> builder
            .partitionValue(firstName)
            .sortValue(lastName)
            .build())
        .build();

    return table.deleteItem(deleteItemEnhancedRequest);
  }

  public PersonEnhanced update(final PersonEnhanced person) {
    DynamoDbTable<PersonEnhanced> table = dynamoDbEnhancedClient.table(
        PersonEnhanced.TABLE_NAME,
        TableSchema.fromBean(PersonEnhanced.class));
    return table.updateItem(person);
  }

  public void putPeople(final List<PersonEnhanced> people) {
    DynamoDbTable<PersonEnhanced> table = dynamoDbEnhancedClient.table(
        PersonEnhanced.TABLE_NAME, TableSchema.fromBean(PersonEnhanced.class));

    Builder<PersonEnhanced> writeBatchBuilder = WriteBatch.builder(PersonEnhanced.class);

    people.forEach(writeBatchBuilder::addPutItem);

    WriteBatch writeBatch = writeBatchBuilder
        .mappedTableResource(table)
        .build();

    dynamoDbEnhancedClient.batchWriteItem(
        BatchWriteItemEnhancedRequest.builder()
            .addWriteBatch(writeBatch)
            .build());
  }

  public void deletePeople(final List<DeletePeopleBatch> deletePeopleBatches) {
    DynamoDbTable<PersonEnhanced> table = dynamoDbEnhancedClient.table(
        PersonEnhanced.TABLE_NAME, TableSchema.fromBean(PersonEnhanced.class));

    Builder<PersonEnhanced> writeBatchBuilder = WriteBatch.builder(PersonEnhanced.class);

    deletePeopleBatches.forEach(deletePeopleBatch -> writeBatchBuilder.addDeleteItem(Key.builder()
        .partitionValue(deletePeopleBatch.firstName())
        .sortValue(deletePeopleBatch.lastName())
        .build()));

    WriteBatch writeBatch = writeBatchBuilder
        .mappedTableResource(table)
        .build();

    dynamoDbEnhancedClient.batchWriteItem(
        BatchWriteItemEnhancedRequest.builder()
            .addWriteBatch(writeBatch)
            .build());
  }
}
