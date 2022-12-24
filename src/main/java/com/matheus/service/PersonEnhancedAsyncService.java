package com.matheus.service;

import com.matheus.model.PersonEnhanced;
import com.matheus.vo.request.PaginationRequest;
import com.matheus.vo.response.PaginationResponse;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import java.util.concurrent.CompletableFuture;
import javax.enterprise.context.ApplicationScoped;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbAsyncTable;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedAsyncClient;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.PagePublisher;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryEnhancedRequest;

@ApplicationScoped
public class PersonEnhancedAsyncService {

  private final DynamoDbEnhancedAsyncClient dynamoDbEnhancedAsyncClient;

  public PersonEnhancedAsyncService(DynamoDbEnhancedAsyncClient dynamoDbEnhancedAsyncClient) {
    this.dynamoDbEnhancedAsyncClient = dynamoDbEnhancedAsyncClient;
  }

  public Multi<PersonEnhanced> findAll() {
    return Multi.createFrom()
        .item(() -> dynamoDbEnhancedAsyncClient.table(
            PersonEnhanced.TABLE_NAME, TableSchema.fromBean(PersonEnhanced.class)))
        .map(DynamoDbAsyncTable::scan)
        .onItem()
        .transformToMultiAndConcatenate(PagePublisher::items);
  }

  public Uni<PaginationResponse<PersonEnhanced>> findByFirstName(final String firstName,
      final PaginationRequest paginationRequest) {
    return Uni.createFrom()
        .item(dynamoDbEnhancedAsyncClient.table(PersonEnhanced.TABLE_NAME,
            TableSchema.fromBean(PersonEnhanced.class)))
        .map(table -> table.query(QueryEnhancedRequest.builder()
            .queryConditional(
                QueryConditional.keyEqualTo(Key.builder().partitionValue(firstName).build()))
            .limit(paginationRequest.getLimit())
            .exclusiveStartKey(paginationRequest.getLastEvaluatedKey())
            .build()))
        .onItem()
        .transformToUni(publisher -> Uni.createFrom().publisher(publisher))
        .map(PaginationResponse::from);
  }

  public Uni<PersonEnhanced> findByFirstNameAndLastName(
      final String firstName, final String lastName) {
    return Uni.createFrom()
        .item(() -> dynamoDbEnhancedAsyncClient.table(PersonEnhanced.TABLE_NAME,
            TableSchema.fromBean(PersonEnhanced.class)))
        .map(table -> table.getItem(
            Key.builder().partitionValue(firstName).sortValue(lastName).build()))
        .onItem()
        .transform(CompletableFuture::join);
  }

  public Uni<PaginationResponse<PersonEnhanced>> findByCpf(
      final String cpf, final PaginationRequest paginationRequest) {
    return Uni.createFrom()
        .item(() -> dynamoDbEnhancedAsyncClient.table(PersonEnhanced.TABLE_NAME,
                TableSchema.fromBean(PersonEnhanced.class))
            .index(PersonEnhanced.CPF_INDEX_NAME))
        .map(table -> table.query(QueryEnhancedRequest.builder()
            .queryConditional(
                QueryConditional.keyEqualTo(Key.builder().partitionValue(cpf).build()))
            .limit(paginationRequest.getLimit())
            .exclusiveStartKey(paginationRequest.getLastEvaluatedKey())
            .build()))
        .onItem()
        .transformToUni(publisher -> Uni.createFrom().publisher(publisher))
        .map(PaginationResponse::from);
  }

  public Uni<PersonEnhanced> add(final PersonEnhanced person) {
    return Uni.createFrom()
        .item(() -> dynamoDbEnhancedAsyncClient.table(PersonEnhanced.TABLE_NAME,
            TableSchema.fromBean(PersonEnhanced.class)))
        .map(table -> table.putItem(person))
        .onItem()
        .transform(response -> person);
  }

  public Uni<PersonEnhanced> delete(final String firstName, final String lastName) {
    return Uni.createFrom()
        .item(() -> dynamoDbEnhancedAsyncClient.table(PersonEnhanced.TABLE_NAME,
            TableSchema.fromBean(PersonEnhanced.class)))
        .map(table -> table.deleteItem(Key.builder()
            .partitionValue(firstName)
                .sortValue(lastName)
            .build()))
        .onItem()
        .transform(CompletableFuture::join);
  }

  public Uni<PersonEnhanced> update(final PersonEnhanced person) {
    return Uni.createFrom()
        .item(() -> dynamoDbEnhancedAsyncClient.table(PersonEnhanced.TABLE_NAME,
            TableSchema.fromBean(PersonEnhanced.class)))
        .map(table -> table.updateItem(person))
        .onItem()
        .transform(CompletableFuture::join);
  }
}
