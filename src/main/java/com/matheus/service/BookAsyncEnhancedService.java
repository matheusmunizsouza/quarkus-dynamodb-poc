package com.matheus.service;

import com.matheus.model.BookEnhanced;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import java.util.concurrent.CompletableFuture;
import javax.enterprise.context.ApplicationScoped;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbAsyncTable;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedAsyncClient;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.PagePublisher;

@ApplicationScoped
public class BookAsyncEnhancedService {

  private final DynamoDbEnhancedAsyncClient dynamoDbEnhancedAsyncClient;

  public BookAsyncEnhancedService(DynamoDbEnhancedAsyncClient dynamoDbEnhancedAsyncClient) {
    this.dynamoDbEnhancedAsyncClient = dynamoDbEnhancedAsyncClient;
  }

  public Multi<BookEnhanced> findAll() {
    return Multi.createFrom()
        .item(() -> dynamoDbEnhancedAsyncClient.table(
            BookEnhanced.BOOK_TABLE, TableSchema.fromBean(BookEnhanced.class)))
        .map(DynamoDbAsyncTable::scan)
        .onItem()
        .transformToMultiAndConcatenate(PagePublisher::items);
  }

  public Uni<BookEnhanced> findByIsbn(final String isbn) {
    return Uni.createFrom()
        .item(() -> dynamoDbEnhancedAsyncClient.table(BookEnhanced.BOOK_TABLE,
            TableSchema.fromBean(BookEnhanced.class)))
        .map(table -> table.getItem(Key.builder().partitionValue(isbn).build()))
        .onItem()
        .transform(CompletableFuture::join);
  }

  public Uni<BookEnhanced> add(final BookEnhanced book) {
    return Uni.createFrom()
        .item(() -> dynamoDbEnhancedAsyncClient.table(BookEnhanced.BOOK_TABLE,
            TableSchema.fromBean(BookEnhanced.class)))
        .map(table -> table.putItem(book))
        .onItem()
        .ignore()
        .andSwitchTo(() -> findByIsbn(book.getIsbn()));
  }
}
