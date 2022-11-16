package com.matheus.service;

import com.matheus.model.Book;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import java.util.HashMap;
import java.util.Map;
import javax.enterprise.context.ApplicationScoped;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;

@ApplicationScoped
public class BookAsyncService {

  private final DynamoDbAsyncClient dynamoDbAsyncClient;

  public BookAsyncService(DynamoDbAsyncClient dynamoDbAsyncClient) {
    this.dynamoDbAsyncClient = dynamoDbAsyncClient;
  }

  public Multi<Book> findAll() {
    return Multi.createFrom()
        .completionStage(() -> dynamoDbAsyncClient.scan(scanRequest()))
        .onItem()
        .transformToIterable(res -> res.items().stream().map(Book::from).toList());
  }

  public Uni<Book> findByIsbn(final String isbn) {
    return Uni.createFrom()
        .completionStage(() -> dynamoDbAsyncClient.getItem(getRequest(isbn)))
        .onItem()
        .transform(res -> Book.from(res.item()));
  }

  public Uni<Book> add(final Book book) {
    return Uni.createFrom()
        .completionStage(() -> dynamoDbAsyncClient.putItem(putRequest(book)))
        .onItem()
        .ignore()
        .andSwitchTo(() -> findByIsbn(book.getIsbn()));
  }

  private static ScanRequest scanRequest() {
    return ScanRequest.builder()
        .tableName(Book.BOOK_TABLE)
        .build();
  }

  private GetItemRequest getRequest(final String isbn) {
    return GetItemRequest.builder()
        .tableName(Book.BOOK_TABLE)
        .key(Map.of(Book.ISBN_COLUMN, AttributeValue.builder().s(isbn).build()))
        .build();
  }

  private PutItemRequest putRequest(final Book book) {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put(Book.ISBN_COLUMN, AttributeValue.builder().s(book.getIsbn()).build());
    item.put(Book.NAME_COLUMN, AttributeValue.builder().s(book.getName()).build());
    item.put(Book.DESCRIPTION_COLUMN, AttributeValue.builder().s(book.getDescription()).build());

    return PutItemRequest.builder()
        .tableName(Book.BOOK_TABLE)
        .item(item)
        .build();
  }
}
