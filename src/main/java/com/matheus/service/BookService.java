package com.matheus.service;

import com.matheus.model.Book;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.enterprise.context.ApplicationScoped;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;

@ApplicationScoped
public class BookService {

  private final DynamoDbClient dynamoDbClient;

  public BookService(DynamoDbClient dynamoDbClient) {
    this.dynamoDbClient = dynamoDbClient;
  }

  public List<Book> findAll() {
    ScanRequest scanRequest = ScanRequest.builder()
        .tableName(Book.BOOK_TABLE)
        .build();

    return dynamoDbClient.scanPaginator(scanRequest).items().stream()
        .map(Book::from)
        .toList();
  }

  public Book findByIsbn(final String isbn) {
    GetItemRequest getItemRequest = GetItemRequest.builder()
        .tableName(Book.BOOK_TABLE)
        .key(Map.of(Book.ISBN_COLUMN, AttributeValue.builder().s(isbn).build()))
        .build();

    return Book.from(dynamoDbClient.getItem(getItemRequest)
        .item());
  }

  public Book add(final Book book) {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put(Book.ISBN_COLUMN, AttributeValue.builder().s(book.getIsbn()).build());
    item.put(Book.NAME_COLUMN, AttributeValue.builder().s(book.getName()).build());
    item.put(Book.DESCRIPTION_COLUMN, AttributeValue.builder().s(book.getDescription()).build());

    PutItemRequest putItemRequest = PutItemRequest.builder()
        .tableName(Book.BOOK_TABLE)
        .item(item)
        .build();

    dynamoDbClient.putItem(putItemRequest);
    return book;
  }
}
