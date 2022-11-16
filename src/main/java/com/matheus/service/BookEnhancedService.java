package com.matheus.service;

import com.matheus.model.BookEnhanced;
import java.util.List;
import javax.enterprise.context.ApplicationScoped;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;

@ApplicationScoped
public class BookEnhancedService {

  private final DynamoDbEnhancedClient dynamoDbEnhancedClient;

  public BookEnhancedService(DynamoDbEnhancedClient dynamoDbEnhancedClient) {
    this.dynamoDbEnhancedClient = dynamoDbEnhancedClient;
  }

  public List<BookEnhanced> findAll() {
    DynamoDbTable<BookEnhanced> bookTable = dynamoDbEnhancedClient.table(BookEnhanced.BOOK_TABLE,
        TableSchema.fromBean(BookEnhanced.class));
    return bookTable.scan().items().stream().toList();
  }

  public BookEnhanced findByIsbn(final String isbn) {
    DynamoDbTable<BookEnhanced> bookTable = dynamoDbEnhancedClient.table(BookEnhanced.BOOK_TABLE,
        TableSchema.fromBean(BookEnhanced.class));
    return bookTable.getItem(Key.builder().partitionValue(isbn).build());
  }

  public BookEnhanced add(final BookEnhanced book) {
    DynamoDbTable<BookEnhanced> bookTable = dynamoDbEnhancedClient.table(BookEnhanced.BOOK_TABLE,
        TableSchema.fromBean(BookEnhanced.class));
    bookTable.putItem(book);
    return book;
  }
}
