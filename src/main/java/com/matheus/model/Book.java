package com.matheus.model;

import java.util.Map;
import java.util.Objects;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public final class Book {

  public static final String BOOK_TABLE = "Book";
  public static final String ISBN_COLUMN = "Isbn";
  public static final String NAME_COLUMN = "Name";
  public static final String DESCRIPTION_COLUMN = "Description";

  private final String isbn;
  private final String name;
  private final String description;

  private Book(String isbn, String name, String description) {
    this.isbn = isbn;
    this.name = name;
    this.description = description;
  }

  public static Book from(Map<String, AttributeValue> item) {
    if (item == null || item.isEmpty()) {
      throw new IllegalArgumentException("Item is null or empty");
    }
    return new Book(item.get(ISBN_COLUMN).s(), item.get(NAME_COLUMN).s(),
        item.get(DESCRIPTION_COLUMN).s());
  }

  public String getIsbn() {
    return isbn;
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Book book = (Book) o;
    return isbn.equals(book.isbn) && name.equals(book.name) && Objects.equals(description,
        book.description);
  }

  @Override
  public int hashCode() {
    return Objects.hash(isbn, name, description);
  }

  @Override
  public String toString() {
    return "Book{" +
        "isbn='" + isbn + '\'' +
        ", name='" + name + '\'' +
        ", description='" + description + '\'' +
        '}';
  }
}
