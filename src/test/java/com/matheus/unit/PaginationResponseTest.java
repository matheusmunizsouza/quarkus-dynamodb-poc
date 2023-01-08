package com.matheus.unit;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.matheus.model.Person;
import com.matheus.vo.response.PaginationResponse;
import io.quarkus.test.junit.QuarkusTest;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.enhanced.dynamodb.model.Page;
import software.amazon.awssdk.enhanced.dynamodb.model.PageIterable;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

@QuarkusTest
class PaginationResponseTest {

  @Test
  @DisplayName("Should create pagination response successfully")
  void shouldCreatePaginationResponseSuccessfully() {
    Person person = Person.of("Name", "Lastname", "86679311033");
    List<Person> people = List.of(person);
    PaginationResponse<Person> paginationOf = PaginationResponse.of(people,
        Map.of());

    Map<String, AttributeValue> lastKey = Map.of(
        Person.FIRST_NAME_COLUMN, AttributeValue.builder().s("Name").build(),
        Person.LAST_NAME_COLUMN, AttributeValue.builder().s("Lastname").build());
    PaginationResponse<Person> paginationOfWithLastKey = PaginationResponse.of(people, lastKey);

    Page<Person> personPage = Page.create(people);
    PaginationResponse<Person> paginationFromPage = PaginationResponse.from(personPage);

    Page<Person> personPageWithLastKey = Page.create(people, lastKey);
    PaginationResponse<Person> paginationFromPageWithLastKey = PaginationResponse.from(
        personPageWithLastKey);

    PageIterable<Person> pageIterable = PageIterable.create(() -> getIterator(personPage));
    PaginationResponse<Person> paginationFromPageIterable = PaginationResponse.from(pageIterable);

    PageIterable<Person> pageWithLastKeyIterable = PageIterable.create(
        () -> getIterator(personPageWithLastKey));
    PaginationResponse<Person> paginationFromPageIterableWithLastKey = PaginationResponse.from(
        pageWithLastKeyIterable);

    assertAll(
        () -> assertEquals(people, paginationOf.getItems()),
        () -> assertEquals(1, paginationOf.getSize()),
        () -> assertEquals(Map.of(), paginationOf.getLastEvaluatedKey()),
        () -> assertEquals(people, paginationOfWithLastKey.getItems()),
        () -> assertEquals(1, paginationOfWithLastKey.getSize()),
        () -> assertEquals(Map.of(
                "firstName", "Name",
                "lastName", "Lastname"),
            paginationOfWithLastKey.getLastEvaluatedKey()),
        () -> assertEquals(people, paginationFromPage.getItems()),
        () -> assertEquals(1, paginationFromPage.getSize()),
        () -> assertEquals(Map.of(), paginationFromPage.getLastEvaluatedKey()),
        () -> assertEquals(people, paginationFromPageWithLastKey.getItems()),
        () -> assertEquals(1, paginationFromPageWithLastKey.getSize()),
        () -> assertEquals(Map.of(
                "firstName", "Name",
                "lastName", "Lastname")
            , paginationFromPageWithLastKey.getLastEvaluatedKey()),
        () -> assertEquals(people, paginationFromPageIterable.getItems()),
        () -> assertEquals(1, paginationFromPageIterable.getSize()),
        () -> assertEquals(Map.of(), paginationFromPageIterable.getLastEvaluatedKey()),
        () -> assertEquals(people, paginationFromPageIterableWithLastKey.getItems()),
        () -> assertEquals(1, paginationFromPageIterableWithLastKey.getSize()),
        () -> assertEquals(Map.of(
                "firstName", "Name",
                "lastName", "Lastname")
            , paginationFromPageIterableWithLastKey.getLastEvaluatedKey()));
  }

  private Iterator<Page<Person>> getIterator(Page<Person> personPage) {
    return new Iterator<>() {
      @Override
      public boolean hasNext() {
        return false;
      }

      @Override
      public Page<Person> next() {
        return personPage;
      }
    };
  }
}
