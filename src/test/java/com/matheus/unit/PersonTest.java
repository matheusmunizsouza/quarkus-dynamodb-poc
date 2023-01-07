package com.matheus.unit;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.matheus.model.Person;
import io.quarkus.test.junit.QuarkusTest;
import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

@QuarkusTest
class PersonTest {

  @Test
  @DisplayName("Should create a person successfully")
  void shouldCreatePersonSuccessfully() {
    Person personOf = Person.of("Name", "Lastname", "86679311033");

    Map<String, AttributeValue> map = Map.of(
        Person.FIRST_NAME_COLUMN, AttributeValue.builder().s("Name").build(),
        Person.LAST_NAME_COLUMN, AttributeValue.builder().s("Lastname").build(),
        Person.CPF_COLUMN, AttributeValue.builder().s("86679311033").build());

    Person personFromMap = Person.from(map);

    assertAll(
        () -> assertEquals("Name", personOf.getFirstName()),
        () -> assertEquals("Lastname", personOf.getLastName()),
        () -> assertEquals("86679311033", personOf.getCpf()),
        () -> assertEquals("Name", personFromMap.getFirstName()),
        () -> assertEquals("Lastname", personFromMap.getLastName()),
        () -> assertEquals("86679311033", personFromMap.getCpf()));
  }

  @Test
  @DisplayName("Should convert person to dynamodb attributes successfully")
  void shouldConvertPersonToDynamodbAttributesSuccessfully() {
    Person person = Person.of("Name", "Lastname", "86679311033");

    Map<String, AttributeValue> personAttributes = person.toDynamodbAttributes();

    assertAll(
        () -> assertNotNull(personAttributes),
        () -> assertEquals("Name", personAttributes.get(Person.FIRST_NAME_COLUMN).s()),
        () -> assertEquals("Lastname", personAttributes.get(Person.LAST_NAME_COLUMN).s()),
        () -> assertEquals("86679311033", personAttributes.get(Person.CPF_COLUMN).s()));
  }

  @Test
  @DisplayName("Should thrown exception when try to create a person with empty map")
  void shouldThrowExceptionWhenTryCreatePersonWithEmptyMap() {
    Map<String, AttributeValue> emptyMap = Collections.emptyMap();

    Assertions.assertThrows(IllegalArgumentException.class, () -> Person.from(emptyMap));
  }
}
