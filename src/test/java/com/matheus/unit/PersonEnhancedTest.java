package com.matheus.unit;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.matheus.model.PersonEnhanced;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@QuarkusTest
class PersonEnhancedTest {

  @Test
  @DisplayName("Should create a person successfully")
  void shouldCreatePersonSuccessfully() {
    PersonEnhanced personOf = PersonEnhanced.of("Name", "Lastname", "86679311033");

    PersonEnhanced personDefaultConstructor = new PersonEnhanced();
    personDefaultConstructor.setFirstName("Name");
    personDefaultConstructor.setLastName("Lastname");
    personDefaultConstructor.setCpf("86679311033");

    assertAll(
        () -> assertEquals("Name", personOf.getFirstName()),
        () -> assertEquals("Lastname", personOf.getLastName()),
        () -> assertEquals("86679311033", personOf.getCpf()),
        () -> assertEquals("Name", personDefaultConstructor.getFirstName()),
        () -> assertEquals("Lastname", personDefaultConstructor.getLastName()),
        () -> assertEquals("86679311033", personDefaultConstructor.getCpf()));
  }
}
