package com.matheus.model;

import java.util.Map;
import java.util.Objects;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public final class Person {

  public static final String TABLE_NAME = "person";
  public static final String FIRST_NAME_COLUMN = "firstName";
  public static final String LAST_NAME_COLUMN = "lastName";
  public static final String CPF_COLUMN = "cpf";

  private final String firstName;
  private final String lastName;
  private final String cpf;

  private Person(String firstName, String lastName, String cpf) {
    this.firstName = firstName;
    this.lastName = lastName;
    this.cpf = cpf;
  }

  public static Person from(Map<String, AttributeValue> item) {
    if (item == null || item.isEmpty()) {
      throw new IllegalArgumentException("Item is null or empty");
    }
    return new Person(item.get(FIRST_NAME_COLUMN).s(), item.get(LAST_NAME_COLUMN).s(),
        item.get(CPF_COLUMN).s());
  }

  public Map<String, AttributeValue> toDynamodbAttributes() {
    return Map.of(
        Person.FIRST_NAME_COLUMN, AttributeValue.builder().s(this.firstName).build(),
        Person.LAST_NAME_COLUMN, AttributeValue.builder().s(this.lastName).build(),
        Person.CPF_COLUMN, AttributeValue.builder().s(this.cpf).build());
  }

  public String getFirstName() {
    return firstName;
  }

  public String getLastName() {
    return lastName;
  }

  public String getCpf() {
    return cpf;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Person person = (Person) o;
    return firstName.equals(person.firstName) && lastName.equals(person.lastName) && Objects.equals(
        cpf,
        person.cpf);
  }

  @Override
  public int hashCode() {
    return Objects.hash(firstName, lastName, cpf);
  }

  @Override
  public String toString() {
    return "Person{" +
        "firstName='" + firstName + '\'' +
        ", lastName='" + lastName + '\'' +
        ", cpf='" + cpf + '\'' +
        '}';
  }
}
