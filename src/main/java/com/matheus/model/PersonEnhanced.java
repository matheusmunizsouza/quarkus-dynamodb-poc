package com.matheus.model;

import java.util.Objects;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbAttribute;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSecondaryPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSortKey;

@DynamoDbBean
public final class PersonEnhanced {

  public static final String TABLE_NAME = "person";
  public static final String CPF_INDEX_NAME = "cpf_index";
  private String firstName;
  private String lastName;
  private String cpf;

  @DynamoDbPartitionKey
  @DynamoDbAttribute("firstName")
  public String getFirstName() {
    return firstName;
  }

  public void setFirstName(String firstName) {
    this.firstName = firstName;
  }

  @DynamoDbSortKey
  @DynamoDbAttribute("lastName")
  public String getLastName() {
    return lastName;
  }

  public void setLastName(String lastName) {
    this.lastName = lastName;
  }

  @DynamoDbSecondaryPartitionKey(indexNames = "cpf_index")
  @DynamoDbAttribute("cpf")
  public String getCpf() {
    return cpf;
  }

  public void setCpf(String cpf) {
    this.cpf = cpf;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PersonEnhanced personEnhanced = (PersonEnhanced) o;
    return firstName.equals(personEnhanced.firstName) && lastName.equals(personEnhanced.lastName)
        && Objects.equals(cpf, personEnhanced.cpf);
  }

  @Override
  public int hashCode() {
    return Objects.hash(firstName, lastName, cpf);
  }
}
