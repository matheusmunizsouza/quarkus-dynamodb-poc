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

  public PersonEnhanced() {
  }

  public PersonEnhanced(String firstName, String lastName, String cpf) {
    this.firstName = firstName;
    this.lastName = lastName;
    this.cpf = cpf;
  }

  public static PersonEnhanced of(String firstName, String lastName, String cpf) {
    return new PersonEnhanced(firstName, lastName, cpf);
  }

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
}
