package com.matheus.unit;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.matheus.vo.request.PaginationRequest;
import io.quarkus.test.junit.QuarkusTest;
import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

@QuarkusTest
class PaginationRequestTest {

  @Test
  @DisplayName("Should create pagination request successfully")
  void shouldCreatePaginationRequestSuccessfully() {
    ObjectMapper objectMapper = new ObjectMapper();

    PaginationRequest EmptyPaginationRequest = objectMapper.convertValue(
        Collections.EMPTY_MAP, PaginationRequest.class);

    Map<String, Integer> limit = Map.of("limit", 5);
    PaginationRequest paginationRequestWithLimit = objectMapper.convertValue(limit,
        PaginationRequest.class);

    Map<String, String> limitAndLastKey = Map.of(
        "limit", "2",
        "lastEvaluatedKey", "firstName:Person5,lastName:lastNameTest");
    PaginationRequest paginationRequestWithLimitAndLastKey = objectMapper.convertValue(
        limitAndLastKey, PaginationRequest.class);

    assertAll(
        () -> assertEquals(10, EmptyPaginationRequest.getLimit()),
        () -> assertNull(EmptyPaginationRequest.getLastEvaluatedKey()),
        () -> assertEquals(5, paginationRequestWithLimit.getLimit()),
        () -> assertNull(paginationRequestWithLimit.getLastEvaluatedKey()),
        () -> assertEquals(2, paginationRequestWithLimitAndLastKey.getLimit()),
        () -> assertEquals(Map.of(
                "firstName", AttributeValue.builder().s("Person5").build(),
                "lastName", AttributeValue.builder().s("lastNameTest").build()),
            paginationRequestWithLimitAndLastKey.getLastEvaluatedKey()));
  }
}
