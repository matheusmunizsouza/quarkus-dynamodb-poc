package com.matheus.vo.response;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import software.amazon.awssdk.enhanced.dynamodb.model.Page;
import software.amazon.awssdk.enhanced.dynamodb.model.PageIterable;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public final class PaginationResponse<T> {

  private final List<T> items;
  private final int size;
  private final Map<String, String> lastEvaluatedKey;

  private PaginationResponse(final List<T> items, final int size,
      final Map<String, String> lastEvaluatedKey) {
    this.items = items;
    this.size = size;
    this.lastEvaluatedKey = lastEvaluatedKey;
  }

  public static <T> PaginationResponse<T> from(final Page<T> page) {
    return new PaginationResponse<>(page.items(), page.items().size(),
        transformLastEvaluatedKey(page.lastEvaluatedKey()));
  }

  public static <T> PaginationResponse<T> from(final PageIterable<T> page) {
    List<T> items = page.iterator().next().items().stream().toList();
    return new PaginationResponse<>(items, items.size(),
        transformLastEvaluatedKey(page.iterator().next().lastEvaluatedKey()));
  }

  public static <T> PaginationResponse<T> of(final List<T> items,
      final Map<String, AttributeValue> lastEvaluatedKey) {
    return new PaginationResponse<>(items, items.size(),
        transformLastEvaluatedKey(lastEvaluatedKey));
  }

  private static HashMap<String, String> transformLastEvaluatedKey(
      final Map<String, AttributeValue> lastEvaluatedKey) {
    HashMap<String, String> keys = new HashMap<>();
    if (lastEvaluatedKey != null) {
      lastEvaluatedKey.forEach((key, attributeValue) -> keys.put(key, attributeValue.s()));
    }
    return keys;
  }

  public List<T> getItems() {
    return items;
  }

  public int getSize() {
    return size;
  }

  public Map<String, String> getLastEvaluatedKey() {
    return lastEvaluatedKey;
  }
}
