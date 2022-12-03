package com.matheus.vo.request;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.QueryParam;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.utils.StringUtils;

public final class PaginationRequest {

  @QueryParam("limit")
  private Integer limit;
  @QueryParam("lastEvaluatedKey")
  private String lastEvaluatedKey;

  public int getLimit() {
    return limit == null ? 10 : limit;
  }

  public Map<String, AttributeValue> getLastEvaluatedKey() {
    if (!StringUtils.isEmpty(lastEvaluatedKey)) {
      HashMap<String, AttributeValue> keys = new HashMap<>();
      for (String keyValue : lastEvaluatedKey.split(",")) {
        String[] split = keyValue.split(":");
        keys.put(split[0], AttributeValue.builder().s(split[1]).build());
      }
      return keys;
    } else {
      return Collections.emptyMap();
    }
  }
}
