package com.example.ingestrouter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Resolves outbound Kafka topic from a JSON payload using {@link RouterConfig#routes}.
 */
final class RouteResolver {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final RouterConfig config;

  RouteResolver(RouterConfig config) {
    this.config = config;
  }

  boolean hasRoutablePayload(String json) {
    return resolveTopic(json) != null;
  }

  /**
   * @return Kafka topic name from configured routes, or null if invalid / unknown key
   */
  String resolveTopic(String json) {
    if (json == null || json.isBlank()) {
      return null;
    }
    try {
      JsonNode root = MAPPER.readTree(json);
      JsonNode t = root.get(config.getTargetField());
      if (t == null || !t.isTextual()) {
        return null;
      }
      String key = t.asText().trim();
      if (key.isEmpty()) {
        return null;
      }
      String topic = config.getRoutes().get(key);
      return topic != null ? topic : null;
    } catch (Exception ignored) {
      return null;
    }
  }
}
