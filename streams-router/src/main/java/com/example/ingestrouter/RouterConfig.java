package com.example.ingestrouter;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Routing policy loaded from JSON (see {@link RouterConfigLoader}).
 */
public final class RouterConfig {

  private final String ingestTopic;
  private final String dlqTopic;
  private final String targetField;
  private final Map<String, String> routes;

  public RouterConfig(
      @JsonProperty("ingestTopic") String ingestTopic,
      @JsonProperty("dlqTopic") String dlqTopic,
      @JsonProperty("targetField") String targetField,
      @JsonProperty("routes") Map<String, String> routes) {
    this.ingestTopic = ingestTopic != null ? ingestTopic.trim() : null;
    this.dlqTopic = dlqTopic != null ? dlqTopic.trim() : null;
    this.targetField = targetField != null && !targetField.isBlank() ? targetField.trim() : "target";
    LinkedHashMap<String, String> r = new LinkedHashMap<>();
    if (routes != null) {
      for (Map.Entry<String, String> e : routes.entrySet()) {
        if (e.getKey() == null || e.getValue() == null) {
          continue;
        }
        String k = e.getKey().trim();
        String v = e.getValue().trim();
        if (!k.isEmpty() && !v.isEmpty()) {
          r.put(k, v);
        }
      }
    }
    this.routes = Collections.unmodifiableMap(r);
  }

  public String getIngestTopic() {
    return ingestTopic;
  }

  public String getDlqTopic() {
    return dlqTopic;
  }

  public String getTargetField() {
    return targetField;
  }

  public Map<String, String> getRoutes() {
    return routes;
  }

  @Override
  public String toString() {
    return "RouterConfig{ingestTopic='"
        + ingestTopic
        + "', dlqTopic='"
        + dlqTopic
        + "', targetField='"
        + targetField
        + "', routes="
        + routes.size()
        + " entries}";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RouterConfig that = (RouterConfig) o;
    return Objects.equals(ingestTopic, that.ingestTopic)
        && Objects.equals(dlqTopic, that.dlqTopic)
        && Objects.equals(targetField, that.targetField)
        && Objects.equals(routes, that.routes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(ingestTopic, dlqTopic, targetField, routes);
  }
}
