package com.example.headerrouter;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.regex.Pattern;

final class RouterConfigLoader {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Pattern TOPIC_NAME = Pattern.compile("[a-zA-Z0-9._-]+");
  private static final int MAX_TOPIC_LEN = 249;
  private static final int MAX_ROUTE_KEY_LEN = 256;
  private static final int MAX_HEADER_NAME_LEN = 256;

  private RouterConfigLoader() {}

  static RouterConfig load(Path path) throws IOException {
    if (!Files.isRegularFile(path)) {
      throw new IllegalStateException("Router config file not found: " + path.toAbsolutePath());
    }
    RouterConfig config = MAPPER.readValue(path.toFile(), RouterConfig.class);
    validate(config);
    return config;
  }

  static void validate(RouterConfig config) {
    if (config.getIngestTopic() == null || config.getIngestTopic().isBlank()) {
      throw new IllegalStateException("router config: ingestTopic is required");
    }
    if (config.getDlqTopic() == null || config.getDlqTopic().isBlank()) {
      throw new IllegalStateException("router config: dlqTopic is required");
    }
    requireValidKafkaTopicName("ingestTopic", config.getIngestTopic());
    requireValidKafkaTopicName("dlqTopic", config.getDlqTopic());

    String header = config.getTargetHeader();
    if (header == null || header.isBlank()) {
      throw new IllegalStateException("router config: targetHeader must be non-blank");
    }
    if (header.length() > MAX_HEADER_NAME_LEN) {
      throw new IllegalStateException("router config: targetHeader too long: " + header);
    }

    Map<String, String> routes = config.getRoutes();
    if (routes == null || routes.isEmpty()) {
      throw new IllegalStateException("router config: routes must be non-empty");
    }
    for (Map.Entry<String, String> e : routes.entrySet()) {
      String key = e.getKey();
      String value = e.getValue();
      if (key == null || key.isBlank()) {
        throw new IllegalStateException("router config: route keys must be non-blank");
      }
      String kt = key.trim();
      if (kt.length() > MAX_ROUTE_KEY_LEN) {
        throw new IllegalStateException("router config: route key too long: " + kt);
      }
      if (value == null || value.isBlank()) {
        throw new IllegalStateException("router config: route value for key '" + kt + "' must be non-blank");
      }
      requireValidKafkaTopicName("routes['" + kt + "']", value.trim());
    }
  }

  private static void requireValidKafkaTopicName(String field, String name) {
    if (name.length() > MAX_TOPIC_LEN || !TOPIC_NAME.matcher(name).matches()) {
      throw new IllegalStateException(
          "router config: invalid Kafka topic name for " + field + ": " + name);
    }
  }
}
