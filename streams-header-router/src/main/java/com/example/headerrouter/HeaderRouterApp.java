package com.example.headerrouter;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Properties;

public final class HeaderRouterApp {

  private static final Logger log = LoggerFactory.getLogger(HeaderRouterApp.class);

  public static void main(String[] args) throws IOException {
    String bootstrap = requiredEnv("BOOTSTRAP_SERVERS");
    String appId = envOrDefault("APPLICATION_ID", "header-router");
    Path configPath = Path.of(envOrDefault("ROUTER_CONFIG_PATH", "/etc/router/config.json"));
    RouterConfig routerConfig = RouterConfigLoader.load(configPath);
    HeaderRouteResolver resolver = new HeaderRouteResolver(routerConfig);

    String ingestTopic = routerConfig.getIngestTopic();
    String dlqTopic = routerConfig.getDlqTopic();

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);
    props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, envOrDefault("NUM_STREAM_THREADS", "1"));
    configureOptionalKafkaSecurity(props);

    StreamsBuilder builder = new StreamsBuilder();
    builder
        .stream(ingestTopic, Consumed.with(Serdes.String(), Serdes.String()))
        .to(
            (key, value, recordContext) -> resolver.resolveTopicOrDlq(recordContext.headers()),
            Produced.with(Serdes.String(), Serdes.String()));

    KafkaStreams streams = new KafkaStreams(builder.build(), props);
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      log.info("Shutting down Kafka Streams");
      streams.close();
    }));

    streams.setStateListener(
        (newState, oldState) -> log.info("Kafka Streams state {} -> {}", oldState, newState));

    log.info(
        "Starting header router: config={}, ingest={}, dlq={}, targetHeader={}, bootstrap={}",
        configPath.toAbsolutePath(),
        ingestTopic,
        dlqTopic,
        routerConfig.getTargetHeader(),
        bootstrap);
    streams.start();
  }

  private static String requiredEnv(String name) {
    String v = System.getenv(name);
    if (v == null || v.isBlank()) {
      throw new IllegalStateException("Missing required environment variable: " + name);
    }
    return v.trim();
  }

  private static String envOrDefault(String name, String def) {
    String v = System.getenv(name);
    return (v == null || v.isBlank()) ? def : v.trim();
  }

  private static Optional<String> optionalEnv(String name) {
    String v = System.getenv(name);
    if (v == null || v.isBlank()) {
      return Optional.empty();
    }
    return Optional.of(v.trim());
  }

  private static void configureOptionalKafkaSecurity(Properties props) {
    Optional<String> securityProtocol = optionalEnv("KAFKA_SECURITY_PROTOCOL");
    Optional<String> saslMechanism = optionalEnv("KAFKA_SASL_MECHANISM");
    Optional<String> saslUsername = optionalEnv("KAFKA_SASL_USERNAME");
    Optional<String> saslPassword = optionalEnv("KAFKA_SASL_PASSWORD");

    securityProtocol.ifPresent(value -> props.put("security.protocol", value));
    saslMechanism.ifPresent(value -> props.put("sasl.mechanism", value));

    if (saslUsername.isPresent() || saslPassword.isPresent()) {
      if (saslUsername.isEmpty() || saslPassword.isEmpty()) {
        throw new IllegalStateException(
            "KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD must both be set when using SASL");
      }
      if (saslMechanism.isEmpty()) {
        throw new IllegalStateException("KAFKA_SASL_MECHANISM is required when SASL credentials are set");
      }
      props.put(
          "sasl.jaas.config",
          "org.apache.kafka.common.security.scram.ScramLoginModule required username=\""
              + escapeForJaas(saslUsername.get())
              + "\" password=\""
              + escapeForJaas(saslPassword.get())
              + "\";");
    }
  }

  private static String escapeForJaas(String value) {
    return value.replace("\\", "\\\\").replace("\"", "\\\"");
  }
}
