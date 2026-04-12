package com.example.ingestrouter;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

public final class IngestRouterApp {

  private static final Logger log = LoggerFactory.getLogger(IngestRouterApp.class);

  public static void main(String[] args) throws IOException {
    String bootstrap = requiredEnv("BOOTSTRAP_SERVERS");
    String appId = envOrDefault("APPLICATION_ID", "ingest-router");
    Path configPath = Path.of(envOrDefault("ROUTER_CONFIG_PATH", "/etc/router/config.json"));
    RouterConfig routerConfig = RouterConfigLoader.load(configPath);
    RouteResolver resolver = new RouteResolver(routerConfig);

    String ingestTopic = routerConfig.getIngestTopic();
    String dlqTopic = routerConfig.getDlqTopic();

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);
    props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, envOrDefault("NUM_STREAM_THREADS", "1"));

    StreamsBuilder builder = new StreamsBuilder();
    KStream<String, String> ingest =
        builder.stream(ingestTopic, Consumed.with(Serdes.String(), Serdes.String()));

    // BranchedKStream map keys are splitterName + branchName (no separator), e.g. "ingest-route" + "valid".
    final String splitName = "ingest-route";
    Map<String, KStream<String, String>> branches =
        ingest
            .split(Named.as(splitName))
            .branch((k, v) -> resolver.hasRoutablePayload(v), Branched.as("valid"))
            .defaultBranch(Branched.as("invalid"));

    KStream<String, String> validBranch = branches.get(splitName + "valid");
    KStream<String, String> invalidBranch = branches.get(splitName + "invalid");
    Objects.requireNonNull(validBranch, "missing branch " + splitName + "valid; keys=" + branches.keySet());
    Objects.requireNonNull(invalidBranch, "missing branch " + splitName + "invalid; keys=" + branches.keySet());

    validBranch.to(
        (key, value, recordContext) -> resolver.resolveTopic(value),
        Produced.with(Serdes.String(), Serdes.String()));

    invalidBranch.to(dlqTopic, Produced.with(Serdes.String(), Serdes.String()));

    KafkaStreams streams = new KafkaStreams(builder.build(), props);
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      log.info("Shutting down Kafka Streams");
      streams.close();
    }));

    streams.setStateListener(
        (newState, oldState) -> log.info("Kafka Streams state {} -> {}", oldState, newState));

    log.info(
        "Starting ingest router: config={}, ingest={}, dlq={}, bootstrap={}",
        configPath.toAbsolutePath(),
        ingestTopic,
        dlqTopic,
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
}
