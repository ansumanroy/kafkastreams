package com.example.headerrouter;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.ByteBuffer;

/**
 * Resolves outbound Kafka topic from a record header using {@link RouterConfig#routes}.
 * Always returns a real topic name (route or DLQ); never null.
 */
final class HeaderRouteResolver {

  private final RouterConfig config;

  HeaderRouteResolver(RouterConfig config) {
    this.config = config;
  }

  /**
   * @return mapped route topic, or {@link RouterConfig#getDlqTopic()} when the header is
   *     missing, blank, not valid UTF-8, or not present in {@code routes}
   */
  String resolveTopicOrDlq(Headers headers) {
    String dlq = config.getDlqTopic();
    if (headers == null) {
      return dlq;
    }
    Header header = headers.lastHeader(config.getTargetHeader());
    if (header == null || header.value() == null) {
      return dlq;
    }
    String key = decodeUtf8(header.value());
    if (key == null) {
      return dlq;
    }
    key = key.trim();
    if (key.isEmpty()) {
      return dlq;
    }
    String topic = config.getRoutes().get(key);
    return topic != null ? topic : dlq;
  }

  private static String decodeUtf8(byte[] value) {
    try {
      CharsetDecoder decoder =
          StandardCharsets.UTF_8
              .newDecoder()
              .onMalformedInput(CodingErrorAction.REPORT)
              .onUnmappableCharacter(CodingErrorAction.REPORT);
      return decoder.decode(ByteBuffer.wrap(value)).toString();
    } catch (CharacterCodingException e) {
      return null;
    }
  }
}
