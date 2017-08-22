package brave.kafka;

import brave.propagation.Propagation;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import zipkin.internal.Util;

class KafkaPropagation {

  private KafkaPropagation() {
  }

  static class ProducerInjector implements Propagation.Setter<ProducerRecord, String> {
    @Override public void put(ProducerRecord carrier, String key, String value) {
      carrier.headers().add(key, value.getBytes(Util.UTF_8));
    }
  }

  static class ConsumerInjector implements Propagation.Setter<ConsumerRecord, String> {
    @Override public void put(ConsumerRecord carrier, String key, String value) {
      carrier.headers().add(key, value.getBytes(Util.UTF_8));
    }
  }

  static class ConsumerExtractor implements Propagation.Getter<ConsumerRecord, String> {
    @Override public String get(ConsumerRecord carrier, String key) {
      Header header = carrier.headers().lastHeader(key);
      if (header == null) return null;
      return new String(header.value(), Util.UTF_8);
    }
  }
}
