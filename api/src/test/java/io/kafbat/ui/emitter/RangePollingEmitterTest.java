package io.kafbat.ui.emitter;

import static org.assertj.core.api.Assertions.assertThat;

import io.kafbat.ui.AbstractIntegrationTest;
import io.kafbat.ui.model.ConsumerPosition;
import io.kafbat.ui.model.PollingModeDTO;
import io.kafbat.ui.model.TopicMessageDTO;
import io.kafbat.ui.model.TopicMessageEventDTO;
import io.kafbat.ui.producer.KafkaTestProducer;
import io.kafbat.ui.serde.api.Serde;
import io.kafbat.ui.serdes.ConsumerRecordDeserializer;
import io.kafbat.ui.serdes.PropertyResolverImpl;
import io.kafbat.ui.serdes.builtin.StringSerde;
import io.kafbat.ui.service.PollingCursorsStorage;
import io.kafbat.ui.util.ApplicationMetrics;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

class RangePollingEmitterTest extends AbstractIntegrationTest {

  // small records let polling window grow, then large records exceed bytes limit of a round
  static final String MIXED_SIZES_TOPIC =
      RangePollingEmitterTest.class.getSimpleName() + "_mixed_" + UUID.randomUUID();
  static final int SMALL_RECORDS = 1000;
  static final int LARGE_RECORDS = 200;
  static final int LARGE_RECORD_BYTES = 300 * 1024;
  static final long BYTES_LIMIT = 2L * 1024 * 1024;
  // window of a not grown round (page size) holds less than the limit plus one poll
  static final int LARGE_RECORDS_PAGE = 20;

  // matches are at the topic end/start, so the round that fills the page is the last one
  static final String SEQUENTIAL_TOPIC =
      RangePollingEmitterTest.class.getSimpleName() + "_seq_" + UUID.randomUUID();
  static final int SEQUENTIAL_RECORDS = 6000;

  static final int MAX_POLL_RECORDS = 19;

  final PollingCursorsStorage cursorsStorage = new PollingCursorsStorage();

  @BeforeAll
  static void setup() {
    createTopic(new NewTopic(MIXED_SIZES_TOPIC, 1, (short) 1));
    createTopic(new NewTopic(SEQUENTIAL_TOPIC, 1, (short) 1));
    String largePayload = "x".repeat(LARGE_RECORD_BYTES);
    try (var producer = KafkaTestProducer.forKafka(kafka)) {
      for (int i = 0; i < SMALL_RECORDS; i++) {
        producer.send(MIXED_SIZES_TOPIC, "small_" + i);
      }
      for (int i = 0; i < LARGE_RECORDS; i++) {
        producer.send(MIXED_SIZES_TOPIC, "large_" + i + "_" + largePayload);
      }
      for (int i = SMALL_RECORDS; i < 2 * SMALL_RECORDS; i++) {
        producer.send(MIXED_SIZES_TOPIC, "small_" + i);
      }
      for (int i = 0; i < SEQUENTIAL_RECORDS; i++) {
        producer.send(SEQUENTIAL_TOPIC, "v_" + i);
      }
    }
  }

  @AfterAll
  static void cleanup() {
    deleteTopic(MIXED_SIZES_TOPIC);
    deleteTopic(SEQUENTIAL_TOPIC);
  }

  @Test
  void forwardEmitterLimitsBytesRetainedByRoundWithGrownWindow() {
    var position = new ConsumerPosition(PollingModeDTO.EARLIEST, MIXED_SIZES_TOPIC, List.of(), null, null);
    var emitter = new ForwardEmitter(this::createConsumer, position, LARGE_RECORDS_PAGE, createRecordsDeserializer(),
        MessageFilters.containsStringFilter("large_"), PollingSettings.createDefault(BYTES_LIMIT),
        createCursor(position, LARGE_RECORDS_PAGE));

    assertThat(largeRecordIndexes(emittedValues(emitter)))
        .containsExactlyElementsOf(IntStream.range(0, LARGE_RECORDS_PAGE).boxed().toList());
    assertRetainedBytesLimited(emitter);
  }

  @Test
  void backwardEmitterLimitsBytesRetainedByRoundWithGrownWindow() {
    var position = new ConsumerPosition(PollingModeDTO.LATEST, MIXED_SIZES_TOPIC, List.of(), null, null);
    var emitter = new BackwardEmitter(this::createConsumer, position, LARGE_RECORDS_PAGE, createRecordsDeserializer(),
        MessageFilters.containsStringFilter("large_"), PollingSettings.createDefault(BYTES_LIMIT),
        createCursor(position, LARGE_RECORDS_PAGE));

    assertThat(largeRecordIndexes(emittedValues(emitter)))
        .containsExactlyElementsOf(
            IntStream.range(0, LARGE_RECORDS_PAGE).map(i -> LARGE_RECORDS - 1 - i).boxed().toList());
    assertRetainedBytesLimited(emitter);
  }

  @Test
  void forwardEmitterReturnsCursorWhenPageIsFilledByLastRound() {
    int pageSize = 100;
    Predicate<TopicMessageDTO> filter = m -> sequenceNumber(m) >= SEQUENTIAL_RECORDS - 1000;
    var position = new ConsumerPosition(PollingModeDTO.EARLIEST, SEQUENTIAL_TOPIC, List.of(), null, null);
    var emitter = new ForwardEmitter(this::createConsumer, position, pageSize, createRecordsDeserializer(), filter,
        PollingSettings.createDefault(), createCursor(position, pageSize));

    assertThat(emittedValues(emitter)).isEqualTo(sequenceValues(5000, 5100));

    var cursor = registeredCursor();
    assertThat(cursor.consumerPosition().offsets().tpOffsets())
        .isEqualTo(Map.of(new TopicPartition(SEQUENTIAL_TOPIC, 0), 5100L));

    var nextPage = new ForwardEmitter(this::createConsumer, cursor.consumerPosition(), pageSize,
        cursor.deserializer(), cursor.filter(), PollingSettings.createDefault(),
        createCursor(cursor.consumerPosition(), pageSize));
    assertThat(emittedValues(nextPage)).isEqualTo(sequenceValues(5100, 5200));
  }

  @Test
  void backwardEmitterReturnsCursorWhenPageIsFilledByLastRound() {
    int pageSize = 100;
    Predicate<TopicMessageDTO> filter = m -> sequenceNumber(m) < 1000;
    var position = new ConsumerPosition(PollingModeDTO.LATEST, SEQUENTIAL_TOPIC, List.of(), null, null);
    var emitter = new BackwardEmitter(this::createConsumer, position, pageSize, createRecordsDeserializer(), filter,
        PollingSettings.createDefault(), createCursor(position, pageSize));

    assertThat(emittedValues(emitter)).isEqualTo(sequenceValues(900, 1000).reversed());

    var cursor = registeredCursor();
    assertThat(cursor.consumerPosition().offsets().tpOffsets())
        .isEqualTo(Map.of(new TopicPartition(SEQUENTIAL_TOPIC, 0), 900L));

    var nextPage = new BackwardEmitter(this::createConsumer, cursor.consumerPosition(), pageSize,
        cursor.deserializer(), cursor.filter(), PollingSettings.createDefault(),
        createCursor(cursor.consumerPosition(), pageSize));
    assertThat(emittedValues(nextPage)).isEqualTo(sequenceValues(800, 900).reversed());
  }

  // a round can exceed the limit by at most one poll
  private void assertRetainedBytesLimited(RangePollingEmitter emitter) {
    assertThat(emitter.getMaxRetainedRoundBytes())
        .isLessThanOrEqualTo(BYTES_LIMIT + MAX_POLL_RECORDS * (LARGE_RECORD_BYTES + 1024L));
  }

  private static List<String> emittedValues(RangePollingEmitter emitter) {
    List<TopicMessageEventDTO> events = Flux.create(emitter).collectList().block();
    assertThat(events).isNotNull();
    return events.stream()
        .filter(e -> e.getType() == TopicMessageEventDTO.TypeEnum.MESSAGE)
        .map(e -> e.getMessage().getValue())
        .toList();
  }

  // not comparing large values themselves to keep assertion messages readable
  private static List<Integer> largeRecordIndexes(List<String> values) {
    return values.stream().map(v -> Integer.parseInt(v.split("_")[1])).toList();
  }

  private static int sequenceNumber(TopicMessageDTO msg) {
    return Integer.parseInt(msg.getValue().substring("v_".length()));
  }

  private static List<String> sequenceValues(int fromInclusive, int toExclusive) {
    return IntStream.range(fromInclusive, toExclusive).mapToObj(i -> "v_" + i).toList();
  }

  private Cursor registeredCursor() {
    assertThat(cursorsStorage.asMap()).hasSize(1);
    return cursorsStorage.asMap().values().iterator().next();
  }

  private Cursor.Tracking createCursor(ConsumerPosition position, int pageSize) {
    cursorsStorage.asMap().clear();
    return cursorsStorage.createNewCursor(createRecordsDeserializer(), position, _ -> true, pageSize);
  }

  private EnhancedConsumer createConsumer() {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, MAX_POLL_RECORDS);
    return new EnhancedConsumer(props, PollingThrottler.noop(), ApplicationMetrics.noop());
  }

  private static ConsumerRecordDeserializer createRecordsDeserializer() {
    Serde s = new StringSerde();
    s.configure(PropertyResolverImpl.empty(), PropertyResolverImpl.empty(), PropertyResolverImpl.empty());
    return new ConsumerRecordDeserializer(
        StringSerde.NAME,
        s.deserializer(null, Serde.Target.KEY),
        StringSerde.NAME,
        s.deserializer(null, Serde.Target.VALUE),
        StringSerde.NAME,
        s.deserializer(null, Serde.Target.KEY),
        s.deserializer(null, Serde.Target.VALUE),
        msg -> msg
    );
  }
}
