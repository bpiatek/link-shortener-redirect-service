package pl.bpiatek.linkshortenerredirectservice.link;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import pl.bpiatek.contracts.link.LinkLifecycleEventProto.LinkCreated;
import pl.bpiatek.contracts.link.LinkLifecycleEventProto.LinkDeleted;
import pl.bpiatek.contracts.link.LinkLifecycleEventProto.LinkLifecycleEvent;
import pl.bpiatek.contracts.link.LinkLifecycleEventProto.LinkUpdated;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@ActiveProfiles("test")
class LinkLifecycleConsumerIT implements WithFullInfrastructure {

    @Autowired
    private KafkaTemplate<String, LinkLifecycleEvent> kafkaTemplate;

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Value("${topic.link.lifecycle}")
    private String topicName;

    @Autowired
    private ObjectMapper objectMapper;


    private static final String REDIS_KEY_PREFIX = "link:";

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", redpanda::getBootstrapServers);
    }

    @AfterEach
    void cleanup() {
        redisTemplate.getConnectionFactory().getConnection().flushAll();
    }

    @Test
    void shouldSetCacheKeyWhenLinkCreatedEventIsConsumed() {
        // given
        var now = Instant.now();
        var shortUrl = "created123";
        var longUrl = "https://example.com/created";
        var payload = LinkCreated.newBuilder()
                .setShortUrl(shortUrl)
                .setLongUrl(longUrl)
                .setIsActive(true)
                .setCreatedAt(toProtoTimestamp(now))
                .build();
        var event = LinkLifecycleEvent.newBuilder().setLinkCreated(payload).build();

        // when
        kafkaTemplate.send(topicName, event);

        // then
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            var cachedValue = redisTemplate.opsForValue().get(REDIS_KEY_PREFIX + shortUrl);
            assertThat(cachedValue).isNotNull();

            var redirectInfo = objectMapper.readValue(cachedValue, RedirectInfo.class);
            assertThat(redirectInfo.longUrl()).isEqualTo(longUrl);
            assertThat(redirectInfo.isDeleted()).isFalse();
            assertThat(redirectInfo.isActive()).isTrue();
            assertThat(redirectInfo.updatedAtMicros()).isEqualTo(toMicros(now));
        });
    }

    @Test
    void shouldUpdateCacheKeyWhenLinkUpdatedEventIsConsumed() throws JsonProcessingException {
        // given
        var shortUrl = "updated456";
        var originalLongUrl = "https://example.com/original";
        var updatedLongUrl = "https://example.com/updated";

        var now = Instant.now();
        var initialInfo = new RedirectInfo(originalLongUrl, true, toMicros(now), false);
        redisTemplate.opsForValue().set(REDIS_KEY_PREFIX + shortUrl, objectMapper.writeValueAsString(initialInfo));

        var future = now.plus(Duration.ofMinutes(1));

        var payload = LinkUpdated.newBuilder()
                .setShortUrl(shortUrl)
                .setLongUrl(updatedLongUrl)
                .setIsActive(true)
                .setUpdatedAt(toProtoTimestamp(future))
                .build();

        var event = LinkLifecycleEvent.newBuilder().setLinkUpdated(payload).build();

        // when
        kafkaTemplate.send(topicName, event);

        // then
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            var cachedValue = redisTemplate.opsForValue().get(REDIS_KEY_PREFIX + shortUrl);
            assertThat(cachedValue).isNotNull();

            var redirectInfo = objectMapper.readValue(cachedValue, RedirectInfo.class);
            assertThat(redirectInfo.longUrl()).isEqualTo(updatedLongUrl);
            assertThat(redirectInfo.isDeleted()).isFalse();
            assertThat(redirectInfo.isActive()).isTrue();
            assertThat(redirectInfo.updatedAtMicros()).isEqualTo(toMicros(future));
        });
    }

    @Test
    void shouldDeleteCacheKeyWhenLinkDeletedEventIsConsumed() throws JsonProcessingException {
        // given:
        var shortUrl = "deleted789";
        var info = new RedirectInfo("some-url", true, 1000L, false);
        redisTemplate.opsForValue().set(REDIS_KEY_PREFIX + shortUrl, objectMapper.writeValueAsString(info));

        var payload = LinkDeleted.newBuilder()
                .setShortUrl(shortUrl)
                .setDeletedAt(Timestamp.newBuilder().setSeconds(2000).build())
                .build();
        var event = LinkLifecycleEvent.newBuilder().setLinkDeleted(payload).build();

        // when
        kafkaTemplate.send(topicName, event);

        // then
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            var cachedValue = redisTemplate.opsForValue().get(REDIS_KEY_PREFIX + shortUrl);
            assertThat(cachedValue).isNotNull();

            var redirectInfo = objectMapper.readValue(cachedValue, RedirectInfo.class);
            assertThat(redirectInfo.isDeleted()).isTrue();
        });
    }

    private long toMicros(Instant instant) {
        return (instant.getEpochSecond() * 1_000_000L) + (instant.getNano() / 1_000);
    }

    private Timestamp toProtoTimestamp(Instant instant) {
        return Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }
}