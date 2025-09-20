package pl.bpiatek.linkshortenerredirectservice.link;

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
import org.testcontainers.junit.jupiter.Testcontainers;
import pl.bpiatek.contracts.link.LinkLifecycleEventProto.LinkCreated;
import pl.bpiatek.contracts.link.LinkLifecycleEventProto.LinkDeleted;
import pl.bpiatek.contracts.link.LinkLifecycleEventProto.LinkLifecycleEvent;
import pl.bpiatek.contracts.link.LinkLifecycleEventProto.LinkUpdated;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@Testcontainers
@ActiveProfiles("test")
class LinkLifecycleConsumerTest implements WithFullInfrastructure {

    @Autowired
    private KafkaTemplate<String, LinkLifecycleEvent> kafkaTemplate;

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Value("${topic.link.lifecycle}")
    private String topicName;

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
        var shortUrl = "created123";
        var longUrl = "https://example.com/created";
        var payload = LinkCreated.newBuilder()
                .setShortUrl(shortUrl)
                .setLongUrl(longUrl)
                .build();
        var event = LinkLifecycleEvent.newBuilder().setLinkCreated(payload).build();

        // when
        kafkaTemplate.send(topicName, event);

        // then
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            var cachedValue = redisTemplate.opsForValue().get(REDIS_KEY_PREFIX + shortUrl);
            assertThat(cachedValue).isEqualTo(longUrl);
        });
    }

    @Test
    void shouldUpdateCacheKeyWhenLinkUpdatedEventIsConsumed() {
        // given
        var shortUrl = "updated456";
        var originalLongUrl = "https://example.com/original";
        var updatedLongUrl = "https://example.com/updated";
        redisTemplate.opsForValue().set(REDIS_KEY_PREFIX + shortUrl, originalLongUrl);

        var payload = LinkUpdated.newBuilder()
                .setShortUrl(shortUrl)
                .setLongUrl(updatedLongUrl)
                .build();
        var event = LinkLifecycleEvent.newBuilder().setLinkUpdated(payload).build();

        // when
        kafkaTemplate.send(topicName, event);

        // then
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            var cachedValue = redisTemplate.opsForValue().get(REDIS_KEY_PREFIX + shortUrl);
            assertThat(cachedValue).isEqualTo(updatedLongUrl);
        });
    }

    @Test
    void shouldDeleteCacheKeyWhenLinkDeletedEventIsConsumed() {
        // given:
        var shortUrl = "deleted789";
        redisTemplate.opsForValue().set(REDIS_KEY_PREFIX + shortUrl, "some-url");

        var payload = LinkDeleted.newBuilder().setShortUrl(shortUrl).build();
        var event = LinkLifecycleEvent.newBuilder().setLinkDeleted(payload).build();

        // when
        kafkaTemplate.send(topicName, event);

        // then
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            var hasKey = redisTemplate.hasKey(REDIS_KEY_PREFIX + shortUrl);
            assertThat(hasKey).isFalse();
        });
    }
}