package pl.bpiatek.linkshortenerredirectservice.link;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest
@ActiveProfiles("test")
class RedirectCacheRepositoryIT implements WithFullInfrastructure {

    @Autowired
    private RedirectCacheRepository repository;

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    private static final String REDIS_KEY_PREFIX = "link:";

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", redpanda::getBootstrapServers);
    }

    @AfterEach
    void tearDown() {
        // Clean up Redis after every test to prevent data leakage
        redisTemplate.getConnectionFactory().getConnection().serverCommands().flushAll();
    }

    @Test
    void shouldReturnOptionalEmptyWhenKeyDoesNotExist() {
        // given
        var shortUrl = "missing123";

        // when
        var result = repository.findByShortUrl(shortUrl);

        // then
        assertThat(result).isEmpty();
    }

    @Test
    void shouldReturnRedirectInfoWhenValidJsonExists() throws Exception {
        // given
        var shortUrl = "valid456";
        var longUrl = "https://example.com/target";
        var info = new RedirectInfo(longUrl, true, 1000L, false);

        redisTemplate.opsForValue().set(
                REDIS_KEY_PREFIX + shortUrl,
                objectMapper.writeValueAsString(info)
        );

        // when
        var result = repository.findByShortUrl(shortUrl);

        // then
        assertThat(result).isPresent();
        assertThat(result.get().longUrl()).isEqualTo(longUrl);
        assertThat(result.get().isActive()).isTrue();
        assertThat(result.get().updatedAtMicros()).isEqualTo(1000L);
        assertThat(result.get().isDeleted()).isFalse();
    }

    @Test
    void shouldReturnTombstoneWhenLinkIsDeleted() throws Exception {
        // given
        var shortUrl = "deleted789";
        var tombstone = new RedirectInfo(null, false, 2000L, true);

        redisTemplate.opsForValue().set(
                REDIS_KEY_PREFIX + shortUrl,
                objectMapper.writeValueAsString(tombstone)
        );

        // when
        Optional<RedirectInfo> result = repository.findByShortUrl(shortUrl);

        // then
        assertThat(result).isPresent();
        assertThat(result.get().isDeleted()).isTrue();
        assertThat(result.get().longUrl()).isNull();
    }

    @Test
    void shouldThrowIllegalStateExceptionWhenJsonIsCorrupted() {
        // given
        var shortUrl = "corruptedKey";
        var corruptedJson = "this-is-not-valid-json, just plain text";

        redisTemplate.opsForValue().set(REDIS_KEY_PREFIX + shortUrl, corruptedJson);

        // when & then
        assertThatThrownBy(() -> repository.findByShortUrl(shortUrl))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Corrupted data in Redis for shortUrl: " + shortUrl)
                .hasCauseInstanceOf(com.fasterxml.jackson.core.JsonProcessingException.class);
    }
}