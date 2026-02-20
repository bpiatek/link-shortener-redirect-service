package pl.bpiatek.linkshortenerredirectservice.link;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public class RedirectCacheRepository {

    private static final Logger log = LoggerFactory.getLogger(RedirectCacheRepository.class);
    private static final String REDIS_KEY_PREFIX = "link:";

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;

    public RedirectCacheRepository(StringRedisTemplate redisTemplate, ObjectMapper objectMapper) {
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
    }

    public Optional<RedirectInfo> findByShortUrl(String shortUrl) {
        var jsonValue = redisTemplate.opsForValue().get(REDIS_KEY_PREFIX + shortUrl);

        if (jsonValue == null) {
            return Optional.empty();
        }

        try {
            return Optional.of(objectMapper.readValue(jsonValue, RedirectInfo.class));
        } catch (JsonProcessingException e) {
            log.error("Redis data corruption for key: {}", shortUrl, e);
            throw new IllegalStateException("Corrupted data in Redis for shortUrl: " + shortUrl, e);
        }
    }
}
