package pl.bpiatek.linkshortenerredirectservice.link;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import pl.bpiatek.contracts.link.LinkLifecycleEventProto;

import java.time.Duration;
import java.util.Collections;

@Component
class LinkLifecycleConsumer {

    private static final Logger log = LoggerFactory.getLogger(LinkLifecycleConsumer.class);
    private static final String REDIS_KEY_PREFIX = "link:";
    private static final String LUA_UPSERT_SCRIPT = """
            local existing = redis.call('get', KEYS[1])
            if existing then
                local existingData = cjson.decode(existing)
                if tonumber(ARGV[2]) <= tonumber(existingData.updatedAtMicros) then
                    return 0 -- Status: Stale
                end
            end
            redis.call('set', KEYS[1], ARGV[1])
            return 1 -- Status: Updated
            """;

    private final ObjectMapper objectMapper;
    private final StringRedisTemplate redisTemplate;

    LinkLifecycleConsumer(ObjectMapper objectMapper, StringRedisTemplate redisTemplate) {
        this.objectMapper = objectMapper;
        this.redisTemplate = redisTemplate;
    }

    @KafkaListener(
            topics = "${topic.link.lifecycle}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "linkLifecycleEventsContainerFactory"
    )
    public void consumeLinkLifecycleEvent(LinkLifecycleEventProto.LinkLifecycleEvent event,
                                          @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                          @Header(KafkaHeaders.OFFSET) long offset) {
        log.info("Processing event from partition {} at offset {}", partition, offset);
        var payloadCase = event.getEventPayloadCase();

        switch (payloadCase) {
            case LINK_CREATED -> handleUpsert(event.getLinkCreated().getShortUrl(),
                    event.getLinkCreated().getLongUrl(),
                    event.getLinkCreated().getIsActive(),
                    convertToMicros(event.getLinkCreated().getCreatedAt()));
            case LINK_UPDATED -> handleUpsert(event.getLinkUpdated().getShortUrl(),
                    event.getLinkUpdated().getLongUrl(),
                    event.getLinkUpdated().getIsActive(),
                    convertToMicros(event.getLinkUpdated().getUpdatedAt()));
            case LINK_DELETED -> handleLinkDeleted(event.getLinkDeleted());
            case EVENTPAYLOAD_NOT_SET -> log.warn("Received LinkLifecycleEvent with no payload set.");
            default -> log.warn("Received unknown event type in LinkLifecycleEvent: {}", payloadCase);
        }
    }

    private void handleUpsert(String shortUrl, String longUrl, boolean isActive, long eventMicros) {
        var redisKey = buildRedisKey(shortUrl);
        var info = new RedirectInfo(longUrl, isActive, eventMicros, false);

        try {
            var jsonPayload = objectMapper.writeValueAsString(info);
            var redisScript = new DefaultRedisScript<>(LUA_UPSERT_SCRIPT, Long.class);
            var result = redisTemplate.execute(
                    redisScript,
                    Collections.singletonList(redisKey),
                    jsonPayload,
                    String.valueOf(eventMicros)
            );

            if (Long.valueOf(1).equals(result)) {
                log.info("Atomic cache update successful for: {} (version: {})", shortUrl, eventMicros);
            } else {
                log.debug("Skipped stale update for: {}. Newer version already exists in Redis.", shortUrl);
            }
        } catch (JsonProcessingException e) {
            log.warn("Could not parse existing cache for version check, overwriting...");
        } catch (Exception e) {
            log.error("Redis Lua execution failed for {}", shortUrl, e);
        }
    }

    private void handleLinkDeleted(LinkLifecycleEventProto.LinkDeleted payload) {
        var redisKey = buildRedisKey(payload.getShortUrl());
        long deletedAtMicros = convertToMicros(payload.getDeletedAt());

        log.info("Received LinkDeleted event. Deleting cache key: {}", redisKey);
        var tombstone = new RedirectInfo(null, false, deletedAtMicros, true);

        try {
            var jsonValue = objectMapper.writeValueAsString(tombstone);
            redisTemplate.opsForValue().set(redisKey, jsonValue, Duration.ofHours(24));
            log.info("Tombstone set for: {} at {}", redisKey, tombstone);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize tombstone", e);
        }
    }

    private long convertToMicros(com.google.protobuf.Timestamp protoTimestamp) {
        return (protoTimestamp.getSeconds() * 1_000_000L) + (protoTimestamp.getNanos() / 1_000);
    }

    private String buildRedisKey(String shortUrl) {
        return REDIS_KEY_PREFIX + shortUrl;
    }
}