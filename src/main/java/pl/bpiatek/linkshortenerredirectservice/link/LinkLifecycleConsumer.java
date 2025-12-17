package pl.bpiatek.linkshortenerredirectservice.link;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import pl.bpiatek.contracts.link.LinkLifecycleEventProto;

@Component
class LinkLifecycleConsumer {

    private static final Logger log = LoggerFactory.getLogger(LinkLifecycleConsumer.class);
    private static final String REDIS_KEY_PREFIX = "link:";

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
    public void consumeLinkLifecycleEvent(LinkLifecycleEventProto.LinkLifecycleEvent event) {
        var payloadCase = event.getEventPayloadCase();
        switch (payloadCase) {
            case LINK_CREATED -> handleLinkCreated(event.getLinkCreated());
            case LINK_UPDATED -> handleLinkUpdated(event.getLinkUpdated());
            case LINK_DELETED -> handleLinkDeleted(event.getLinkDeleted());
            case EVENTPAYLOAD_NOT_SET ->
                    log.warn("Received LinkLifecycleEvent with no payload set.");
            default ->
                    log.warn("Received unknown event type in LinkLifecycleEvent: {}", payloadCase);
        }
    }

    private void handleLinkCreated(LinkLifecycleEventProto.LinkCreated payload) {
        var redisKey = buildRedisKey(payload.getShortUrl());
        var info = new RedirectInfo(payload.getLongUrl(), payload.getIsActive());

        try {
            log.info("Received LinkCreated event. Hydrating cache for key: {}", redisKey);
            var jsonValue = objectMapper.writeValueAsString(info);
            redisTemplate.opsForValue().set(redisKey, jsonValue);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize redirect info", e);
        }
    }

    private void handleLinkUpdated(LinkLifecycleEventProto.LinkUpdated payload) {
        var redisKey = buildRedisKey(payload.getShortUrl());
        var info = new RedirectInfo(payload.getLongUrl(), payload.getIsActive());

        try {
            log.info("Received LinkUpdated event. Hydrating cache for key: {}", redisKey);
            var jsonValue = objectMapper.writeValueAsString(info);
            redisTemplate.opsForValue().set(redisKey, jsonValue);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize redirect info", e);
        }
    }

    private void handleLinkDeleted(LinkLifecycleEventProto.LinkDeleted payload) {
        var redisKey = buildRedisKey(payload.getShortUrl());
        log.info("Received LinkDeleted event. Deleting key from cache: {}", redisKey);
        redisTemplate.delete(redisKey);
    }

    private String buildRedisKey(String shortUrl) {
        return REDIS_KEY_PREFIX + shortUrl;
    }
}