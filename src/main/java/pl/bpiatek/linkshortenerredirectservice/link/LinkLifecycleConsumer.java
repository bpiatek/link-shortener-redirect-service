package pl.bpiatek.linkshortenerredirectservice.link;

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

    private final StringRedisTemplate redisTemplate;

    LinkLifecycleConsumer(StringRedisTemplate redisTemplate) {
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
        log.info("Received LinkCreated event. Hydrating cache for key: {}", redisKey);
        redisTemplate.opsForValue().set(redisKey, payload.getLongUrl());
    }

    private void handleLinkUpdated(LinkLifecycleEventProto.LinkUpdated payload) {
        var redisKey = buildRedisKey(payload.getShortUrl());
        log.info("Received LinkUpdated event. Hydrating cache for key: {}", redisKey);
        redisTemplate.opsForValue().set(redisKey, payload.getLongUrl());
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