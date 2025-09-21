package pl.bpiatek.linkshortenerredirectservice.link;

import jakarta.servlet.http.HttpServletRequest;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import pl.bpiatek.contracts.link.LinkClickEventProto.LinkClickEvent;

import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;

@Service
public class ClickEventProducer {

    private static final Logger log = LoggerFactory.getLogger(ClickEventProducer.class);
    private static final String SOURCE_HEADER_VALUE = "redirect-service";

    private final KafkaTemplate<String, LinkClickEvent> kafkaTemplate;
    private final String topicName;

    ClickEventProducer(KafkaTemplate<String, LinkClickEvent> kafkaTemplate,
                       @Value("${topic.link.clicks}") String topicName) {
        this.kafkaTemplate = kafkaTemplate;
        this.topicName = topicName;
    }

    @Async
    public void sendClickEvent(String shortUrl, HttpServletRequest request) {
        var eventToSend = LinkClickEvent.newBuilder()
                .setShortUrl(shortUrl)
                .setIpAddress(getClientIp(request))
                .setUserAgent(request.getHeader("User-Agent"))
                .build();

        var producerRecord = new ProducerRecord<>(topicName, shortUrl, eventToSend);

        producerRecord.headers().add(new RecordHeader("trace-id", UUID.randomUUID().toString().getBytes(UTF_8)));
        producerRecord.headers().add(new RecordHeader("source", SOURCE_HEADER_VALUE.getBytes(UTF_8)));

        kafkaTemplate.send(producerRecord).whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Successfully published LinkClickEvent for shortCode: {}", shortUrl);
            } else {
                log.error("Failed to publish LinkClickEvent for shortCode: {}. Reason: {}", shortUrl, ex.getMessage());
            }
        });
    }

    private String getClientIp(HttpServletRequest request) {
        var remoteAddr = "";
        if (request != null) {
            remoteAddr = request.getHeader("X-FORWARDED-FOR");
            if (remoteAddr == null || remoteAddr.isEmpty()) {
                remoteAddr = request.getRemoteAddr();
            }
        }
        return remoteAddr;
    }
}
