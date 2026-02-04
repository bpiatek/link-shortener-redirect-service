package pl.bpiatek.linkshortenerredirectservice.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import pl.bpiatek.linkshortenerredirectservice.link.ClickEventProducer;
import pl.bpiatek.linkshortenerredirectservice.link.RedirectInfo;

import java.net.URI;

@RestController
class RedirectController {

    private static final Logger log = LoggerFactory.getLogger(RedirectController.class);
    private static final String REDIS_KEY_PREFIX = "link:";

    private final StringRedisTemplate redisTemplate;
    private final ClickEventProducer clickEventProducer;

    @Value("${app.ui.url}")
    private String uiUrl;

    private final ObjectMapper objectMapper;


    RedirectController(StringRedisTemplate redisTemplate, ClickEventProducer clickEventProducer, ObjectMapper objectMapper) {
        this.redisTemplate = redisTemplate;
        this.clickEventProducer = clickEventProducer;
        this.objectMapper = objectMapper;
    }

    @GetMapping("/{shortUrl}")
    public ResponseEntity<Void> redirect(@PathVariable String shortUrl, HttpServletRequest request) {
        var jsonValue = redisTemplate.opsForValue().get(REDIS_KEY_PREFIX + shortUrl);

        if (jsonValue == null) {
            return ResponseEntity.status(HttpStatus.FOUND)
                    .location(URI.create("/404"))
                    .build();
        }

        try {
            var redirectInfo = objectMapper.readValue(jsonValue, RedirectInfo.class);

            if (!redirectInfo.isActive()) {
                return ResponseEntity.status(HttpStatus.FOUND)
                        .location(URI.create("/inactive"))
                        .build();
            }

            clickEventProducer.sendClickEvent(shortUrl, request);
            return ResponseEntity.status(HttpStatus.FOUND)
                    .location(URI.create(redirectInfo.longUrl()))
                    .build();

        } catch (JsonProcessingException e) {
            log.error("Redis data corruption for key: {}", shortUrl, e);
            return ResponseEntity.internalServerError().build();
        }
    }
}