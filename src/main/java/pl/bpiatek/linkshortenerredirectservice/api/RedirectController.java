package pl.bpiatek.linkshortenerredirectservice.api;

import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import pl.bpiatek.linkshortenerredirectservice.link.ClickEventProducer;

import java.net.URI;

import static org.springframework.http.HttpStatus.FOUND;

@RestController
class RedirectController {

    private static final Logger log = LoggerFactory.getLogger(RedirectController.class);
    private static final String REDIS_KEY_PREFIX = "link:";

    private final StringRedisTemplate redisTemplate;
    private final ClickEventProducer clickEventProducer;

    RedirectController(StringRedisTemplate redisTemplate, ClickEventProducer clickEventProducer) {
        this.redisTemplate = redisTemplate;
        this.clickEventProducer = clickEventProducer;
    }

    @GetMapping("/{shortUrl}")
    public ResponseEntity<Void> redirect(@PathVariable String shortUrl, HttpServletRequest request) {
        log.info("Is redirect endpoint running in virtual thread: {}", Thread.currentThread().isVirtual());
        var longUrl = redisTemplate.opsForValue().get(REDIS_KEY_PREFIX + shortUrl);

        if (longUrl != null) {
            clickEventProducer.sendClickEvent(shortUrl, request);

            return ResponseEntity.status(FOUND)
                    .location(URI.create(longUrl))
                    .build();
        } else {
            return ResponseEntity.notFound().build();
        }
    }
}