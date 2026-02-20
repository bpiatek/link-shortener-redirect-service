package pl.bpiatek.linkshortenerredirectservice.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import pl.bpiatek.linkshortenerredirectservice.link.ClickEventPublisher;
import pl.bpiatek.linkshortenerredirectservice.link.ClientIpExtractor;
import pl.bpiatek.linkshortenerredirectservice.link.RedirectCacheRepository;
import pl.bpiatek.linkshortenerredirectservice.link.RedirectInfo;

import java.net.URI;

@RestController
class RedirectController {

    private static final Logger log = LoggerFactory.getLogger(RedirectController.class);

    private final RedirectCacheRepository redirectRepository;
    private final ClickEventPublisher clickEventPublisher;
    private final String uiUrl;

    RedirectController(RedirectCacheRepository redirectRepository,
                       ClickEventPublisher clickEventPublisher,
                       @Value("${app.ui.url}") String uiUrl) {
        this.redirectRepository = redirectRepository;
        this.clickEventPublisher = clickEventPublisher;
        this.uiUrl = uiUrl;
    }

    @GetMapping("/{shortUrl}")
    public ResponseEntity<Void> redirect(@PathVariable String shortUrl, HttpServletRequest request) {
        try {
            return redirectRepository.findByShortUrl(shortUrl)
                    .map(info -> handleRedirect(shortUrl, info, request))
                    .orElseGet(() -> redirectToUi("/404"));
        } catch (IllegalStateException e) {
            return ResponseEntity.internalServerError().build();
        }
    }

    private ResponseEntity<Void> handleRedirect(String shortUrl, RedirectInfo info, HttpServletRequest request) {
        if (info.isDeleted()) {
            log.info("Short URL {} is marked as deleted (tombstone).", shortUrl);
            return redirectToUi("/404");
        }

        if (!info.isActive()) {
            return redirectToUi("/inactive");
        }

        clickEventPublisher.publishSafe(
                shortUrl,
                ClientIpExtractor.extract(request),
                request.getHeader(HttpHeaders.USER_AGENT)
        );

        return ResponseEntity.status(HttpStatus.FOUND)
                .location(URI.create(info.longUrl()))
                .build();
    }

    private ResponseEntity<Void> redirectToUi(String path) {
        return ResponseEntity.status(HttpStatus.FOUND)
                .location(URI.create(uiUrl + path))
                .build();
    }
}