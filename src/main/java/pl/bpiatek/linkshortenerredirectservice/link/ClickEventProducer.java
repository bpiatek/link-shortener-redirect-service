package pl.bpiatek.linkshortenerredirectservice.link;

import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class ClickEventProducer {

    private static final Logger log = LoggerFactory.getLogger(ClickEventProducer.class);

    private final ClickEventPublisher clickEventPublisher;

    public ClickEventProducer(ClickEventPublisher clickEventPublisher) {
        this.clickEventPublisher = clickEventPublisher;
    }


    public void sendClickEvent(String shortUrl, HttpServletRequest request) {
        var ip = getClientIp(request);
        var userAgent = request.getHeader("User-Agent");

        clickEventPublisher.doSendClickEvent(shortUrl, ip, userAgent);
    }

    private String getClientIp(HttpServletRequest request) {
        if (request == null) {
            return "";
        }

        var ipAddress = request.getHeader("CF-Connecting-IP");
        if (ipAddress == null || ipAddress.isEmpty()) {
            ipAddress = request.getRemoteAddr();
        }

        return ipAddress;
    }
}
