package pl.bpiatek.linkshortenerredirectservice.api;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import pl.bpiatek.linkshortenerredirectservice.config.TestSecurityConfiguration;
import pl.bpiatek.linkshortenerredirectservice.link.ClickEventPublisher;
import pl.bpiatek.linkshortenerredirectservice.link.RedirectCacheRepository;
import pl.bpiatek.linkshortenerredirectservice.link.RedirectInfo;

import java.util.Optional;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(RedirectController.class)
@Import(TestSecurityConfiguration.class)
class RedirectControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private RedirectCacheRepository redirectRepository;

    @MockitoBean
    private ClickEventPublisher clickEventPublisher;

    @Test
    void shouldRedirectToLongUrlWhenShortUrlExists() throws Exception {
        // given
        var shortUrl = "aB5xZ1";
        var longUrl = "https://example.com/target";
        var defaultIp = "127.0.0.1";
        var defaultAgent = "standard-agent";

        var info = new RedirectInfo(longUrl, true, 1000L, false);
        given(redirectRepository.findByShortUrl(shortUrl)).willReturn(Optional.of(info));

        // when
        mockMvc.perform(get("/" + shortUrl)
                        .header("User-Agent", defaultAgent)
                        .with(request -> {
                            request.setRemoteAddr(defaultIp);
                            return request;
                        }))
                // then
                .andExpect(status().isFound())
                .andExpect(header().string("Location", longUrl));

        verify(clickEventPublisher).publishSafe(eq(shortUrl), eq(defaultIp), eq(defaultAgent));
    }

    @Test
    void shouldRedirectToUi404WhenShortUrlDoesNotExist() throws Exception {
        // given
        var shortUrl = "notFound";
        given(redirectRepository.findByShortUrl(shortUrl)).willReturn(Optional.empty());

        // when
        mockMvc.perform(get("/" + shortUrl))
                // then
                .andExpect(status().isFound())
                .andExpect(header().string("Location", containsString("/404")));

        verifyNoInteractions(clickEventPublisher);
    }

    @Test
    void shouldRedirectToUi404WhenTombstoneExists() throws Exception {
        // given
        var shortUrl = "deletedUrl";
        var tombstone = new RedirectInfo(null, false, 2000L, true);
        given(redirectRepository.findByShortUrl(shortUrl)).willReturn(Optional.of(tombstone));

        // when
        mockMvc.perform(get("/" + shortUrl))
                // then
                .andExpect(status().isFound())
                .andExpect(header().string("Location", containsString("/404")));

        verifyNoInteractions(clickEventPublisher);
    }

    @Test
    void shouldRedirectToInactiveWhenLinkIsDisabled() throws Exception {
        // given
        var shortUrl = "inactiveUrl";
        var inactiveInfo = new RedirectInfo("https://example.com", false, 1000L, false);
        given(redirectRepository.findByShortUrl(shortUrl)).willReturn(Optional.of(inactiveInfo));

        // when
        mockMvc.perform(get("/" + shortUrl))
                // then
                .andExpect(status().isFound())
                .andExpect(header().string("Location", containsString("/inactive")));

        verifyNoInteractions(clickEventPublisher);
    }

    @Test
    void shouldUseXForwardedForIPWhenPresent() throws Exception {
        // given
        var shortUrl = "proxy123";
        var realIp = "200.200.200.200";
        var proxyChain = realIp + ", 10.0.0.1, 192.168.1.50";

        var info = new RedirectInfo("https://example.com", true, 1000L, false);
        given(redirectRepository.findByShortUrl(shortUrl)).willReturn(Optional.of(info));

        // when
        mockMvc.perform(get("/" + shortUrl)
                        .header("X-Forwarded-For", proxyChain)
                        .header("User-Agent", "test-agent")
                        .with(request -> {
                            request.setRemoteAddr("10.0.0.1");
                            return request;
                        }))
                .andExpect(status().isFound());

        // then
        verify(clickEventPublisher).publishSafe(eq(shortUrl), eq(realIp), eq("test-agent"));
    }

    @Test
    void shouldUseCFConnectingIPWhenXForwardedIsMissing() throws Exception {
        // given
        var shortUrl = "cf123";
        var realIp = "8.8.8.8";

        var info = new RedirectInfo("https://example.com", true, 1000L, false);
        given(redirectRepository.findByShortUrl(shortUrl)).willReturn(Optional.of(info));

        // when
        mockMvc.perform(get("/" + shortUrl)
                        .header("CF-Connecting-IP", realIp)
                        .header("User-Agent", "cf-agent")
                        .with(request -> {
                            request.setRemoteAddr("10.0.0.1");
                            return request;
                        }))
                .andExpect(status().isFound());

        // then
        verify(clickEventPublisher).publishSafe(eq(shortUrl), eq(realIp), eq("cf-agent"));
    }

    @Test
    void shouldReturn500WhenDataIsCorrupted() throws Exception {
        // given
        var shortUrl = "corrupted";
        given(redirectRepository.findByShortUrl(shortUrl))
                .willThrow(new IllegalStateException("Corrupted data in Redis"));

        // when
        mockMvc.perform(get("/" + shortUrl))
                // then
                .andExpect(status().isInternalServerError());

        verifyNoInteractions(clickEventPublisher);
    }
}