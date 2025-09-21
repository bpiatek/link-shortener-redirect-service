package pl.bpiatek.linkshortenerredirectservice.api;

import jakarta.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import pl.bpiatek.linkshortenerredirectservice.config.TestSecurityConfiguration;
import pl.bpiatek.linkshortenerredirectservice.link.ClickEventProducer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest
@Import(TestSecurityConfiguration.class)
class RedirectControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private StringRedisTemplate redisTemplate;

    @MockitoBean
    private ClickEventProducer clickEventProducer;

    @Mock
    private ValueOperations<String, String> valueOperations;

    private static final String REDIS_KEY_PREFIX = "link:";

    @BeforeEach
    void setUp() {
        given(redisTemplate.opsForValue()).willReturn(valueOperations);
    }

    @Test
    void shouldRedirectToLongUrlWhenShortUrlExists() throws Exception {
        // given
        var shortUrl = "aB5xZ1";
        var longUrl = "https://example.com/a-very-long-and-complex-url";
        var redisKey = REDIS_KEY_PREFIX + shortUrl;
        given(valueOperations.get(redisKey)).willReturn(longUrl);

        // then
        mockMvc.perform(get("/" + shortUrl))
                .andExpect(status().isFound())
                .andExpect(header().string("Location", longUrl));

        verify(clickEventProducer).sendClickEvent(eq(shortUrl), any(HttpServletRequest.class));
    }

    @Test
    void shouldReturnNotFoundWhenShortUrlDoesNotExist() throws Exception {
        // given
        var shortUrl = "notFound";
        var redisKey = REDIS_KEY_PREFIX + shortUrl;
        given(valueOperations.get(redisKey)).willReturn(null);

        // then
        mockMvc.perform(get("/" + shortUrl))
                .andExpect(status().isNotFound());

        verifyNoInteractions(clickEventProducer);
    }
}