package pl.bpiatek.linkshortenerredirectservice.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import java.net.http.HttpClient;
import java.time.Duration;

@Configuration
class VaultRestClientConfig {

    @Bean
    RestClient vaultRestClient(RestClient.Builder builder,
                               @Value("${vault.address:http://vault.vault.svc.cluster.local:8200}") String vaultAddress) {
        var httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(2))
                .build();

        var requestFactory = new JdkClientHttpRequestFactory(httpClient);
        requestFactory.setReadTimeout(Duration.ofSeconds(2));

        return builder
                .baseUrl(vaultAddress)
                .requestFactory(new JdkClientHttpRequestFactory(httpClient))
                .defaultStatusHandler(HttpStatusCode::isError, (req, res) -> {})
                .build();
    }
}
