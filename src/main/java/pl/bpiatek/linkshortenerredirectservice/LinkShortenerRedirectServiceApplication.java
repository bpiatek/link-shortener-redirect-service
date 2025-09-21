package pl.bpiatek.linkshortenerredirectservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@EnableAsync
@SpringBootApplication
public class LinkShortenerRedirectServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(LinkShortenerRedirectServiceApplication.class, args);
    }

}
