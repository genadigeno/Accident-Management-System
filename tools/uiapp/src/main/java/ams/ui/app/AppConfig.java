package ams.ui.app;

import ams.ui.app.config.DiscoveryProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.web.client.RestClient;

@Configuration
@EnableScheduling
@EnableConfigurationProperties(DiscoveryProperties.class)
public class AppConfig {

    /** Short-timeout client used by the discovery poller so a hung service can't stall a cycle. */
    @Bean
    RestClient discoveryRestClient(DiscoveryProperties properties) {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(properties.getTimeoutMs());
        factory.setReadTimeout(properties.getTimeoutMs());
        return RestClient.builder().requestFactory(factory).build();
    }
}
