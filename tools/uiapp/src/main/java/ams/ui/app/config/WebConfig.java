package ams.ui.app.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * Enables CORS on the REST API so the dashboard can be hosted as a separate front-end app
 * (a different origin) while talking to this Spring Boot back-end. The STOMP/WebSocket endpoint
 * already allows cross-origin connections (see {@code WebSocketConfig}).
 */
@Configuration
public class WebConfig implements WebMvcConfigurer {

    @Value("${frontend.cors.allowed-origins:*}")
    private String[] allowedOrigins;

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/api/**")
                .allowedOriginPatterns(allowedOrigins)
                .allowedMethods("GET", "POST", "PUT", "DELETE", "OPTIONS")
                .allowedHeaders("*");
    }
}
