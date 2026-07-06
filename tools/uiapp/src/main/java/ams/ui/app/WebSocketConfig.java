package ams.ui.app;

import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketTransportRegistration;

@Configuration
@EnableWebSocketMessageBroker
public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {

    @Override
    public void configureMessageBroker(MessageBrokerRegistry config) {
        config.enableSimpleBroker("/topic");
        config.setApplicationDestinationPrefixes("/ams");
    }

    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry) {
        registry.addEndpoint("/discover")
                .setAllowedOriginPatterns("*");
    }

    /**
     * Bounds per-session buffering so one slow browser cannot pile frames up on the heap:
     * a session that cannot keep up (buffer over 512KB or a send stuck for 10s) is closed
     * and the client's STOMP auto-reconnect starts it fresh.
     */
    @Override
    public void configureWebSocketTransport(WebSocketTransportRegistration registry) {
        registry.setSendTimeLimit(10_000)
                .setSendBufferSizeLimit(512 * 1024)
                .setMessageSizeLimit(128 * 1024);
    }
}
