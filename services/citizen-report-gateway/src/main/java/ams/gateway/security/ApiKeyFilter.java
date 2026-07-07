package ams.gateway.security;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Simple API-key gate for the public report API ({@code X-API-Key} header). Keys are a
 * comma-separated list in {@code gateway.api-keys}; with the property EMPTY the gate is
 * disabled (convenient for local development). Actuator endpoints are never gated.
 */
@Slf4j
@Component
public class ApiKeyFilter extends OncePerRequestFilter {

    public static final String HEADER = "X-API-Key";

    private final Set<String> apiKeys;

    public ApiKeyFilter(@Value("${gateway.api-keys:}") String configuredKeys) {
        this.apiKeys = Arrays.stream(configuredKeys.split(","))
                .map(String::trim)
                .filter(key -> !key.isEmpty())
                .collect(Collectors.toSet());
        log.info("API-key gate {}", apiKeys.isEmpty() ? "DISABLED (no keys configured)"
                : "enabled (" + apiKeys.size() + " key(s))");
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        return apiKeys.isEmpty() || request.getRequestURI().startsWith("/actuator");
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                                    FilterChain chain) throws ServletException, IOException {
        String presented = request.getHeader(HEADER);
        if (presented == null || !apiKeys.contains(presented)) {
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            response.setContentType(MediaType.APPLICATION_JSON_VALUE);
            response.getWriter().write("{\"error\":\"missing or invalid " + HEADER + "\"}");
            return;
        }
        chain.doFilter(request, response);
    }
}
