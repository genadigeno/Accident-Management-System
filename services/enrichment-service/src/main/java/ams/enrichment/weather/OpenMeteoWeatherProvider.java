package ams.enrichment.weather;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.net.http.HttpClient;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Locale;

/**
 * Current weather from the free <a href="https://open-meteo.com">Open-Meteo</a> API (no API key).
 * Results are cached by coordinate rounded to ~1&nbsp;km with a short TTL, so a burst of nearby
 * incidents makes at most one call, and the enrichment path never blocks on a slow response
 * (short timeout, degrade to {@link Weather#unavailable()} on any failure — this sits inline on
 * the stream and must not stall it).
 */
@Slf4j
@Component
public class OpenMeteoWeatherProvider implements WeatherProvider {

    private final RestClient http;
    private final String baseUrl;

    private final Cache<String, Weather> cache = Caffeine.newBuilder()
            .maximumSize(5_000)
            .expireAfterWrite(Duration.ofMinutes(10))
            .build();

    public OpenMeteoWeatherProvider(
            @Value("${weather.open-meteo.url:https://api.open-meteo.com/v1/forecast}") String baseUrl,
            @Value("${weather.open-meteo.connect-timeout-ms:2000}") int connectTimeoutMs,
            @Value("${weather.open-meteo.read-timeout-ms:3000}") int readTimeoutMs,
            // Escape hatch for TLS-intercepting corporate proxies (dev only) — mirrors the
            // project's maven ssl.insecure workaround. Leave false in production.
            @Value("${weather.open-meteo.insecure-tls:false}") boolean insecureTls) {
        this.baseUrl = baseUrl;
        this.http = insecureTls
                ? RestClient.builder().requestFactory(insecureFactory(connectTimeoutMs, readTimeoutMs)).build()
                : RestClient.builder().requestFactory(secureFactory(connectTimeoutMs, readTimeoutMs)).build();
        if (insecureTls) {
            log.warn("Open-Meteo TLS verification DISABLED (weather.open-meteo.insecure-tls=true) — dev only");
        }
    }

    private static SimpleClientHttpRequestFactory secureFactory(int connectTimeoutMs, int readTimeoutMs) {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(connectTimeoutMs);
        factory.setReadTimeout(readTimeoutMs);
        return factory;
    }

    private static JdkClientHttpRequestFactory insecureFactory(int connectTimeoutMs, int readTimeoutMs) {
        try {
            TrustManager[] trustAll = { new X509TrustManager() {
                public void checkClientTrusted(X509Certificate[] c, String a) {}
                public void checkServerTrusted(X509Certificate[] c, String a) {}
                public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
            }};
            SSLContext ssl = SSLContext.getInstance("TLS");
            ssl.init(null, trustAll, new java.security.SecureRandom());
            HttpClient client = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofMillis(connectTimeoutMs))
                    .sslContext(ssl)
                    .build();
            JdkClientHttpRequestFactory factory = new JdkClientHttpRequestFactory(client);
            factory.setReadTimeout(Duration.ofMillis(readTimeoutMs));
            return factory;
        } catch (Exception e) {
            throw new IllegalStateException("could not build insecure TLS client", e);
        }
    }

    @Override
    public Weather at(double latitude, double longitude) {
        String key = String.format(Locale.ROOT, "%.2f:%.2f", latitude, longitude);
        Weather cached = cache.getIfPresent(key);
        if (cached != null) {
            return cached;
        }
        Weather weather = fetch(latitude, longitude);
        // Only cache successful lookups, so a transient outage doesn't pin "unknown" for 10 min.
        if (weather.temperatureC() != -999.0) {
            cache.put(key, weather);
        }
        return weather;
    }

    private Weather fetch(double latitude, double longitude) {
        try {
            Response r = http.get()
                    .uri(baseUrl + "?latitude={lat}&longitude={lon}&current=temperature_2m,precipitation,weather_code",
                            latitude, longitude)
                    .retrieve()
                    .body(Response.class);
            if (r == null || r.current() == null) {
                return Weather.unavailable();
            }
            Current c = r.current();
            return new Weather(condition(c.weatherCode()),
                    c.temperature() != null ? c.temperature() : -999.0,
                    c.precipitation() != null ? c.precipitation() : -1.0);
        } catch (Exception e) {
            log.debug("weather unavailable for {},{}: {}", latitude, longitude, e.getMessage());
            return Weather.unavailable();
        }
    }

    /** WMO weather-interpretation codes → a coarse condition label. */
    static String condition(Integer code) {
        if (code == null) return "unknown";
        if (code == 0) return "clear";
        if (code <= 3) return "cloudy";
        if (code <= 48) return "fog";
        if (code <= 67) return "rain";
        if (code <= 77) return "snow";
        if (code <= 82) return "rain";
        if (code <= 86) return "snow";
        return "thunderstorm";
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    record Response(Current current) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    record Current(@JsonProperty("temperature_2m") Double temperature,
                   Double precipitation,
                   @JsonProperty("weather_code") Integer weatherCode) {}
}
