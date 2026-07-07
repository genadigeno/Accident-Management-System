package ams.enrichment.weather;

/** Adapter for a weather backend (default: Open-Meteo). Must never throw — degrade to unavailable. */
public interface WeatherProvider {
    Weather at(double latitude, double longitude);
}
