package ams.lawenforcement.bolo;

/**
 * Severity of a BOLO ("Be On the Lookout") alert derived from an incident description.
 *
 * <ul>
 *   <li>{@link #CRITICAL} — weapons, hostages, explosives → page SWAT / counter-terrorism.</li>
 *   <li>{@link #HIGH} — stolen vehicles, armed robbery → broadcast to patrol cars.</li>
 *   <li>{@link #NONE} — no threat keywords matched.</li>
 * </ul>
 */
public enum BoloLevel {
    NONE,
    HIGH,
    CRITICAL
}
