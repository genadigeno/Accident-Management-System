package ams.dispatch.domain;

/**
 * Lifecycle of a dispatch. {@code WAITING} is call stacking: no unit of the required type was
 * available, so the incident queues until one frees up.
 */
public enum DispatchStatus {
    WAITING,
    DISPATCHED,
    EN_ROUTE,
    ON_SCENE,
    CLEARED
}
