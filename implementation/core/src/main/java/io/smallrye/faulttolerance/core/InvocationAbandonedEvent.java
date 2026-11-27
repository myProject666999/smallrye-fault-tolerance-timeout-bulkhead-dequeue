package io.smallrye.faulttolerance.core;

/**
 * Fired when the caller has given up on the guarded invocation before it produced a result,
 * but the guarded invocation itself is still running or waiting to run.
 * <p>
 * Unlike {@link io.smallrye.faulttolerance.core.timeout.TimeoutEvents.Finished TimeoutEvents.Finished},
 * this event does not describe a single strategy's outcome; it describes the caller's state and may
 * in the future be fired by other sources of abandonment besides a timeout (for example explicit
 * cancellation). Strategies that may keep doing work after the caller abandoned the invocation
 * (such as an asynchronous bulkhead with a waiting queue) register a handler for this event to
 * release the resources that the abandoned invocation still holds.
 * <p>
 * Because event handlers are append-only and are never removed, a handler must only act on the
 * attempt that registered it; see the {@code Bulkhead} implementation for the reference pattern.
 */
public enum InvocationAbandonedEvent implements FaultToleranceEvent {
    INSTANCE
}
