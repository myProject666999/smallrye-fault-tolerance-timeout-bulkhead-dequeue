package io.smallrye.faulttolerance.core.bulkhead;

import static io.smallrye.faulttolerance.core.FaultToleranceContextUtil.async;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.eclipse.microprofile.faulttolerance.exceptions.BulkheadException;
import org.eclipse.microprofile.faulttolerance.exceptions.TimeoutException;
import org.junit.jupiter.api.Test;

import io.smallrye.faulttolerance.core.Completer;
import io.smallrye.faulttolerance.core.FaultToleranceContext;
import io.smallrye.faulttolerance.core.FaultToleranceStrategy;
import io.smallrye.faulttolerance.core.Future;
import io.smallrye.faulttolerance.core.metrics.MeteredOperation;
import io.smallrye.faulttolerance.core.metrics.MetricsCollector;
import io.smallrye.faulttolerance.core.metrics.MetricsRecorder;
import io.smallrye.faulttolerance.core.retry.AsyncDelay;
import io.smallrye.faulttolerance.core.retry.Retry;
import io.smallrye.faulttolerance.core.retry.SyncDelay;
import io.smallrye.faulttolerance.core.stopwatch.TestStopwatch;
import io.smallrye.faulttolerance.core.timeout.Timeout;
import io.smallrye.faulttolerance.core.timer.TestTimer;
import io.smallrye.faulttolerance.core.timer.TimerTask;
import io.smallrye.faulttolerance.core.util.ExceptionDecision;
import io.smallrye.faulttolerance.core.util.ResultDecision;
import io.smallrye.faulttolerance.core.util.RunnableWrapper;
import io.smallrye.faulttolerance.core.util.barrier.Barrier;

/**
 * When an asynchronous invocation is guarded by both a {@link Timeout} and a {@link Bulkhead},
 * a timeout that elapses while the invocation is waiting in the bulkhead queue must withdraw
 * the queued task: it leaves the queue, returns its capacity permit, never runs the guarded
 * method and completes with a deterministic error, while the caller already observes
 * {@link TimeoutException}. A task that has already started running is not forcefully stopped.
 */
public class BulkheadTimeoutDequeueTest {
    /**
     * An invocation that never completes on its own; complete it through the returned completer.
     */
    private static FaultToleranceStrategy<String> pendingInvocation(BlockingQueue<Completer<String>> pending,
            AtomicInteger invocationCount) {
        return ctx -> {
            invocationCount.incrementAndGet();
            Completer<String> completer = Completer.create();
            pending.add(completer);
            return completer.future();
        };
    }

    private static Timeout<String> timeoutAround(Bulkhead<String> bulkhead, TestTimer timer) {
        return new Timeout<>(bulkhead, "test", 1000, timer);
    }

    private static EventCounters registerEventCounters(FaultToleranceContext<String> ctx) {
        EventCounters counters = new EventCounters();
        ctx.registerEventHandler(BulkheadEvents.DecisionMade.class, event -> {
            if (event.accepted) {
                counters.accepted.incrementAndGet();
            } else {
                counters.rejected.incrementAndGet();
            }
        });
        ctx.registerEventHandler(BulkheadEvents.StartedWaiting.class,
                ignored -> counters.startedWaiting.incrementAndGet());
        ctx.registerEventHandler(BulkheadEvents.FinishedWaiting.class,
                ignored -> counters.finishedWaiting.incrementAndGet());
        ctx.registerEventHandler(BulkheadEvents.StartedRunning.class,
                ignored -> counters.startedRunning.incrementAndGet());
        ctx.registerEventHandler(BulkheadEvents.FinishedRunning.class,
                ignored -> counters.finishedRunning.incrementAndGet());
        return counters;
    }

    private static class EventCounters {
        final AtomicInteger accepted = new AtomicInteger();
        final AtomicInteger rejected = new AtomicInteger();
        final AtomicInteger startedWaiting = new AtomicInteger();
        final AtomicInteger finishedWaiting = new AtomicInteger();
        final AtomicInteger startedRunning = new AtomicInteger();
        final AtomicInteger finishedRunning = new AtomicInteger();
    }

    private Completer<String> nextPending(BlockingQueue<Completer<String>> pending) throws InterruptedException {
        Completer<String> completer = pending.poll(5, TimeUnit.SECONDS);
        assertThat(completer).isNotNull();
        return completer;
    }

    // --- numeric scenario -----------------------------------------------------

    @Test
    public void timedOutQueuedTaskIsWithdrawnAndFreesQueueSlotAndPermit() throws Throwable {
        BlockingQueue<Completer<String>> pending = new LinkedBlockingQueue<>();
        AtomicInteger invocationCount = new AtomicInteger();
        Bulkhead<String> bulkhead = new Bulkhead<>(pendingInvocation(pending, invocationCount),
                "test", 1, 2, false);

        TestTimer timerA = new TestTimer();
        TestTimer timerB = new TestTimer();
        TestTimer timerC = new TestTimer();
        TestTimer timerD = new TestTimer();
        TestTimer timerE = new TestTimer();

        // A starts running immediately; B and C occupy the queue
        FaultToleranceContext<String> ctxA = async(null);
        FaultToleranceContext<String> ctxB = async(null);
        FaultToleranceContext<String> ctxC = async(null);
        EventCounters countersA = registerEventCounters(ctxA);
        EventCounters countersB = registerEventCounters(ctxB);
        EventCounters countersC = registerEventCounters(ctxC);

        Future<String> resultA = timeoutAround(bulkhead, timerA).apply(ctxA);
        Future<String> resultB = timeoutAround(bulkhead, timerB).apply(ctxB);
        Future<String> resultC = timeoutAround(bulkhead, timerC).apply(ctxC);

        assertThat(bulkhead.getAvailableCapacityPermits()).isZero();
        assertThat(bulkhead.getQueueSize()).isEqualTo(2);
        assertThat(invocationCount).hasValue(1);

        // B times out while still waiting: it must be withdrawn before the caller sees the timeout
        TimerTask timeoutTaskB = timerB.nextScheduledTask();
        timerB.executeSynchronously(timeoutTaskB);

        assertThat(bulkhead.getAvailableCapacityPermits()).isEqualTo(1);
        assertThat(bulkhead.getQueueSize()).isEqualTo(1);
        assertThatThrownBy(resultB::awaitBlocking).isExactlyInstanceOf(TimeoutException.class);
        assertThat(invocationCount).hasValue(1);
        assertThat(countersB.accepted).hasValue(1);
        assertThat(countersB.rejected).hasValue(0);
        assertThat(countersB.startedWaiting).hasValue(1);
        assertThat(countersB.finishedWaiting).hasValue(1);
        assertThat(countersB.startedRunning).hasValue(0);
        assertThat(countersB.finishedRunning).hasValue(0);

        // the freed queue slot accepts D; the bulkhead is full again, so E is rejected
        FaultToleranceContext<String> ctxD = async(null);
        FaultToleranceContext<String> ctxE = async(null);
        EventCounters countersD = registerEventCounters(ctxD);
        EventCounters countersE = registerEventCounters(ctxE);

        Future<String> resultD = timeoutAround(bulkhead, timerD).apply(ctxD);
        Future<String> resultE = timeoutAround(bulkhead, timerE).apply(ctxE);

        assertThat(bulkhead.getQueueSize()).isEqualTo(2);
        assertThat(bulkhead.getAvailableCapacityPermits()).isZero();
        assertThatThrownBy(resultE::awaitBlocking).isExactlyInstanceOf(BulkheadException.class);
        assertThat(countersD.accepted).hasValue(1);
        assertThat(countersE.accepted).hasValue(0);
        assertThat(countersE.rejected).hasValue(1);

        // complete A: C (the older queued task) starts running, D keeps waiting
        nextPending(pending).complete("A");
        assertThat(resultA.awaitBlocking()).isEqualTo("A");
        assertThat(invocationCount).hasValue(2);
        assertThat(bulkhead.getQueueSize()).isEqualTo(1);

        // complete C: D finally runs
        nextPending(pending).complete("C");
        assertThat(resultC.awaitBlocking()).isEqualTo("C");
        nextPending(pending).complete("D");
        assertThat(resultD.awaitBlocking()).isEqualTo("D");

        assertThat(invocationCount).hasValue(3);
        assertThat(bulkhead.getQueueSize()).isZero();
        assertThat(bulkhead.getAvailableCapacityPermits()).isEqualTo(3);

        // B's guarded method never ran; running events stay balanced across A, C, D
        EventCounters total = new EventCounters();
        for (EventCounters counters : List.of(countersA, countersB, countersC, countersD, countersE)) {
            total.accepted.addAndGet(counters.accepted.get());
            total.rejected.addAndGet(counters.rejected.get());
            total.startedWaiting.addAndGet(counters.startedWaiting.get());
            total.finishedWaiting.addAndGet(counters.finishedWaiting.get());
            total.startedRunning.addAndGet(counters.startedRunning.get());
            total.finishedRunning.addAndGet(counters.finishedRunning.get());
        }
        assertThat(total.accepted).hasValue(4); // A, B, C, D
        assertThat(total.rejected).hasValue(1); // E
        assertThat(total.startedRunning).hasValue(3); // A, C, D
        assertThat(total.finishedRunning).hasValue(3);
        assertThat(total.startedWaiting).hasValue(4); // A waited for zero time, B, C, D waited in the queue
        assertThat(total.finishedWaiting).hasValue(4);

        // no timer tasks left behind
        assertThat(timerA.hasScheduledTasks()).isFalse();
        assertThat(timerB.hasScheduledTasks()).isFalse();
        assertThat(timerC.hasScheduledTasks()).isFalse();
        assertThat(timerD.hasScheduledTasks()).isFalse();
    }

    // --- interleaving scenario ------------------------------------------------

    @Test
    public void timeoutAfterTaskStartedRunningDoesNotWithdrawTask() throws Throwable {
        Barrier beforeDoRun = Barrier.noninterruptible();
        CountDownLatch taskReachedBarrier = new CountDownLatch(1);

        // B's wrapper stops after the state switched to RUNNING but before `doRun()` calls
        // the guarded method, reproducing the narrow window between dequeue and execution.
        RunnableWrapper wrapper = runnable -> () -> {
            if (Thread.currentThread().getName().startsWith("completion-thread")) {
                taskReachedBarrier.countDown();
                try {
                    beforeDoRun.await();
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
            }
            runnable.run();
        };

        BlockingQueue<Completer<String>> pending = new LinkedBlockingQueue<>();
        AtomicInteger invocationCount = new AtomicInteger();
        Bulkhead<String> bulkhead = new Bulkhead<>(pendingInvocation(pending, invocationCount),
                "test", 1, 1, false, wrapper);

        TestTimer timerA = new TestTimer();
        TestTimer timerB = new TestTimer();

        FaultToleranceContext<String> ctxA = async(null);
        FaultToleranceContext<String> ctxB = async(null);
        EventCounters countersB = registerEventCounters(ctxB);

        Future<String> resultA = timeoutAround(bulkhead, timerA).apply(ctxA);
        Future<String> resultB = timeoutAround(bulkhead, timerB).apply(ctxB);

        assertThat(bulkhead.getQueueSize()).isEqualTo(1);

        // complete A on another thread: it dequeues B, B switches to RUNNING and blocks in the wrapper
        Thread completionThread = new Thread(() -> {
            try {
                nextPending(pending).complete("A");
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
        }, "completion-thread");
        completionThread.start();

        assertThat(taskReachedBarrier.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(bulkhead.getQueueSize()).isZero();
        // B was dequeued and its state switched to RUNNING, but `doRun()` has not been called yet.
        // A already released its capacity permit on completion, B still holds its own, so one is free;
        // the work permit is held by B.
        int availableCapacityAfterDequeue = bulkhead.getAvailableCapacityPermits();
        assertThat(availableCapacityAfterDequeue).isEqualTo(1);
        assertThat(invocationCount).hasValue(1);

        // B has started running, so the timeout must not withdraw it; the caller still gets the timeout
        timerB.executeSynchronously(timerB.nextScheduledTask());
        assertThatThrownBy(resultB::awaitBlocking).isExactlyInstanceOf(TimeoutException.class);
        // the timeout did not withdraw B: it is not back in the queue and its permit is still held
        assertThat(bulkhead.getQueueSize()).isZero();
        assertThat(bulkhead.getAvailableCapacityPermits()).isEqualTo(availableCapacityAfterDequeue);
        assertThat(invocationCount).hasValue(1);

        // let B finish: the guarded method runs exactly once and the permits are released normally
        beforeDoRun.open();
        completionThread.join(5_000);
        assertThat(completionThread.isAlive()).isFalse();

        assertThat(countersB.startedRunning).hasValue(1);
        nextPending(pending).complete("B");
        assertThat(resultA.awaitBlocking()).isEqualTo("A");
        assertThat(invocationCount).hasValue(2);
        assertThat(bulkhead.getQueueSize()).isZero();
        assertThat(bulkhead.getAvailableCapacityPermits()).isEqualTo(2);
        assertThat(countersB.finishedRunning).hasValue(1);
    }

    // --- retry scenario -------------------------------------------------------

    @Test
    public void eachRetryAttemptRegistersItsOwnHandlerAndReleasesItsOwnPermit() throws Throwable {
        BlockingQueue<Completer<String>> pending = new LinkedBlockingQueue<>();
        AtomicInteger invocationCount = new AtomicInteger();

        // A occupies the only running slot for the whole test; both retry attempts of B queue up
        Bulkhead<String> bulkhead = new Bulkhead<>(pendingInvocation(pending, invocationCount),
                "test", 1, 2, false);

        TestTimer timerA = new TestTimer();
        TestTimer timerB = new TestTimer();

        FaultToleranceContext<String> ctxA = async(null);
        FaultToleranceContext<String> ctxB = async(null);
        EventCounters countersB = registerEventCounters(ctxB);

        Future<String> resultA = timeoutAround(bulkhead, timerA).apply(ctxA);

        // `Retry` wraps a fresh `Timeout` per attempt, and both attempts use the same timer,
        // so each attempt schedules its own timer task on `timerB`
        Timeout<String> timeoutPerAttempt = new Timeout<>(bulkhead, "test", 1000, timerB);
        Retry<String> retry = new Retry<>(timeoutPerAttempt, "retry",
                ResultDecision.ALWAYS_EXPECTED, ExceptionDecision.ALWAYS_FAILURE,
                1, Long.MAX_VALUE, SyncDelay.NONE, AsyncDelay.NONE, new TestStopwatch(), null);
        Future<String> resultB = retry.apply(ctxB);

        assertThat(bulkhead.getAvailableCapacityPermits()).isEqualTo(1); // size 1 + queue 2, A and B's 1st attempt
        assertThat(bulkhead.getQueueSize()).isEqualTo(1);

        // 1st attempt times out in the queue and is withdrawn; the retry immediately queues a 2nd attempt
        timerB.executeSynchronously(timerB.nextScheduledTask());
        assertThat(bulkhead.getQueueSize()).isEqualTo(1);
        assertThat(bulkhead.getAvailableCapacityPermits()).isEqualTo(1);
        assertThat(countersB.startedRunning).hasValue(0);

        // 2nd attempt times out in the queue and is withdrawn as well; retries are exhausted
        timerB.executeSynchronously(timerB.nextScheduledTask());
        assertThatThrownBy(resultB::awaitBlocking).isExactlyInstanceOf(TimeoutException.class);

        assertThat(bulkhead.getQueueSize()).isZero();
        assertThat(invocationCount).hasValue(1); // only A ever ran
        assertThat(countersB.accepted).hasValue(2); // both attempts were accepted, none rejected
        assertThat(countersB.rejected).hasValue(0);
        assertThat(countersB.startedWaiting).hasValue(2);
        assertThat(countersB.finishedWaiting).hasValue(2);
        assertThat(countersB.startedRunning).hasValue(0);

        // finish A: all capacity permits must be back, the two withdrawn attempts each returned exactly one
        nextPending(pending).complete("A");
        assertThat(resultA.awaitBlocking()).isEqualTo("A");
        assertThat(bulkhead.getAvailableCapacityPermits()).isEqualTo(3);
        assertThat(bulkhead.getQueueSize()).isZero();
    }

    // --- context propagation --------------------------------------------------

    @Test
    public void withdrawnTaskDoesNotDisturbContextOfLaterQueuedTask() throws Throwable {
        CopyOnWriteArrayList<String> observedContexts = new CopyOnWriteArrayList<>();
        ThreadLocal<String> context = new ThreadLocal<>();

        RunnableWrapper contextPropagation = runnable -> {
            String captured = context.get();
            return () -> {
                String previous = context.get();
                context.set(captured);
                try {
                    runnable.run();
                } finally {
                    context.set(previous);
                }
            };
        };

        BlockingQueue<Completer<String>> pending = new LinkedBlockingQueue<>();
        AtomicInteger invocationCount = new AtomicInteger();
        FaultToleranceStrategy<String> invocation = ctx -> {
            observedContexts.add(context.get());
            return pendingInvocation(pending, invocationCount).apply(ctx);
        };

        Bulkhead<String> bulkhead = new Bulkhead<>(invocation, "test", 1, 2, false, contextPropagation);

        TestTimer timerA = new TestTimer();
        TestTimer timerB = new TestTimer();
        TestTimer timerC = new TestTimer();

        context.set("A");
        Future<String> resultA = timeoutAround(bulkhead, timerA).apply(async(null));
        context.set("B");
        Future<String> resultB = timeoutAround(bulkhead, timerB).apply(async(null));
        context.set("C");
        Future<String> resultC = timeoutAround(bulkhead, timerC).apply(async(null));
        context.remove();

        assertThat(bulkhead.getQueueSize()).isEqualTo(2);

        // B is withdrawn while waiting; C must later run with C's own context, on A's completion thread
        timerB.executeSynchronously(timerB.nextScheduledTask());
        assertThatThrownBy(resultB::awaitBlocking).isExactlyInstanceOf(TimeoutException.class);
        assertThat(bulkhead.getQueueSize()).isEqualTo(1);

        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread completionThread = new Thread(() -> {
            try {
                nextPending(pending).complete("A");
            } catch (Throwable e) {
                failure.set(e);
            }
        }, "completion-thread");
        completionThread.start();
        completionThread.join(5_000);
        assertThat(completionThread.isAlive()).isFalse();
        assertThat(failure.get()).isNull();

        assertThat(resultA.awaitBlocking()).isEqualTo("A");
        nextPending(pending).complete("C");
        assertThat(resultC.awaitBlocking()).isEqualTo("C");

        // B never ran; C observed its own context even though it ran on A's completion thread
        assertThat(observedContexts).containsExactly("A", "C");
        assertThat(invocationCount).hasValue(2);
    }

    // --- metrics --------------------------------------------------------------

    @Test
    public void withdrawalRecordsOneAcceptedNoRejectionAndWaitingDuration() throws Throwable {
        BlockingQueue<Completer<String>> pending = new LinkedBlockingQueue<>();
        AtomicInteger invocationCount = new AtomicInteger();
        Bulkhead<String> bulkhead = new Bulkhead<>(pendingInvocation(pending, invocationCount),
                "test", 1, 1, false);
        RecordingMetricsRecorder recorder = new RecordingMetricsRecorder();
        MetricsCollector<String> metrics = new MetricsCollector<>(bulkhead, recorder,
                new AsyncBulkheadOperation());

        TestTimer timerA = new TestTimer();
        TestTimer timerB = new TestTimer();

        Future<String> resultA = new Timeout<>(metrics, "test", 1000, timerA).apply(async(null));
        Future<String> resultB = new Timeout<>(metrics, "test", 1000, timerB).apply(async(null));

        assertThat(bulkhead.getQueueSize()).isEqualTo(1);
        assertThat(recorder.currentWaiting()).isEqualTo(1);

        timerB.executeSynchronously(timerB.nextScheduledTask());
        assertThatThrownBy(resultB::awaitBlocking).isExactlyInstanceOf(TimeoutException.class);

        // both calls were accepted once and the withdrawal added no rejection
        assertThat(recorder.accepted).hasValue(2);
        assertThat(recorder.rejected).hasValue(0);
        // the waiting gauge decremented when B was withdrawn, and B's waiting duration is recorded
        // (A also records a ~zero-duration waiting pair when it starts running), because B really
        // did wait in the queue before being withdrawn
        assertThat(recorder.currentWaiting()).isZero();
        assertThat(recorder.waitingDurations).hasValue(2);
        // B never ran, so it has no running duration (A is still running and reports one on completion)
        assertThat(recorder.runningDurations).hasValue(0);

        nextPending(pending).complete("A");
        assertThat(resultA.awaitBlocking()).isEqualTo("A");
        assertThat(invocationCount).hasValue(1);
        // only A ever ran
        assertThat(recorder.runningDurations).hasValue(1);
    }

    // --- concurrency stress ---------------------------------------------------

    @Test
    public void concurrentWithdrawalAndDequeueNeverLeaksOrDoublesPermits() throws Throwable {
        int size = 4;
        int queueSize = 20;
        int rejectedCount = 16;
        int acceptedCount = size + queueSize;
        int totalCalls = acceptedCount + rejectedCount;
        int deterministicTimeouts = 8; // withdrawn before any concurrent activity
        int racingTimeouts = 7; // fired concurrently with dequeue

        for (int iteration = 0; iteration < 20; iteration++) {
            BlockingQueue<Completer<String>> pending = new LinkedBlockingQueue<>();
            AtomicInteger invocationCount = new AtomicInteger();
            Bulkhead<String> bulkhead = new Bulkhead<>(pendingInvocation(pending, invocationCount),
                    "stress", size, queueSize, false);

            List<TestTimer> timers = new CopyOnWriteArrayList<>();
            List<Future<String>> results = new CopyOnWriteArrayList<>();
            List<EventCounters> allCounters = new CopyOnWriteArrayList<>();

            for (int i = 0; i < totalCalls; i++) {
                TestTimer timer = new TestTimer();
                timers.add(timer);
                FaultToleranceContext<String> ctx = async(null);
                allCounters.add(registerEventCounters(ctx));
                // a rejected call is returned as an already failed future, not thrown
                results.add(new Timeout<>(bulkhead, "stress", 1000, timer).apply(ctx));
            }

            // `size` tasks run and hold one completer each; the other accepted tasks wait in the queue
            assertThat(results).hasSize(totalCalls);
            assertThat(bulkhead.getQueueSize()).isEqualTo(queueSize);
            assertThat(bulkhead.getAvailableCapacityPermits()).isZero();
            assertThat(invocationCount.get()).isEqualTo(size);

            // phase 1: withdraw queued tasks while no work permit is released, so the outcome is
            // deterministic (every targeted task is still queued). This also bounds the expected
            // number of method executions regardless of how the races in phase 2 resolve.
            for (int i = 0; i < deterministicTimeouts; i++) {
                TestTimer timer = timers.get(size + i);
                timer.executeSynchronously(timer.nextScheduledTask());
            }
            assertThat(bulkhead.getQueueSize()).isEqualTo(queueSize - deterministicTimeouts);
            assertThat(bulkhead.getAvailableCapacityPermits()).isEqualTo(deterministicTimeouts);

            // phase 2: timeout withdrawals and dequeues race. Timeout threads target the remaining
            // queued timers, while completion threads release the running tasks (which dequeues and
            // runs queued tasks on the completing thread). A barrier starts every thread together.
            int totalCapacity = size + queueSize;
            int timeoutThreads = 4;
            int completionThreads = size;
            int participants = timeoutThreads + completionThreads;
            java.util.concurrent.CyclicBarrier barrier = new java.util.concurrent.CyclicBarrier(participants);
            CountDownLatch done = new CountDownLatch(participants);
            List<Thread> threads = new java.util.ArrayList<>();
            AtomicReference<Throwable> workerFailure = new AtomicReference<>();

            for (int t = 0; t < timeoutThreads; t++) {
                int from = t * (racingTimeouts / timeoutThreads);
                int to = (t == timeoutThreads - 1) ? racingTimeouts : (t + 1) * (racingTimeouts / timeoutThreads);
                Thread thread = new Thread(() -> {
                    try {
                        barrier.await();
                        for (int i = from; i < to; i++) {
                            TestTimer timer = timers.get(size + deterministicTimeouts + i);
                            TimerTask task = timer.nextScheduledTask();
                            if (task != null) {
                                timer.executeSynchronously(task);
                            }
                        }
                    } catch (Throwable e) {
                        workerFailure.compareAndSet(null, e);
                    } finally {
                        done.countDown();
                    }
                }, "stress-timeout-" + t);
                threads.add(thread);
                thread.start();
            }
            for (int t = 0; t < completionThreads; t++) {
                Thread thread = new Thread(() -> {
                    try {
                        barrier.await();
                        // keep finishing running tasks until every accepted task has either been
                        // withdrawn or completed; full capacity permits is the terminal condition
                        while (bulkhead.getAvailableCapacityPermits() < totalCapacity) {
                            Completer<String> completer = pending.poll(100, TimeUnit.MILLISECONDS);
                            if (completer != null) {
                                completer.complete("ok");
                            }
                        }
                    } catch (Throwable e) {
                        workerFailure.compareAndSet(null, e);
                    } finally {
                        done.countDown();
                    }
                }, "stress-complete-" + t);
                threads.add(thread);
                thread.start();
            }

            assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
            for (Thread thread : threads) {
                thread.join(5_000);
                assertThat(thread.isAlive()).isFalse();
            }
            assertThat(workerFailure.get()).isNull();

            // the completion threads already drained every running task to the terminal idle state
            assertThat(bulkhead.getQueueSize()).isZero();
            assertThat(bulkhead.getAvailableCapacityPermits()).isEqualTo(totalCapacity);
            assertThat(pending).isEmpty();

            // every accepted invocation finishes with a value or a timeout; rejected ones fail
            // with BulkheadException. Awaiting each result proves no future was left pending.
            int timeoutsObserved = 0;
            int rejectionsObserved = 0;
            for (Future<String> result : results) {
                try {
                    assertThat(result.awaitBlocking()).isEqualTo("ok");
                } catch (TimeoutException e) {
                    timeoutsObserved++;
                } catch (BulkheadException e) {
                    rejectionsObserved++;
                }
            }
            assertThat(rejectionsObserved).isEqualTo(rejectedCount);
            assertThat(timeoutsObserved).isLessThanOrEqualTo(deterministicTimeouts + racingTimeouts);
            assertThat(timeoutsObserved).isGreaterThanOrEqualTo(deterministicTimeouts);

            // aggregate the per-context bulkhead events
            int startedRunning = 0;
            int finishedRunning = 0;
            int startedWaiting = 0;
            int finishedWaiting = 0;
            int accepted = 0;
            int rejected = 0;
            for (EventCounters counters : allCounters) {
                startedRunning += counters.startedRunning.get();
                finishedRunning += counters.finishedRunning.get();
                startedWaiting += counters.startedWaiting.get();
                finishedWaiting += counters.finishedWaiting.get();
                accepted += counters.accepted.get();
                rejected += counters.rejected.get();
            }

            // the guarded method body runs exactly once per StartedRunning, never for a withdrawn task
            assertThat(invocationCount.get()).isEqualTo(startedRunning);
            // running and waiting event pairs stay balanced regardless of the timeout/dequeue races
            assertThat(finishedRunning).isEqualTo(startedRunning);
            assertThat(finishedWaiting).isEqualTo(startedWaiting);
            assertThat(accepted).isEqualTo(acceptedCount);
            assertThat(rejected).isEqualTo(rejectedCount);
            // phase 1 alone guarantees deterministicTimeouts withdrawals, so strictly fewer
            // tasks run than were accepted, and every withdrawal skipped the guarded method
            assertThat(startedRunning).isLessThanOrEqualTo(acceptedCount - deterministicTimeouts);

            // invariants: no permit lost or doubled, queue drained, every executed method finished
            assertThat(bulkhead.getQueueSize()).isZero();
            assertThat(bulkhead.getAvailableCapacityPermits()).isEqualTo(size + queueSize);
            assertThat(pending).isEmpty();
        }
    }

    private static final class RecordingMetricsRecorder implements MetricsRecorder {
        final AtomicInteger accepted = new AtomicInteger();
        final AtomicInteger rejected = new AtomicInteger();
        final AtomicInteger waitingDurations = new AtomicInteger();
        final AtomicInteger runningDurations = new AtomicInteger();
        volatile java.util.function.LongSupplier waitingSupplier;

        long currentWaiting() {
            return waitingSupplier.getAsLong();
        }

        @Override
        public void executionFinished(boolean succeeded, boolean fallbackDefined, boolean fallbackApplied,
                long duration) {
        }

        @Override
        public void retryAttempted() {
        }

        @Override
        public void retryValueReturned(boolean retried) {
        }

        @Override
        public void retryExceptionNotRetryable(boolean retried) {
        }

        @Override
        public void retryMaxRetriesReached(boolean retried) {
        }

        @Override
        public void retryMaxDurationReached(boolean retried) {
        }

        @Override
        public void timeoutFinished(boolean timedOut, long time) {
        }

        @Override
        public void circuitBreakerFinished(
                io.smallrye.faulttolerance.core.circuit.breaker.CircuitBreakerEvents.Result result) {
        }

        @Override
        public void circuitBreakerMovedToOpen() {
        }

        @Override
        public void registerCircuitBreakerIsClosed(java.util.function.BooleanSupplier supplier) {
        }

        @Override
        public void registerCircuitBreakerIsOpen(java.util.function.BooleanSupplier supplier) {
        }

        @Override
        public void registerCircuitBreakerIsHalfOpen(java.util.function.BooleanSupplier supplier) {
        }

        @Override
        public void registerCircuitBreakerTimeSpentInClosed(java.util.function.LongSupplier supplier) {
        }

        @Override
        public void registerCircuitBreakerTimeSpentInOpen(java.util.function.LongSupplier supplier) {
        }

        @Override
        public void registerCircuitBreakerTimeSpentInHalfOpen(java.util.function.LongSupplier supplier) {
        }

        @Override
        public void bulkheadDecisionMade(boolean accepted) {
            if (accepted) {
                this.accepted.incrementAndGet();
            } else {
                this.rejected.incrementAndGet();
            }
        }

        @Override
        public void registerBulkheadExecutionsRunning(java.util.function.LongSupplier supplier) {
        }

        @Override
        public void registerBulkheadExecutionsWaiting(java.util.function.LongSupplier supplier) {
            this.waitingSupplier = supplier;
        }

        @Override
        public void updateBulkheadRunningDuration(long time) {
            runningDurations.incrementAndGet();
        }

        @Override
        public void updateBulkheadWaitingDuration(long time) {
            waitingDurations.incrementAndGet();
        }

        @Override
        public void rateLimitDecisionMade(boolean permitted) {
        }
    }

    private static final class AsyncBulkheadOperation implements MeteredOperation {
        @Override
        public boolean enabled() {
            return true;
        }

        @Override
        public boolean mayBeAsynchronous() {
            return true;
        }

        @Override
        public boolean hasBulkhead() {
            return true;
        }

        @Override
        public int bulkheadLimit() {
            return 1;
        }

        @Override
        public int bulkheadQueueSize() {
            return 1;
        }

        @Override
        public boolean hasCircuitBreaker() {
            return false;
        }

        @Override
        public long circuitBreakerDelay() {
            return 0;
        }

        @Override
        public int circuitBreakerRequestVolumeThreshold() {
            return 0;
        }

        @Override
        public double circuitBreakerFailureRatio() {
            return 0;
        }

        @Override
        public int circuitBreakerSuccessThreshold() {
            return 0;
        }

        @Override
        public boolean hasFallback() {
            return false;
        }

        @Override
        public boolean hasRateLimit() {
            return false;
        }

        @Override
        public int rateLimitLimit() {
            return 0;
        }

        @Override
        public long rateLimitWindow() {
            return 0;
        }

        @Override
        public long rateLimitMinSpacing() {
            return 0;
        }

        @Override
        public boolean hasRetry() {
            return false;
        }

        @Override
        public int retryMaxRetries() {
            return 0;
        }

        @Override
        public long retryMaxDuration() {
            return 0;
        }

        @Override
        public boolean hasTimeout() {
            return false;
        }

        @Override
        public long timeoutDuration() {
            return 0;
        }

        @Override
        public String name() {
            return "mock";
        }

        @Override
        public Object cacheKey() {
            return name();
        }
    }
}
