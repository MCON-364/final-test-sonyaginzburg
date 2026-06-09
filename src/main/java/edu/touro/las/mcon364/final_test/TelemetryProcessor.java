package edu.touro.las.mcon364.final_test;

import java.util.ArrayList;
import java.util.DoubleSummaryStatistics;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

/**
 * TelemetryProcessor – concurrent sensor-data pipeline
 *
 * Scenario: a fleet of devices continuously emits telemetry readings.
 * Each reading is represented as a {@link TelemetryEvent} carrying a device id,
 * a numeric metric value, and a nanosecond timestamp. Readings arrive faster than
 * they can be processed synchronously, so a multi-worker, queue-based pipeline
 * is required.
 *
 * Requirements:
 * - submit(event) enqueues an event so a worker thread can process it.
 *   It must throw {@link IllegalArgumentException} if event is null.
 *   Events submitted before start() is called must be silently discarded.
 * - start(workerCount) spins up {@code workerCount} worker threads that continuously
 *   drain the queue and process events. It must throw {@link IllegalArgumentException}
 *   if workerCount ≤ 0. Calling start() a second time must be a no-op(should make no difference).
 * - stop() signals all workers to finish, waits for them to terminate, then processes
 *   any events still left in the queue before returning.
 * - getTotalProcessed() returns the running total of events fully processed.
 * - getStats() returns a {@link DoubleSummaryStatistics} snapshot of all processed
 *   metric values. Each call must return a fresh, independent object.
 *
 * Thread-safety requirements:
 * - submit() and the read methods (getTotalProcessed, getStats) may be called
 *   concurrently from multiple threads without data loss or corruption.
 * - Use java.util.concurrent building blocks. Do not use raw synchronized blocks.
 */
public class TelemetryProcessor {

    // ── declare whatever fields you need ─────────────────────────────────────
    private final BlockingQueue<TelemetryEvent> queue = new LinkedBlockingQueue<>();

    private final AtomicInteger totalProcessed = new AtomicInteger(0);

    // no sure abt this
    private final AtomicReference<DoubleSummaryStatistics> events =  new AtomicReference<>(new DoubleSummaryStatistics());

    // volatile to ensure all threads see the latest value of running
    private volatile boolean running = true;

    private ExecutorService executorService;

    // ── public API ────────────────────────────────────────────────────────────

    /**
     * Add an event to the processing queue.
     *
     * Events submitted before {@link #start(int)} is called must be silently discarded.
     *
     * @param event the telemetry event to enqueue; must not be null
     * @throws IllegalArgumentException if event is null
     */
    public void submit(TelemetryEvent event) {
        // implement this method
        if (running) {
            queue.offer(event);
        }
    }

    /**
     * Start processing events.
     * @param workerCount number of worker threads to create; must be ≥ 1
     * @throws IllegalArgumentException if workerCount ≤ 0
     */
    public void start(int workerCount) {
        // implement this method
        if (workerCount <= 0) {
            throw new IllegalArgumentException("workerCount must be greater than 0");
        }
        IntStream.range(0, workerCount).forEach(i -> {
            executorService.submit(this::workerLoop);
        });
    }


    /**
     * Stop processing events.
     * @throws InterruptedException if the calling thread is interrupted while waiting
     */
    public void stop() throws InterruptedException {
        // ?
    }

    private void process(TelemetryEvent event) {
// ?
    }

    /**
     * Return the total number of events that have been fully processed.
     */
    public int getTotalProcessed() {
        // implement this method
        return totalProcessed.get();
    }

    /**
     * Return a point-in-time snapshot of summary statistics for all processed
     * metric values (count, sum, min, max, average).
     *
     * Each call must return a <em>new</em>, independent {@link DoubleSummaryStatistics}
     * object so that callers cannot corrupt the internal state.
     *
     */
    public DoubleSummaryStatistics getStats() {
        // implement this method
        DoubleSummaryStatistics snapshot = new DoubleSummaryStatistics();
              snapshot.combine(events.get());
              return snapshot;
    }

    private void workerLoop() {
        while (running || !queue.isEmpty()) {
            try {
                TelemetryEvent e = queue.poll(100, java.util.concurrent.TimeUnit.MILLISECONDS);
                if (e != null) {
                    process(e);
                }
            } catch (Exception e) {
                //execptions
                e.printStackTrace();
            }
        }
    }
}
