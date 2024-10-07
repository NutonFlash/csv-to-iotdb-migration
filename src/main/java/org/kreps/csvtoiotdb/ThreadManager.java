package org.kreps.csvtoiotdb;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Manages a pool of threads for concurrent task execution.
 */
public class ThreadManager {
    private final ExecutorService executorService;

    /**
     * Constructs a ThreadManager instance.
     *
     * @param numThreads The number of threads in the pool.
     */
    public ThreadManager(int numThreads) {
        this.executorService = Executors.newFixedThreadPool(numThreads);
    }

    /**
     * Submits a task to be executed by the thread pool.
     *
     * @param task The Runnable task to execute.
     */
    public void submitTask(Runnable task) {
        executorService.submit(task);
    }

    /**
     * Shuts down the thread pool gracefully, waiting for ongoing tasks to complete.
     */
    public void shutdown() {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
