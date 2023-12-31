package com.panov.workq;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

public class WorkQueue<T, R> {
    private final BlockingQueue<Runnable> taskQueue;
    private final BiFunction<T, WorkQueue<T, R>, R> handler;
    private final int maxQueueSize;
    private final int maxWorkers;
    private final int executionTimeout;
    private final List<Pair<R>> resultsRaw;

    private final AtomicInteger totalTasks;

    public WorkQueue(
            BiFunction<T, WorkQueue<T, R>, R> handler,
            int maxQueueSize,
            int maxWorkers,
            int executionTimeout
    ) {
        this.taskQueue = new LinkedBlockingQueue<>();
        this.handler = handler;
        this.maxQueueSize = maxQueueSize;
        this.maxWorkers = maxWorkers;
        this.executionTimeout = executionTimeout;
        totalTasks = new AtomicInteger(0);
        resultsRaw = new ArrayList<>();
    }

    public synchronized void add(T task) {
        if (taskQueue.size() == maxQueueSize) {
            throw new IllegalStateException("Tasks queue limit is reached");
        }
        int id = totalTasks.getAndIncrement();
        taskQueue.add(() ->
            addResultToList(handler.apply(task, this), id)
        );
    }

    public synchronized void addAll(Collection<T> tasks) {
        if (taskQueue.size() + tasks.size() > maxQueueSize) {
            throw new IllegalStateException("Tasks queue limit is reached");
        }
        tasks.forEach(t -> {
                int id = totalTasks.getAndIncrement();
                taskQueue.add(() ->
                    addResultToList(handler.apply(t, this), id)
                );
            }
        );
    }

    public List<R> execute() throws InterruptedException {
        ThreadPoolExecutor pool = new ThreadPoolExecutor(
                Math.min(3, maxWorkers),
                maxWorkers,
                0,
                TimeUnit.MILLISECONDS,
                taskQueue
        );

        pool.prestartAllCoreThreads();
        pool.shutdown();
        boolean terminated = pool.awaitTermination(executionTimeout, TimeUnit.MILLISECONDS);

        if (!terminated) {
            throw new IllegalStateException("Process has been executing too long");
        }

        return resultsRaw
                .stream()
                .sorted(Comparator.comparing(Pair::getId))
                .map(rr -> rr.element)
                .toList();
    }

    private synchronized void addResultToList(R result, Integer id) {
        resultsRaw.add(new Pair<>(result, id));
    }

    private static class Pair<E> {
        E element;
        Integer id;

        Pair(E element, Integer id) {
            this.element = element;
            this.id = id;
        }

        Integer getId() {
            return id;
        }
    }
}
