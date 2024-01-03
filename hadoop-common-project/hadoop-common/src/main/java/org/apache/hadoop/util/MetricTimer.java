package org.apache.hadoop.util;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.LinkedList;

public class MetricTimer implements AutoCloseable {
    private static final ThreadLocal<Deque<Long>> startTimes = ThreadLocal.withInitial(LinkedList::new);
    private final ConcurrentHashMap<String, List<Long>> recordedMetrics = new ConcurrentHashMap<>();
    private final String metric;
    private final String logFile;
    private final String summaryFile;

    public MetricTimer(String metric) {
        this.metric = metric;
        this.logFile = String.format("Benchmark/%s.log", metric);
        this.summaryFile = String.format("Benchmark/Summary/%s.summary.log", metric);
    }

    public void start() {
        Deque<Long> stack = startTimes.get();
        stack.push(System.nanoTime());
    }

    public void stop(String label) {
        Deque<Long> stack = startTimes.get();
        if (!stack.isEmpty()) {
            long start = stack.pop();
            long duration = System.nanoTime() - start;
            recordedMetrics.computeIfAbsent(label, k -> new ArrayList<>()).add(duration);
            writeLog(label, duration);
        }
    }

    private void writeLog(String label, long duration) {
        try (FileWriter fw = new FileWriter(logFile, true)) {
            double durationSeconds = duration / 1_000_000_000.0;
            fw.write(String.format("%d (ns)\t%.6f (secs)\t[ %s ]\n", duration, durationSeconds, label));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void writeMetricSummary() {
        try (FileWriter summaryFw = new FileWriter(summaryFile)) {
            summaryFw.write(String.valueOf('-').repeat(50) + "\n");
            summaryFw.write(metric + "'s Operation Summary\n");
            summaryFw.write(String.valueOf('-').repeat(50) + "\n\n");

            for (java.util.Map.Entry<String, List<Long>> entry : recordedMetrics.entrySet()) {
                String label = entry.getKey();
                List<Long> durations = entry.getValue();
                double average_ns = durations.stream().mapToDouble(Long::doubleValue).average().orElse(0);
                long min_ns = durations.stream().min(Long::compare).orElse(0L);
                long max_ns = durations.stream().max(Long::compare).orElse(0L);
                double total_ns = durations.stream().mapToDouble(Long::doubleValue).sum();

                double average_sec = average_ns / 1_000_000_000.0;
                double min_sec = min_ns / 1_000_000_000.0;
                double max_sec = max_ns / 1_000_000_000.0;
                double total_sec = total_ns / 1_000_000_000.0;

                summaryFw.write(String.format("%s\n", label));
                summaryFw.write(String.format(
                        "\tOccurrence: %d\n\tSum: %.2f ns (%.6f s)" +
                                "\n\tAvg: %.2f ns (%.6f s)\n\tMin: %d ns (%.6f s)" +
                                "\n\tMax: %d ns (%.6f s)\n\n",
                        durations.size(), total_ns, total_sec,
                        average_ns, average_sec, min_ns, min_sec, max_ns, max_sec)
                );
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        writeMetricSummary();
    }
}
