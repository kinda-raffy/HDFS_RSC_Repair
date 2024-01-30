package org.apache.hadoop.util;

import com.sun.jndi.ldap.LdapURL;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

public class MetricTimer implements AutoCloseable {
    private static final ThreadLocal<Deque<Long>> startTimes = ThreadLocal.withInitial(LinkedList::new);
    private final ConcurrentHashMap<String, List<Long>> recordedMetrics = new ConcurrentHashMap<>();
    private final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    private final String metric;
    private final String summaryFile;
    private final FileWriter logFileWriter;

    public MetricTimer(String metric) {
        this.metric = metric;
        String logFile = String.format("Benchmark/%s.log", metric);
        this.summaryFile = String.format("Benchmark/Summary/%s.summary.log", metric);
        try {
            this.logFileWriter = new FileWriter(logFile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void start() {
        try {
            Deque<Long> stack = startTimes.get();
            stack.push(threadMXBean.getCurrentThreadCpuTime());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        // stack.push(System.nanoTime());
    }

    public synchronized void stop(String label) {
        Deque<Long> stack = startTimes.get();
        if (!stack.isEmpty()) {
            long start = stack.pop();
            try {
                long duration = threadMXBean.getCurrentThreadCpuTime() - start;
                // recordedMetrics.computeIfAbsent(label, k -> Collections.synchronizedList(new ArrayList<>())).add(duration);
                writeLog(label, duration);
                recordedMetrics.computeIfAbsent(label, k -> new ArrayList<>()).add(duration);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            // long duration = System.nanoTime() - start;
        }
    }

    private void writeLog(String label, long duration) {
        try {
            double durationSeconds = duration / 1_000_000_000.0;
            logFileWriter.write(String.format("%d (ns)\t%.6f (secs)\t[ %s ]\n", duration, durationSeconds, label));
            logFileWriter.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void writeMetricSummary() {
        try (FileWriter summaryFw = new FileWriter(summaryFile)) {
            String dashLine = new String(new char[50]).replace("\0", "-");
            summaryFw.write(dashLine + "\n");
            summaryFw.write(metric + "'s Operation Summary\n");
            summaryFw.write(dashLine + "\n\n");

            for (Map.Entry<String, List<Long>> entry : recordedMetrics.entrySet()) {
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
