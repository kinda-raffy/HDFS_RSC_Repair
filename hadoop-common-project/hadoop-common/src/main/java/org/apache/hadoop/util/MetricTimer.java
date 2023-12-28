package org.apache.hadoop.util;

import javax.annotation.Nullable;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class MetricTimer implements AutoCloseable {
    private final Stack<Long> timerStack = new Stack<>();
    private final HashMap<String, ArrayList<Long>> recordedMetrics = new HashMap<>();
    private final FileWriter fw;
    private final FileWriter summaryFw;
    private final String metric;

    public MetricTimer(String metric) throws IOException {
        this.fw = new FileWriter(String.format("Benchmark/%s.log", metric));
        this.summaryFw = new FileWriter(String.format("Benchmark/Summary/%s.summary.log", metric));
        this.metric = metric;
    }

    public synchronized void start() {
        timerStack.push(System.nanoTime());
    }

    public synchronized void stop(@Nullable String label) {
        long start = timerStack.pop();
        long end = System.nanoTime();
        long duration_ns = end - start;
        double duration_seconds = duration_ns / 1_000_000_000.0;
        try {
            fw.write(String.format("%d (ns)\t%.6f (secs)", duration_ns, duration_seconds));
            int stackDepth = timerStack.size();
            if (label != null) {
                fw.write(String.format("\t[ %d | %s ]\n", stackDepth, label));
            } else {
                fw.write(String.format("\t[ %d ]\n", stackDepth));
            }
            fw.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        recordedMetrics.computeIfPresent(label, (k, v) -> {
            v.add(duration_ns);
            return v;
        });
        recordedMetrics.computeIfAbsent(label, k -> {
            ArrayList<Long> durations = new ArrayList<>();
            durations.add(duration_ns);
            return durations;
        });
    }

    private synchronized void writeMetricSummary() throws IOException {
        summaryFw.write(String.valueOf('-').repeat(50) + "\n");
        summaryFw.write(metric + "'s Operation Summary\n");
        summaryFw.write(String.valueOf('-').repeat(50) + "\n\n");
        for (Map.Entry<String, ArrayList<Long>> entry : recordedMetrics.entrySet()) {
            String label = entry.getKey();
            ArrayList<Long> durations = entry.getValue();
            if (!durations.isEmpty()) {
                double average_ns = durations.stream().mapToLong(Long::longValue).average().orElse(0.0);
                long min_ns = Collections.min(durations);
                long max_ns = Collections.max(durations);

                double average_sec = average_ns / 1_000_000_000.0;
                double min_sec = min_ns / 1_000_000_000.0;
                double max_sec = max_ns / 1_000_000_000.0;

                double total_ns = durations.stream().mapToLong(Long::longValue).sum();
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
        }
        summaryFw.flush();
    }

    @Override
    public synchronized void close() throws Exception {
        writeMetricSummary();
        summaryFw.close();
        fw.close();
    }
}
