package org.apache.hadoop.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TimerFactory {
    private static final Map<String, MetricTimer> metricTimers = new HashMap<>();

    public TimerFactory() {
    }

    public static synchronized MetricTimer getTimer(String metric) {
        try {
            if (!metricTimers.containsKey(metric)) {
                metricTimers.put(metric, new MetricTimer(metric));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return metricTimers.get(metric);
    }

    public static synchronized void closeAll() throws Exception {
        for (MetricTimer timer : metricTimers.values()) {
            timer.close();
        }
    }
}