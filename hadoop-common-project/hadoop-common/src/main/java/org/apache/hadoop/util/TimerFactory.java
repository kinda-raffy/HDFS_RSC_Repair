package org.apache.hadoop.util;

import java.util.HashMap;
import java.util.Map;

public class TimerFactory {
    private static final Map<String, MetricTimer> metricTimers = new HashMap<>();

    public TimerFactory() {
    }

    public static synchronized MetricTimer getTimer(String metric) {
        if (!metricTimers.containsKey(metric)) {
            metricTimers.put(metric, new MetricTimer(metric));
        }
        return metricTimers.get(metric);
    }

    public static synchronized void closeAll() throws Exception {
        for (MetricTimer timer : metricTimers.values()) {
            timer.close();
        }
    }
}
