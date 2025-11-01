package de.smartdata.lyser.jsonhelper;

import jakarta.json.JsonObjectBuilder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author Florian
 */
public class NumericAggregator {

    private final Map<String, List<Double>> values = new HashMap<>();

    public void add(String key, String rawValue) {
        try {
            double val = Double.parseDouble(rawValue);
            values.computeIfAbsent(key, k -> new ArrayList<>()).add(val);
        } catch (NumberFormatException ignored) {
            // Optional: Logging
        }
    }

    public void writeTo(JsonObjectBuilder builder, Boolean traceability) {
        for (Map.Entry<String, List<Double>> entry : values.entrySet()) {
            String key = entry.getKey();
            List<Double> nums = entry.getValue();

            if (!nums.isEmpty()) {
                double avg = nums.stream().mapToDouble(Double::doubleValue).average().orElse(Double.NaN);
                builder.add(key, avg);
                if (nums.size() > 1 && traceability) {
//                    String joined = nums.stream()
//                            .map(String::valueOf)
//                            .collect(Collectors.joining(", "));
//                    builder.add(key +"_is_avg_from", joined);
                    builder.add(key + "_is_avg_from", nums+"");
                }
            }
        }
    }

    public int getNumberOfValues() {
        return this.values.size();
    }
}
