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
public class StringAggregator {

    private final Map<String, List<String>> values = new HashMap<>();

    public void add(String key, String rawValue) {
        if (rawValue != null && !rawValue.isEmpty()) {
            values.computeIfAbsent(key, k -> new ArrayList<>()).add(rawValue);
        }
    }

    public void writeTo(JsonObjectBuilder builder, Boolean traceability) {
        for (Map.Entry<String, List<String>> entry : values.entrySet()) {
            String key = entry.getKey();
            List<String> strings = entry.getValue();

            if (!strings.isEmpty()) {
                int medianIndex = strings.size() / 2;
                String median = strings.get(medianIndex);
                builder.add(key, median);

                if (strings.size() > 1 && traceability) {
                    builder.add(key + "_is_median_from", strings.toString());
                }
            }
        }
    }

    public int getNumberOfValues() {
        return this.values.size();
    }
}

