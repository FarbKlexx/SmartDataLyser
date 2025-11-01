package de.smartdata.lyser.aggregate;

import de.ngi.logging.Logger;
import de.ngi.restutils.DateTimeParser;
import de.smartdata.lyser.jsonhelper.JsonPointerHelper;
import de.smartdata.lyser.jsonhelper.NumericAggregator;
import de.smartdata.lyser.jsonhelper.StringAggregator;
import de.smartdata.lyser.rest.dataaggregation.MappingConfig;
import jakarta.json.Json;
import jakarta.json.JsonArray;
import jakarta.json.JsonArrayBuilder;
import jakarta.json.JsonException;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;
import jakarta.json.JsonPointer;
import jakarta.json.JsonReader;
import java.io.StringReader;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * This action collects data from one ore more datasets and merges it into one
 * dataset into another table.
 *
 * @author Florian Fehring
 */
public class TemporalClusterMaker {

    private Map<String, MappingConfig> mapping = new HashMap();

    public void setMapping(Map<String, MappingConfig> mapping) {
        this.mapping = mapping;
    }

    public JsonObject makeCluster(JsonArray sourceData, String datasource_dateattr, Integer cluster_seconds, Boolean traceability, Boolean onlyclosedclusters, Instant calcStartTime) {
        Logger.log();
        JsonObjectBuilder result = Json.createObjectBuilder();

        List<JsonObject> sorted = sourceData.getValuesAs(JsonObject.class).stream()
                .sorted(Comparator.comparing(o -> {
                    String raw = o.getString(datasource_dateattr);
                    Instant ts = DateTimeParser.parseTimestamp(raw).toInstant();
                    return ts;
                }))
                .collect(Collectors.toList());

        Map<Long, List<JsonObject>> clusterMap = new TreeMap<>();
        for (JsonObject obj : sorted) {
            String raw = obj.getString(datasource_dateattr);
            Instant ts = DateTimeParser.parseTimestamp(raw).toInstant();

            // Calculate start of the day
            Instant dayStart = ts.atZone(ZoneOffset.UTC)
                    .toLocalDate()
                    .atStartOfDay(ZoneOffset.UTC)
                    .toInstant();

            long secondsSinceStart = Duration.between(dayStart, ts).getSeconds();
            long clusterKey = secondsSinceStart / cluster_seconds;

//            if (onlyclosedclusters) {
//                // Calculate end time of bucket
//                Instant bucketEnd = dayStart.plusSeconds((clusterKey + 1) * cluster_seconds);
//                // Do not fill bucket because its still open
//                if (bucketEnd.isAfter(calcStartTime)) {
//                    continue;
//                }
//            }
            clusterMap.computeIfAbsent(clusterKey, k -> new ArrayList<>()).add(obj);
        }
        List<List<JsonObject>> clustered = new ArrayList<>(clusterMap.values());

        JsonArrayBuilder clusterErrors = Json.createArrayBuilder();
        JsonArrayBuilder clusterSets = Json.createArrayBuilder();

        for (List<JsonObject> group : clustered) {
            long sumEpochMillis = 0;
            int count = 0;
            for (JsonObject dataset : group) {
                String raw = dataset.getString(datasource_dateattr);
                Instant ts = DateTimeParser.parseTimestamp(raw).toInstant();
                sumEpochMillis += ts.toEpochMilli();
                count++;
            }
            Instant avgTimestamp = Instant.ofEpochMilli(sumEpochMillis / count);
            JsonObjectBuilder extractedSet = Json.createObjectBuilder().add("ts", avgTimestamp.toString());

            int foundValues = 0;
            List<JsonObject> usedSets = new ArrayList<>();
            NumericAggregator numericAggregator = new NumericAggregator();
            StringAggregator stringAggregator = new StringAggregator();

            for (JsonObject sourceDataset : group) {
                for (Map.Entry<String, MappingConfig> mappingEntry : mapping.entrySet()) {
                    MappingConfig mappingEntryValues = mappingEntry.getValue();
                    String targetAttr = mappingEntryValues.target_attr;

                    try {
                        String sourceVal = sourceDataset.getString(mappingEntryValues.source_attr, null);
                        if (sourceVal == null) {
                            clusterErrors.add("Attribute >" + mappingEntryValues.source_attr + "< was not found in dataset >" + sourceDataset.getInt("id") + "<.");
                            continue;
                        }

                        if (mappingEntryValues.source_pointer != null) {
                            try (JsonReader reader = Json.createReader(new StringReader(sourceVal))) {
                                JsonObject value_json = reader.readObject();
                                JsonPointer pointer;
                                try {
                                    pointer = Json.createPointer(mappingEntryValues.source_pointer);
                                } catch (JsonException ex) {
                                    clusterErrors.add("Could not create JsonPointer from >" + mappingEntryValues.source_pointer + "<: " + ex.getLocalizedMessage());
                                    continue;
                                }

                                try {
                                    if (pointer.containsValue(value_json)) {
                                        String valStr = unwrapQuotedString(pointer.getValue(value_json).toString());
                                        if (isNumeric(valStr)) {
                                            numericAggregator.add(targetAttr, valStr);
                                        } else {
                                            stringAggregator.add(targetAttr, valStr);
                                        }
                                        foundValues++;
                                        usedSets.add(sourceDataset);
                                    }
                                } catch (JsonException ex) {
                                    String suggestion = JsonPointerHelper.suggestPointer(value_json, mappingEntryValues.source_pointer);
                                    if (suggestion != null) {
                                        clusterErrors.add(Json.createObjectBuilder()
                                                .add("incorrect", mappingEntryValues.source_pointer)
                                                .add("suggestion", suggestion)
                                                .add("json", value_json));
                                    }
                                }
                            }
                        } else {
                            String cleanedVal = unwrapQuotedString(sourceVal);
                            if (isNumeric(cleanedVal)) {
                                numericAggregator.add(targetAttr, cleanedVal);
                            } else {
                                stringAggregator.add(targetAttr, cleanedVal);
                            }
                            foundValues++;
                            usedSets.add(sourceDataset);
                        }

                    } catch (Exception e) {
                        System.err.println("ERROR occurred: " + e.getLocalizedMessage());
                        e.printStackTrace();
                    }
                }
            }

            if (!usedSets.isEmpty()) {
                numericAggregator.writeTo(extractedSet, traceability);
                stringAggregator.writeTo(extractedSet, traceability);
                // Add used sets for traceability if wanted
                if (traceability) {
                    List<String> insertedIds = new ArrayList<>();
                    JsonArrayBuilder alb = Json.createArrayBuilder();
                    for (JsonObject curSet : usedSets) {
                        String sourceId = curSet.getString("source") + "/" + curSet.getJsonNumber("id").longValue();
                        if (!insertedIds.contains(sourceId)) {
                            alb.add(sourceId);
                            insertedIds.add(sourceId);
                        }
                    }
                    extractedSet.add("usedSets", alb);
                }

                clusterSets.add(extractedSet);
            }
        }

        result.add("clustererrors", clusterErrors);
        result.add("records", clusterSets);
        return result.build();
    }

    /**
     * Numeric detection
     *
     * @param str
     * @return
     */
    public static boolean isNumeric(String str) {
        if (str == null || str.isEmpty()) {
            return false;
        }
        try {
            Double.parseDouble(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    public static String unwrapQuotedString(String input) {
        if (input == null) {
            return null;
        }
        input = input.trim();
        if (input.startsWith("\"") && input.endsWith("\"") && input.length() >= 2) {
            return input.substring(1, input.length() - 1);
        }
        return input;
    }
}
