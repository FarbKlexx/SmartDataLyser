package de.smartdata.lyser.jsonhelper;

/**
 *
 * @author Florian
 */
import jakarta.json.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class JsonPointerHelper {

    public static String suggestPointer(JsonObject json, String invalidPointer) {
        Set<String> validPaths = new HashSet<>();
        collectPaths(json, "", validPaths);

        String[] segments = invalidPointer.split("/");
        String lastSegment = segments.length > 0 ? segments[segments.length - 1] : "";

        // Erst: nur Pfade, bei denen das letzte Segment ähnlich zum Zielattribut ist
        List<String> filtered = validPaths.stream()
                .filter(p -> {
                    String[] parts = p.split("/");
                    String end = parts.length > 0 ? parts[parts.length - 1] : "";
                    return end.toLowerCase().contains(lastSegment.toLowerCase()) ||
                           levenshtein(end.toLowerCase(), lastSegment.toLowerCase()) <= 2;
                })
                .collect(Collectors.toList());

        // Dann: wähle den ähnlichsten Pfad von diesen
        Optional<String> best = filtered.stream()
                .min(Comparator.comparingInt(p -> levenshtein(invalidPointer, p)));

        // Ergänzend prüfen wir auf [X]-Syntax und schlagen Alternativen vor
        if (best.isEmpty()) {
            List<String> arraySuggestions = suggestArrayAwarePointers(json, invalidPointer);
            if (!arraySuggestions.isEmpty()) {
                return arraySuggestions.get(0); // oder: mehrere vorschlagen
            }
        }

        return best.orElse(null);
    }

    private static void collectPaths(JsonValue val, String path, Set<String> paths) {
        paths.add(path.isEmpty() ? "/" : path);
        switch (val.getValueType()) {
            case OBJECT -> {
                JsonObject obj = val.asJsonObject();
                for (String key : obj.keySet()) {
                    collectPaths(obj.get(key), path + "/" + key, paths);
                }
            }
            case ARRAY -> {
                JsonArray arr = val.asJsonArray();
                for (int i = 0; i < arr.size(); i++) {
                    collectPaths(arr.get(i), path + "/" + i, paths);
                }
            }
            default -> {}
        }
    }

    public static List<String> suggestArrayAwarePointers(JsonObject json, String invalidPointer) {
        List<String> suggestions = new ArrayList<>();
        Pattern bracketPattern = Pattern.compile("\\[(.*?)\\]");
        Matcher matcher = bracketPattern.matcher(invalidPointer);

        if (!matcher.find()) return suggestions;

        String[] parts = invalidPointer.split("/");
        if (parts.length < 4) return suggestions;

        String lastSegment = parts[parts.length - 1];

        // Suche Pfad zum Array
        StringBuilder prefixBuilder = new StringBuilder();
        for (int i = 1; i < parts.length - 3; i++) {
            prefixBuilder.append("/").append(parts[i]);
        }
        String arrayPath = prefixBuilder.append("/").append(parts[parts.length - 3]).toString();

        try {
            JsonPointer arrPointer = Json.createPointer(arrayPath);
            JsonArray array = arrPointer.getValue(json).asJsonArray();

            for (int i = 0; i < array.size(); i++) {
                JsonValue val = array.get(i);
                if (val.getValueType() == JsonValue.ValueType.OBJECT) {
                    JsonObject candidate = val.asJsonObject();
                    JsonObject values = candidate.getJsonObject("values");
                    if (values != null && values.containsKey(lastSegment)) {
                        String suggestion = arrayPath + "/" + i + "/values/" + lastSegment;
                        suggestions.add(suggestion);
                    }
                }
            }
        } catch (Exception ignored) {}

        return suggestions;
    }

    private static int levenshtein(String a, String b) {
        int[] costs = new int[b.length() + 1];
        for (int j = 0; j < costs.length; j++) costs[j] = j;
        for (int i = 1; i <= a.length(); i++) {
            costs[0] = i;
            int nw = i - 1;
            for (int j = 1; j <= b.length(); j++) {
                int cj = Math.min(1 + Math.min(costs[j], costs[j - 1]),
                        a.charAt(i - 1) == b.charAt(j - 1) ? nw : nw + 1);
                nw = costs[j];
                costs[j] = cj;
            }
        }
        return costs[b.length()];
    }
}
