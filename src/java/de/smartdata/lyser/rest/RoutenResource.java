package de.smartdata.lyser.rest;

import de.fhbielefeld.scl.logger.Logger;
import de.fhbielefeld.scl.logger.LoggerException;
import de.fhbielefeld.scl.logger.message.Message;
import de.fhbielefeld.scl.logger.message.MessageLevel;
import de.fhbielefeld.scl.rest.util.ResponseListBuilder;
import de.fhbielefeld.scl.rest.util.ResponseObjectBuilder;
import de.fhbielefeld.smartuser.annotations.SmartUserAuth;
import de.smartdata.lyser.data.SmartDataAccessor;
import de.smartdata.lyser.data.SmartDataAccessorException;
import de.smartdata.lyser.threads.ActivindexThread;
import de.smartdata.lyser.threads.CountThread;
import jakarta.json.Json;
import jakarta.json.JsonArray;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;
import jakarta.json.JsonValue;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.naming.NamingException;
import org.eclipse.microprofile.openapi.annotations.Operation;
import static org.eclipse.microprofile.openapi.annotations.enums.SchemaType.STRING;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

/**
 * REST interface for executeing imports
 *
 * @author ffehring
 */
@Path("routen")
@Tag(name = "Routen", description = "Routen data")
public class RoutenResource implements Serializable {

    // Stores last calculated values
    public static Map<String, ResponseObjectBuilder> cache_count = new HashMap();
    public static Map<String, ResponseObjectBuilder> cache_activeindex = new HashMap();

    public RoutenResource() {
        // Init logging
        try {
            String moduleName = (String) new javax.naming.InitialContext().lookup("java:module/ModuleName");
            Logger.getInstance("SmartDataLyser", moduleName);
            Logger.setDebugMode(true);
        } catch (LoggerException | NamingException ex) {
            System.err.println("Error init logger: " + ex.getLocalizedMessage());
        }
    }

    @GET
    @Path("whole")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @SmartUserAuth
    @Operation(summary = "Whole statistic",
            description = "Whole statistic for one collection")
    @APIResponse(
            responseCode = "200",
            description = "Compare result")
    @APIResponse(
            responseCode = "404",
            description = "Collection could not be found")
    @APIResponse(
            responseCode = "500",
            description = "Internal error")
    public Response whole(
            @Parameter(description = "SmartData URL", required = true, example = "/SmartData") @QueryParam("smartdataurl") String smartdataurl,
            @Parameter(description = "Collection name", example = "col1") @QueryParam("collection") String collection,
            @Parameter(description = "Storage name", schema = @Schema(type = STRING, defaultValue = "public")) @QueryParam("storage") String storage,
            @Parameter(description = "Any filter statement accepted by SmartData") @QueryParam("filter") List<String> filters,
            @Parameter(description = "Date attribute", example = "ts") @QueryParam("dateattribute") String dateattribute,
            @Parameter(description = "Exact calculation", example = "true") @QueryParam("exact") boolean exact) {

        ResponseObjectBuilder rob = new ResponseObjectBuilder();

        SmartDataAccessor acc = new SmartDataAccessor(smartdataurl);

        if (smartdataurl.startsWith("/")) {
            smartdataurl = "http://localhost:8080" + smartdataurl;
        }

        if (collection == null) {
            rob.setStatus(Response.Status.BAD_REQUEST);
            rob.addErrorMessage("Parameter >collection< is missing.");
            return rob.toResponse();
        }

        try {
            long countStartTS = System.nanoTime();
            // Get number of all datasets
            rob.add("count", acc.fetchCount(smartdataurl, collection, storage, exact));
            long countEndTS = System.nanoTime();
            rob.add("count_exectime", (countEndTS - countStartTS) / 1000000);

            long byteStartTS = System.nanoTime();
            // Get number of all datasets
            rob.add("byte", acc.fetchSize(smartdataurl, collection, storage));
            long byteEndTS = System.nanoTime();
            rob.add("byte_exectime", (byteEndTS - byteStartTS) / 1000000);

            long firstStartTS = System.nanoTime();
            JsonArray firstSets = acc.fetchData(smartdataurl, collection, storage, dateattribute, filters, dateattribute, null, null, dateattribute + ",asc");
            if (firstSets != null) {
                JsonObject firstSet = firstSets.getJsonObject(0);
                if (firstSet != null && firstSet.containsKey(dateattribute)) {
                    rob.add("firstset", firstSet.getString(dateattribute));
                } else {
                    Message msg = new Message("Dataset does not contain attribute >" + dateattribute + "<", MessageLevel.ERROR);
                    Logger.addMessage(msg);
                }
            }
            long firstEndTS = System.nanoTime();
            rob.add("first_exectime", (firstEndTS - firstStartTS) / 1000000);

            long lastStartTS = System.nanoTime();
            JsonArray lastSets = acc.fetchData(smartdataurl, collection, storage, dateattribute, filters, dateattribute, null, null, dateattribute + ",desc");
            if (lastSets != null) {
                JsonObject lastSet = lastSets.getJsonObject(0);
                if (lastSet != null && lastSet.containsKey(dateattribute)) {
                    rob.add("lastset", lastSet.getString(dateattribute));
                } else {
                    Message msg = new Message("Dataset does not contain attribute >" + dateattribute + "<", MessageLevel.ERROR);
                    Logger.addMessage(msg);
                }
            }
            long lastEndTS = System.nanoTime();
            rob.add("last_exectime", (lastEndTS - lastStartTS) / 1000000);
        } catch (SmartDataAccessorException ex) {
            rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            rob.addErrorMessage("Could not get data for >" + collection + "<: " + ex.getLocalizedMessage());
        }

        rob.setStatus(Response.Status.OK);
        return rob.toResponse();
    }
    
    @GET
    @Path("distinctdays")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @SmartUserAuth
    @Operation(summary = "Get distinct days",
            description = "Returns all distinct days from the timestamp column in a collection")
    @APIResponse(
            responseCode = "200",
            description = "List of distinct days")
    @APIResponse(
            responseCode = "400",
            description = "Invalid parameters")
    @APIResponse(
            responseCode = "500",
            description = "Internal error")
    public Response getDistinctDays(
            @Parameter(description = "SmartData URL", required = true, example = "/SmartData") @QueryParam("smartdataurl") String smartdataurl,
            @Parameter(description = "Collection name", example = "sensor_data") @QueryParam("collection") String collection,
            @Parameter(description = "Storage name", schema = @Schema(type = STRING, defaultValue = "public")) @QueryParam("storage") String storage) {

        ResponseObjectBuilder rob = new ResponseObjectBuilder();

        if (smartdataurl.startsWith("/")) {
            smartdataurl = "http://localhost:8080" + smartdataurl;
        }

        if (collection == null) {
            rob.setStatus(Response.Status.BAD_REQUEST);
            rob.addErrorMessage("Parameter >collection< is missing.");
            return rob.toResponse();
        }

        SmartDataAccessor acc = new SmartDataAccessor(smartdataurl);

        try {
            long startTS = System.nanoTime();

            // Get distinct days from the ts column
            List<String> distinctDays = acc.fetchDistinctDays(smartdataurl, collection, storage);

            long endTS = System.nanoTime();
            rob.add("distinct_days", distinctDays);
            rob.add("execution_time_ms", (endTS - startTS) / 1000000);
            rob.add("count", distinctDays.size());

        } catch (SmartDataAccessorException ex) {
            rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            rob.addErrorMessage("Could not get distinct days for >" + collection + "<: " + ex.getLocalizedMessage());
        }

        rob.setStatus(Response.Status.OK);
        return rob.toResponse();
        }
@GET
@Path("dataByDay")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@SmartUserAuth
@Operation(summary = "Get data by day",
        description = "Returns all data from a collection for a specific day including first/last measurement info and total duration")
@APIResponse(responseCode = "200", description = "Data for the specified day")
@APIResponse(responseCode = "400", description = "Invalid parameters")
@APIResponse(responseCode = "500", description = "Internal error")
public Response getDataByDay(
        @Parameter(description = "SmartData URL", required = true, example = "/SmartData") @QueryParam("smartdataurl") String smartdataurl,
        @Parameter(description = "Collection name", example = "sensor_data") @QueryParam("collection") String collection,
        @Parameter(description = "Storage name", schema = @Schema(type = STRING, defaultValue = "public")) @QueryParam("storage") String storage,
        @Parameter(description = "Date attribute", example = "ts") @QueryParam("dateattribute") String dateattribute,
        @Parameter(description = "Day to filter (format: yyyy-MM-dd)", example = "2023-01-15") @QueryParam("day") String day) {

    ResponseObjectBuilder rob = new ResponseObjectBuilder();

    // Parameter Validation (unchanged)
    if (smartdataurl.startsWith("/")) {
        smartdataurl = "http://localhost:8080" + smartdataurl;
    }

    if (collection == null) {
        rob.setStatus(Response.Status.BAD_REQUEST);
        rob.addErrorMessage("Parameter >collection< is missing.");
        return rob.toResponse();
    }

    if (dateattribute == null) {
        dateattribute = "ts"; // Default to "ts" if not provided
    }

    if (day == null) {
        rob.setStatus(Response.Status.BAD_REQUEST);
        rob.addErrorMessage("Parameter >day< is missing.");
        return rob.toResponse();
    }

    try {
        LocalDateTime.parse(day + "T00:00:00"); // Validate date format
    } catch (DateTimeParseException e) {
        rob.setStatus(Response.Status.BAD_REQUEST);
        rob.addErrorMessage("Invalid day format. Expected format: yyyy-MM-dd");
        return rob.toResponse();
    }

    SmartDataAccessor acc = new SmartDataAccessor(smartdataurl);
    long startTS = System.nanoTime();

    // Define time range
    LocalDateTime startOfDay = LocalDateTime.parse(day + "T00:00:00");
    LocalDateTime endOfDay = LocalDateTime.parse(day + "T23:59:59.999");

    try {
        // Fetch data
        JsonArray data = acc.fetchDataSupNull(
                smartdataurl,
                collection,
                storage,
                "*",
                null,
                dateattribute,
                startOfDay,
                endOfDay,
                dateattribute
        );

        // Initialize variables for first/last measurement
        JsonObject firstMeasurement = null;
        JsonObject lastMeasurement = null;
        String firstTimestamp = null;
        String lastTimestamp = null;
        JsonObject firstPosition = null;
        JsonObject lastPosition = null;
        Long durationMs = null; // Duration in milliseconds

        // Process data if available
        if (data != null && !data.isEmpty()) {
            try {
                // First measurement
                JsonValue firstValue = data.get(0);
                if (firstValue != null && firstValue.getValueType() == JsonValue.ValueType.OBJECT) {
                    JsonObject first = firstValue.asJsonObject();
                    firstTimestamp = first.containsKey("ts") ? first.getString("ts") : null;
                    firstMeasurement = first;

                    if (first.containsKey("pos")) {
                        JsonValue posValue = first.get("pos");
                        if (posValue != null && posValue.getValueType() == JsonValue.ValueType.OBJECT) {
                            firstPosition = posValue.asJsonObject();
                        }
                    }
                }

                // Last measurement
                JsonValue lastValue = data.get(data.size() - 1);
                if (lastValue != null && lastValue.getValueType() == JsonValue.ValueType.OBJECT) {
                    JsonObject last = lastValue.asJsonObject();
                    lastTimestamp = last.containsKey("ts") ? last.getString("ts") : null;
                    lastMeasurement = last;

                    if (last.containsKey("pos")) {
                        JsonValue posValue = last.get("pos");
                        if (posValue != null && posValue.getValueType() == JsonValue.ValueType.OBJECT) {
                            lastPosition = posValue.asJsonObject();
                        }
                    }
                }

                // Calculate duration if both timestamps are available
                if (firstTimestamp != null && lastTimestamp != null) {
                    try {
                        // Ersetze Leerzeichen durch "T" und füge "Z" für UTC hinzu
                        String firstIso = firstTimestamp.replace(" ", "T") + "Z";
                        String lastIso = lastTimestamp.replace(" ", "T") + "Z";

                        Instant firstInstant = Instant.parse(firstIso);
                        Instant lastInstant = Instant.parse(lastIso);
                        durationMs = Duration.between(firstInstant, lastInstant).toMillis();
                    } catch (DateTimeParseException e) {
                        System.err.println("Error parsing timestamps: " + e.getMessage());
                    }
                }

            } catch (Exception e) {
                System.err.println("Error processing measurements: " + e.getMessage());
            }
        }

        long endTS = System.nanoTime();

        // Build response
        rob.add("data", data);
        rob.add("execution_time_ms", (endTS - startTS) / 1000000);
        rob.add("count", data != null ? data.size() : 0);

        // Add first/last measurement info if available
        if (firstMeasurement != null || lastMeasurement != null) {
            JsonObjectBuilder firstLastBuilder = Json.createObjectBuilder();

            if (firstMeasurement != null) {
                JsonObjectBuilder firstObj = Json.createObjectBuilder();
                if (firstTimestamp != null) firstObj.add("timestamp", firstTimestamp);
                else firstObj.addNull("timestamp");

                if (firstPosition != null) firstObj.add("position", firstPosition);
                else firstObj.addNull("position");

                firstObj.add("data", firstMeasurement);
                firstLastBuilder.add("first_measurement", firstObj);
            }

            if (lastMeasurement != null) {
                JsonObjectBuilder lastObj = Json.createObjectBuilder();
                if (lastTimestamp != null) lastObj.add("timestamp", lastTimestamp);
                else lastObj.addNull("timestamp");

                if (lastPosition != null) lastObj.add("position", lastPosition);
                else lastObj.addNull("position");

                lastObj.add("data", lastMeasurement);
                firstLastBuilder.add("last_measurement", lastObj);
            }

            rob.add("first_last_measurements", firstLastBuilder.build());
        }

        // Add duration_ms to the main response (not inside first_last_measurements)
        if (durationMs != null) {
            rob.add("duration_ms", durationMs);
        } else {
            rob.add("duration_ms", JsonValue.NULL);
        }

        rob.setStatus(Response.Status.OK);

    } catch (SmartDataAccessorException e) {
        rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
        rob.addErrorMessage("Error fetching data: " + e.getMessage());
    }

    return rob.toResponse();
}

}
