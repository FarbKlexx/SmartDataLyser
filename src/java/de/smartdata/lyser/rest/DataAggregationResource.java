package de.smartdata.lyser.rest;

import de.fhbielefeld.scl.logger.Logger;
import de.fhbielefeld.scl.logger.LoggerException;
import de.fhbielefeld.scl.rest.util.ResponseListBuilder;
import de.fhbielefeld.scl.rest.util.ResponseObjectBuilder;
import de.fhbielefeld.smartuser.annotations.SmartUserAuth;
import de.smartdata.lyser.aggregate.TemporalClusterMaker;
import de.smartdata.lyser.data.SmartDataAccessor;
import de.smartdata.lyser.data.SmartDataAccessorException;
import de.smartdata.lyser.rest.dataaggregation.ClusterRequest;
import de.smartdata.lyser.rest.dataaggregation.DatasourceConfig;
import jakarta.json.Json;
import jakarta.json.JsonArray;
import jakarta.json.JsonArrayBuilder;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.Map;
import javax.naming.NamingException;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

/**
 * REST interface for createing data aggregations
 *
 * @author ffehring
 */
@Path("aggregation")
@Tag(name = "Aggregation", description = "Aggregate data")
public class DataAggregationResource implements Serializable {

    // Stores last calculated values
    public static Map<String, ResponseObjectBuilder> cache_count = new HashMap();
    public static Map<String, ResponseObjectBuilder> cache_activeindex = new HashMap();

    public DataAggregationResource() {
        // Init logging
        try {
            String moduleName = (String) new javax.naming.InitialContext().lookup("java:module/ModuleName");
            Logger.getInstance("SmartDataLyser", moduleName);
            Logger.setDebugMode(true);
        } catch (LoggerException | NamingException ex) {
            System.err.println("Error init logger: " + ex.getLocalizedMessage());
        }
    }

    @POST
    @Path("temporalcluster")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @SmartUserAuth
    @Operation(summary = "Temporal clusting",
            description = "Builds temporal clusters from datasets that are in the same timeslot.")
    @APIResponse(
            responseCode = "200",
            description = "Count result")
    @APIResponse(
            responseCode = "404",
            description = "One of the collections could not be found")
    @APIResponse(
            responseCode = "500",
            description = "Internal error")
    public Response temporalcluster(ClusterRequest request) {

        ResponseObjectBuilder rob = new ResponseObjectBuilder();

        // Get algorithm start time
        Instant calcStartTime = Instant.now();

        // Check if timerange is given and parse it
        LocalDateTime startDate = LocalDateTime.of(1970, 1, 1, 0, 0);
        LocalDateTime endDate = null;
        try {
            if (request.start != null) {
                startDate = LocalDateTime.parse(request.start);
            }
        } catch (DateTimeParseException ex) {
            rob.addErrorMessage("Could not parse start date: " + ex.getLocalizedMessage());
            rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            return rob.toResponse();
        }
        try {
            if (request.end != null) {
                endDate = LocalDateTime.parse(request.end);
            }
        } catch (DateTimeParseException ex) {
            rob.addErrorMessage("Could not parse end date: " + ex.getLocalizedMessage());
            rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            return rob.toResponse();
        }

        // Set default values if needed
        if (request.traceability == null) {
            request.traceability = false;
        }
        if (request.cluster_seconds == null) {
            request.cluster_seconds = 30;
        }
        if (request.onlyclosedclusters == null) {
            request.onlyclosedclusters = true;
        }
        if (request.date_attr == null) {
            request.date_attr = "ts";
        }

        // Recalculate endpoint when only closed clusters should be build
        if (request.onlyclosedclusters) {
            ZoneId zone = ZoneId.systemDefault();
            Instant startInstant = startDate.atZone(zone).toInstant();
            Instant now = Instant.now();
            long elapsedSeconds = Duration.between(startInstant, now).getSeconds();
            long completedWindows = elapsedSeconds / request.cluster_seconds;
            Instant lastWindowClose = startInstant.plusSeconds(completedWindows * request.cluster_seconds);
            endDate = LocalDateTime.ofInstant(lastWindowClose, zone);
            rob.add("cluster_until", endDate);
        }
        
        JsonArrayBuilder dataBuilder = Json.createArrayBuilder();
        JsonArrayBuilder inputDataSetsUrlsBuilder = Json.createArrayBuilder();
        int inputDataSetsCount = 0;
        // Get data from each datasource
        for (DatasourceConfig curSource : request.datasources) {
            // Check and autocorrect values if possible
            if (curSource.smartdataurl == null) {
                rob.setStatus(Response.Status.BAD_REQUEST);
                rob.addErrorMessage("Parameter >smartdataurl< is missing.");
                return rob.toResponse();
            }

            if (curSource.smartdataurl.startsWith("/")) {
                curSource.smartdataurl = "http://localhost:8080" + curSource.smartdataurl;
            }

            if (curSource.collection == null) {
                rob.setStatus(Response.Status.BAD_REQUEST);
                rob.addErrorMessage("Parameter >collection< is missing.");
                return rob.toResponse();
            }

            if (curSource.dateattribute == null) {
                curSource.dateattribute = "ts";
            }
            if(curSource.order == null) {
                curSource.order = curSource.dateattribute + " ASC";
            }
            if (curSource.limit == null) {
                curSource.limit = 100L;
            }

            // SmartDataAccessor accesses the SmartData either by URL (smartdataurl) or if locally available by useing the JDBC-Resource defined in the given SmartData Instance
            JsonArray curData;
            SmartDataAccessor acc = new SmartDataAccessor(curSource.smartdataurl);

            try {
                curData = acc.fetchData(curSource.smartdataurl, curSource.collection, curSource.storage, curSource.includes, curSource.filter, request.date_attr, startDate, endDate, curSource.order);
                inputDataSetsCount += curData.size();
                inputDataSetsUrlsBuilder.add(acc.getSmartdataRequest());
                // Check if there is data
                if (curData.isEmpty()) {
                    rob.addWarningMessage("There was no data for aggregation found from source >" + curSource.smartdataurl + "/" + curSource.collection + "<.");
                } else {
                    String sourceId = curSource.smartdataurl
                            + ";" + curSource.collection
                            + ";" + curSource.storage;

                    curData.forEach(json -> {
                        if (json instanceof JsonObject) {
                            JsonObject original = (JsonObject) json;
                            JsonObjectBuilder extended = Json.createObjectBuilder(original);
                            extended.add("source", sourceId);
                            dataBuilder.add(extended.build());
                        } else {
                            rob.addWarningMessage("Unexpected non-object element in dataset.");
                        }
                    });
                }
            } catch (SmartDataAccessorException ex) {
                rob.addErrorMessage("Error fetching data from source: " + ex.getLocalizedMessage());
                rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
                return rob.toResponse();
            }
        }
        rob.add("inputDatasetsCount", inputDataSetsCount);
        rob.add("inputDatasetsUrls", inputDataSetsUrlsBuilder.build());

        JsonArray data = dataBuilder.build();

        // Create TemporalClusterer Object
        TemporalClusterMaker tcm = new TemporalClusterMaker();
        tcm.setMapping(request.mapping);
        JsonObject result = tcm.makeCluster(data, request.date_attr, request.cluster_seconds, request.traceability, request.onlyclosedclusters, calcStartTime);

        ResponseListBuilder sourceDatasets = new ResponseListBuilder();
        for (int i = 0; i < data.size(); i++) {
            JsonObject obj = data.getJsonObject(i);
            sourceDatasets.add(obj);

            // If marking sets is activated
            if (request.processed_attr != null) {
                try {
                    JsonObjectBuilder updateSet = Json.createObjectBuilder();
                    updateSet.add("id", obj.getJsonNumber("id").longValue());
                    updateSet.add(request.processed_attr, true);
                    String sourceId = obj.getString("source", null);
                    String[] sourceIdParts = sourceId.split(";");
                    SmartDataAccessor acc = new SmartDataAccessor(sourceIdParts[0]);
                    // Update with sourceIdParts[0] = SmartDataURL, sourceIdParts[1] = collection, sourceIdParts[2] = storage
                    acc.updateData(sourceIdParts[0], sourceIdParts[1], sourceIdParts[2], updateSet.build());
                } catch (SmartDataAccessorException ex) {
                    rob.addErrorMessage("Could not mark datasets as processed. Error: " + ex.getLocalizedMessage());
                }
            }
        }
        if (request.processed_attr != null) {
            rob.addWarningMessage("Used datasets were marked as processed by setting the attribute >" + request.processed_attr + "< to true.");
        }
        if (request.traceability) {
            rob.add("inputDatasets", sourceDatasets);
        }
        rob.add(result);
        rob.setStatus(Response.Status.OK);
        return rob.toResponse();
    }
}
