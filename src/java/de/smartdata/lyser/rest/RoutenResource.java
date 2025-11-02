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
import jakarta.json.JsonArray;
import jakarta.json.JsonObject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.io.Serializable;
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

}
