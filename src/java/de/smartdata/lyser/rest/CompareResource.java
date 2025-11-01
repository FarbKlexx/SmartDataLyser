package de.smartdata.lyser.rest;

import de.fhbielefeld.scl.logger.Logger;
import de.fhbielefeld.scl.logger.LoggerException;
import de.fhbielefeld.scl.rest.util.ResponseObjectBuilder;
import de.fhbielefeld.smartuser.annotations.SmartUserAuth;
import de.smartdata.lyser.data.SmartDataAccessor;
import de.smartdata.lyser.data.SmartDataAccessorException;
import de.smartdata.lyser.compare.Differ;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.io.Serializable;
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
@Path("compare")
@Tag(name = "Compare", description = "Compare collections")
public class CompareResource implements Serializable {

    public CompareResource() {
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
    @Path("count")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @SmartUserAuth
    @Operation(summary = "Count two collections",
            description = "Compare the collections by size")
    @APIResponse(
            responseCode = "200",
            description = "Compare result")
    @APIResponse(
            responseCode = "404",
            description = "One of the collections could not be found")
    @APIResponse(
            responseCode = "500",
            description = "Internal error")
    public Response count(
            @Parameter(description = "URL of the first table", required = true) @QueryParam("smartdataurl_1") String smartdataurl_1,
            @Parameter(description = "First collection", required = true, example = "mycollection") @QueryParam("collection_1") String collection_1,
            @Parameter(description = "First storage",
                    schema = @Schema(type = STRING, defaultValue = "public")) @QueryParam("storage_1") String storage_1,
            @Parameter(description = "URL of the second table", required = true) @QueryParam("smartdataurl_2") String smartdataurl_2,
            @Parameter(description = "Second collection", required = true, example = "mycollection") @QueryParam("collection_2") String collection_2,
            @Parameter(description = "Second storage",
                    schema = @Schema(type = STRING, defaultValue = "public")) @QueryParam("storage_2") String storage_2,
            @Parameter(description = "Exact calculation", example = "true") @QueryParam("exact") boolean exact) {

        ResponseObjectBuilder rob = new ResponseObjectBuilder();

        boolean onenotfound = false;
        int count1;
        try {
            count1 = new SmartDataAccessor().fetchCount(smartdataurl_1, collection_1, storage_1, exact);
        } catch (SmartDataAccessorException ex) {
            String msg = ex.getLocalizedMessage();
            if (msg.contains("returned status: 404")) {
                rob.addErrorMessage("Collection >" + collection_1 + "< does not exists on >" + smartdataurl_1 + "<");
                count1 = 0;
            } else {
                rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
                rob.addErrorMessage(msg);
                rob.addException(ex);
                return rob.toResponse();
            }
        }

        int count2;
        try {
            count2 = new SmartDataAccessor().fetchCount(smartdataurl_2, collection_2, storage_2, exact);
        } catch (SmartDataAccessorException ex) {
            String msg = ex.getLocalizedMessage();
            if (msg.contains("returned status: 404")) {
                rob.addErrorMessage("Collection >" + collection_1 + "< does not exists on >" + smartdataurl_2 + "<");
                count2 = 0;
            } else {
                rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
                rob.addErrorMessage(msg);
                rob.addException(ex);
                return rob.toResponse();
            }
        }

        if (onenotfound) {
            rob.setStatus(Response.Status.NOT_FOUND);
        }
        if (count1 < count2) {
            rob.add("result", "Collection >" + collection_2 + "< is greater.");
        } else if (count2 < count1) {
            rob.add("result", "Collection >" + collection_1 + "< is greater.");
        } else {
            rob.add("result", "Booth collections are equal");
        }

        rob.setStatus(Response.Status.OK);
        return rob.toResponse();
    }

    @GET
    @Path("diff")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @SmartUserAuth
    @Operation(summary = "Diffs the data of two collections",
            description = "Creates an diff of two collections")
    @APIResponse(
            responseCode = "200",
            description = "Diff of the collections")
    @APIResponse(
            responseCode = "404",
            description = "One of the collections could not be found")
    @APIResponse(
            responseCode = "500",
            description = "Internal error")
    public Response diff(
            @Parameter(description = "URL of the first table", required = true) @QueryParam("smartdataurl_1") String smartdataurl_1,
            @Parameter(description = "First collection", required = true, example = "mycollection") @QueryParam("collection_1") String collection_1,
            @Parameter(description = "First storage",
                    schema = @Schema(type = STRING, defaultValue = "public")) @QueryParam("storage_1") String storage_1,
            @Parameter(description = "URL of the second table", required = true) @QueryParam("smartdataurl_2") String smartdataurl_2,
            @Parameter(description = "Second collection", required = true, example = "mycollection") @QueryParam("collection_2") String collection_2,
            @Parameter(description = "Second storage",
                    schema = @Schema(type = STRING, defaultValue = "public")) @QueryParam("storage_2") String storage_2) {

        ResponseObjectBuilder rob = new ResponseObjectBuilder();

        Differ differ = new Differ();
        differ.diff(smartdataurl_1, collection_1, storage_1, smartdataurl_2, collection_2, storage_2);

        rob.setStatus(Response.Status.OK);
        return rob.toResponse();
    }
}
