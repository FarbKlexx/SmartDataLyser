package de.smartdata.lyser.rest;

import de.fhbielefeld.scl.logger.Logger;
import de.fhbielefeld.scl.logger.LoggerException;
import de.fhbielefeld.scl.rest.util.ResponseObjectBuilder;
import de.fhbielefeld.smartuser.annotations.SmartUserAuth;
import de.smartdata.lyser.check.CompletenessChecker;
import de.smartdata.lyser.data.SmartDataAccessorException;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.List;
import javax.naming.NamingException;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

/**
 * REST interface for checking collections
 *
 * @author ffehring
 */
@Path("check")
@Tag(name = "Check", description = "Check data")
public class CheckResource implements Serializable {

    public CheckResource() {
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
    @Path("completeness")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @SmartUserAuth
    @Operation(summary = "Completness",
            description = "Calculates completeness informations")
    @APIResponse(
            responseCode = "200",
            description = "Completeness informations")
    @APIResponse(
            responseCode = "404",
            description = "The collection could not be found")
    @APIResponse(
            responseCode = "500",
            description = "Internal error")
    public Response completeness(
            @Parameter(description = "SmartData URL", required = true) @QueryParam("smartdataurl") String smartdataurl,
            @Parameter(description = "Collection", required = true, example = "mycollection") @QueryParam("collection") String collection,
            @Parameter(description = "Storage", required = false, example = "public") @QueryParam("storage") String storage,
            @Parameter(description = "Any filter statement accepted by SmartData") @QueryParam("filter") List<String> filters,
            @Parameter(description = "Date attribute", required = true, example = "ts") @QueryParam("dateattribute") String dateattribute,
            @Parameter(description = "Start date", example = "2020-12-24T18:00") @QueryParam("start") String start,
            @Parameter(description = "End date", example = "2020-12-24T19:00") @QueryParam("end") String end,
            @Parameter(description = "Measurement frequence (in seconds)", example = "10") @QueryParam("measurefreq") Long measurefreq) {

        ResponseObjectBuilder rob = new ResponseObjectBuilder();

        if (smartdataurl.startsWith("/")) {
            smartdataurl = "http://localhost:8080" + smartdataurl;
        }

        LocalDateTime startDT;
        if (start != null) {
            startDT = LocalDateTime.parse(start);
        } else {
            startDT = LocalDateTime.now().minusDays(30);
        }

        LocalDateTime endDT;
        if (end != null) {
            endDT = LocalDateTime.parse(end);
        } else {
            endDT = LocalDateTime.now();
        }

        if (measurefreq == null) {
            measurefreq = 10L;
        }

        CompletenessChecker cc = new CompletenessChecker();
        try {
            rob = cc.checkTimeCompleteness(smartdataurl, collection, storage, filters, dateattribute, measurefreq, 10L, startDT, endDT);
        } catch (SmartDataAccessorException ex) {
            rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            rob.addErrorMessage("Could not check completeness: " + ex.getLocalizedMessage());
        }

        rob.setStatus(Response.Status.OK);
        return rob.toResponse();
    }

    @GET
    @Path("missingperiods")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @SmartUserAuth
    @Operation(summary = "Missing periods",
            description = "Calculates the periods where data is missing")
    @APIResponse(
            responseCode = "200",
            description = "List of periods with missing data")
    @APIResponse(
            responseCode = "404",
            description = "The collection could not be found")
    @APIResponse(
            responseCode = "500",
            description = "Internal error")
    public Response missingperiods(
            @Parameter(description = "SmartData URL", required = true) @QueryParam("smartdataurl") String smartdataurl,
            @Parameter(description = "Collection", required = true, example = "mycollection") @QueryParam("collection") String collection,
            @Parameter(description = "Storage", required = false, example = "public") @QueryParam("storage") String storage,
            @Parameter(description = "Any filter statement accepted by SmartData") @QueryParam("filter") List<String> filters,
            @Parameter(description = "Date attribute", required = true, example = "ts") @QueryParam("dateattribute") String dateattribute,
            @Parameter(description = "Start date", example = "2020-12-24T18:00") @QueryParam("start") String start,
            @Parameter(description = "End date", example = "2020-12-24T19:00") @QueryParam("end") String end,
            @Parameter(description = "Measurement frequence (in seconds)", example = "10") @QueryParam("measurefreq") Long measurefreq) {

        ResponseObjectBuilder rob = new ResponseObjectBuilder();

        if (smartdataurl.startsWith("/")) {
            smartdataurl = "http://localhost:8080" + smartdataurl;
        }

        LocalDateTime startDT;
        if (start != null) {
            startDT = LocalDateTime.parse(start);
        } else {
            startDT = LocalDateTime.now().minusDays(30);
        }

        LocalDateTime endDT;
        if (end != null) {
            endDT = LocalDateTime.parse(end);
        } else {
            endDT = LocalDateTime.now();
        }

        if (measurefreq == null) {
            measurefreq = 10L;
        }

        CompletenessChecker cc = new CompletenessChecker();
        try {
            rob.add(cc.checkMissingPeriods(smartdataurl, collection, storage, filters, dateattribute, measurefreq, 10L, startDT, endDT));
        } catch (SmartDataAccessorException ex) {
            rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            rob.addErrorMessage("Could not check completeness: " + ex.getLocalizedMessage());
        }
        if(cc.getSmartdataRequest() != null)
            rob.add("smartdataRequest", cc.getSmartdataRequest());
        rob.setStatus(Response.Status.OK);
        return rob.toResponse();
    }

    @GET
    @Path("hassetintime")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @SmartUserAuth
    @Operation(summary = "Has a new set",
            description = "Checks if the collection has a new set in the last x seconds.")
    @APIResponse(
            responseCode = "200",
            description = "List of periods with missing data")
    @APIResponse(
            responseCode = "404",
            description = "The collection could not be found")
    @APIResponse(
            responseCode = "500",
            description = "Internal error")
    public Response hasSetInTime(
            @Parameter(description = "SmartData URL", required = true) @QueryParam("smartdataurl") String smartdataurl,
            @Parameter(description = "Collection", required = true, example = "mycollection") @QueryParam("collection") String collection,
            @Parameter(description = "Storage", required = false, example = "public") @QueryParam("storage") String storage,
            @Parameter(description = "Any filter statement accepted by SmartData") @QueryParam("filter") List<String> filters,
            @Parameter(description = "Date attribute", required = true, example = "ts") @QueryParam("dateattribute") String dateattribute,
            @Parameter(description = "Start date", example = "2020-12-24T18:00") @QueryParam("start") String start,
            @Parameter(description = "End date", example = "2020-12-24T19:00") @QueryParam("end") String end,
            @Parameter(description = "Past seconds", example = "60") @QueryParam("pastseconds") Long pastseconds) {

        if (smartdataurl.startsWith("/")) {
            smartdataurl = "http://localhost:8080" + smartdataurl;
        }

        LocalDateTime endDT;
        if (end != null) {
            endDT = LocalDateTime.parse(end);
        } else {
            endDT = LocalDateTime.now();
        }

        LocalDateTime startDT;
        if (pastseconds != null) {
            startDT = LocalDateTime.now().minusSeconds(pastseconds);
        } else if (start != null) {
            startDT = LocalDateTime.parse(start);
        } else {
            startDT = LocalDateTime.now().minusDays(30);
        }

        CompletenessChecker cc = new CompletenessChecker();
        try {
            ResponseObjectBuilder rob = cc.hasSetInTime(smartdataurl, collection, storage, filters, dateattribute, startDT, endDT);
            return rob.toResponse();
        } catch (SmartDataAccessorException ex) {
            ResponseObjectBuilder rob = new ResponseObjectBuilder();
            rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            rob.addErrorMessage("Could not check completeness: " + ex.getLocalizedMessage());
            return rob.toResponse();
        }
    }
}
