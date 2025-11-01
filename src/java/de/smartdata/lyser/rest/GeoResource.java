package de.smartdata.lyser.rest;

import de.fhbielefeld.scl.logger.Logger;
import de.fhbielefeld.scl.logger.LoggerException;
import de.fhbielefeld.scl.rest.util.ResponseObjectBuilder;
import de.fhbielefeld.smartuser.annotations.SmartUserAuth;
import de.smartdata.lyser.data.SmartDataAccessor;
import de.smartdata.lyser.data.SmartDataAccessorException;
import de.smartdata.lyser.distance.Distance;
import jakarta.json.JsonArray;
import jakarta.json.JsonObject;
import jakarta.json.JsonValue;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import javax.naming.NamingException;
import org.eclipse.microprofile.openapi.annotations.Operation;
import static org.eclipse.microprofile.openapi.annotations.enums.SchemaType.STRING;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * REST interface for executeing imports
 *
 * @author ffehring
 */
@Path("geo")
@Tag(name = "Geo", description = "Provides geo calculations on spatial data.")
public class GeoResource implements Serializable {

    // Stores last calculated values
    public static Map<String, ResponseObjectBuilder> cache_count = new HashMap();
    public static Map<String, ResponseObjectBuilder> cache_activeindex = new HashMap();

    public GeoResource() {
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
    @Path("distance")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @SmartUserAuth
    @Operation(summary = "Distance",
            description = "Calculates the distance between all the datasets that are delivered")
    @APIResponse(
            responseCode = "200",
            description = "Distance from first to last point")
    @APIResponse(
            responseCode = "404",
            description = "Collection could not be found")
    @APIResponse(
            responseCode = "500",
            description = "Internal error")
    public Response distance(
            @Parameter(description = "SmartData URL", required = true, example = "/SmartData") @QueryParam("smartdataurl") String smartdataurl,
            @Parameter(description = "Collections name", example = "col1") @QueryParam("collection") String collection,
            @Parameter(description = "Storage name", schema = @Schema(type = STRING, defaultValue = "public")) @QueryParam("storage") String storage,
            @Parameter(description = "Any filter statement accepted by SmartData") @QueryParam("filter") List<String> filters,
            @Parameter(description = "Date attribute (default: ts)", example = "ts") @QueryParam("dateattribute") String dateattr,
            @Parameter(description = "Start date (default: now - 30 days)", example = "2020-12-24T18:00") @QueryParam("start") String start,
            @Parameter(description = "End date (default: now)", example = "2020-12-24T19:00") @QueryParam("end") String end,
            @Parameter(description = "Geo attribute (default: pos)", example = "point") @QueryParam("geoattr") String geoattr) {

        ResponseObjectBuilder rob = new ResponseObjectBuilder();

        SmartDataAccessor acc = new SmartDataAccessor();

        if (smartdataurl == null) {
            rob.setStatus(Response.Status.BAD_REQUEST);
            rob.addErrorMessage("Parameter >smartdataurl< is missing.");
            return rob.toResponse();
        }

        if (smartdataurl.startsWith("/")) {
            smartdataurl = "http://localhost:8080" + smartdataurl;
        }

        if (collection == null) {
            rob.setStatus(Response.Status.BAD_REQUEST);
            rob.addErrorMessage("Parameter >collection< is missing.");
            return rob.toResponse();
        }

        if (dateattr == null) {
            dateattr = "ts";
        }

        if (geoattr == null) {
            geoattr = "pos";
        }

        LocalDateTime endDT;
        if (end != null) {
            endDT = LocalDateTime.parse(end);
        } else {
            endDT = LocalDateTime.now();
        }

        LocalDateTime startDT;
        if (start != null) {
            startDT = LocalDateTime.parse(start);
        } else {
            startDT = LocalDateTime.now().minusDays(30);
        }

        JsonArray data;

        try {
            data = acc.fetchData(smartdataurl, collection, storage, geoattr, filters, dateattr, startDT, endDT, dateattr, null);
        } catch (SmartDataAccessorException ex) {
            rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            rob.addErrorMessage("Could not calculate distance because of error: " + ex.getLocalizedMessage());
            return rob.toResponse();
        }

        double totalDistance = 0;
        Double prevLat = null;
        Double prevLng = null;

        // Walk trough sets
        for (JsonValue curObj : data) {
            JsonObject dateobj = curObj.asJsonObject();

            JsonObject geoobj = dateobj.getJsonObject(geoattr);
            if (geoobj == null) {
                continue;
            }

            JsonArray geoarr = geoobj.getJsonArray("coordinates");
            if (geoarr == null) {
                continue;
            }

            double lat = geoarr.getJsonNumber(0).doubleValue();
            double lng = geoarr.getJsonNumber(1).doubleValue();

            if (prevLat != null && prevLng != null) {
                totalDistance += Distance.calc(lat, lng, prevLat, prevLng);
            }
            prevLat = lat;
            prevLng = lng;
        }

        rob.add("totalKM", totalDistance);
        rob.setStatus(Response.Status.OK);
        return rob.toResponse();
    }

    @GET
    @Path("altitudediff")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @SmartUserAuth
    @Operation(summary = "AltitudeDiff",
            description = "Calculates the altidude difference sum between all the datasets that are delivered")
    @APIResponse(
            responseCode = "200",
            description = "Altitude difference sum from first to last point")
    @APIResponse(
            responseCode = "404",
            description = "Collection could not be found")
    @APIResponse(
            responseCode = "500",
            description = "Internal error")
    public Response altitudediff(
            @Parameter(description = "SmartData URL", required = true, example = "/SmartData") @QueryParam("smartdataurl") String smartdataurl,
            @Parameter(description = "Collections name", example = "col1") @QueryParam("collection") String collection,
            @Parameter(description = "Storage name", schema = @Schema(type = STRING, defaultValue = "public")) @QueryParam("storage") String storage,
            @Parameter(description = "Any filter statement accepted by SmartData") @QueryParam("filter") List<String> filters,
            @Parameter(description = "Date attribute (default: ts)", example = "ts") @QueryParam("dateattribute") String dateattr,
            @Parameter(description = "Start date (default: now - 30 days)", example = "2020-12-24T18:00") @QueryParam("start") String start,
            @Parameter(description = "End date (default: now)", example = "2020-12-24T19:00") @QueryParam("end") String end,
            @Parameter(description = "Geo attribute (default: pos_altitude)", example = "altitude") @QueryParam("geoattr") String geoattr) {

        ResponseObjectBuilder rob = new ResponseObjectBuilder();

        SmartDataAccessor acc = new SmartDataAccessor();

        if (smartdataurl == null) {
            rob.setStatus(Response.Status.BAD_REQUEST);
            rob.addErrorMessage("Parameter >smartdataurl< is missing.");
            return rob.toResponse();
        }

        if (smartdataurl.startsWith("/")) {
            smartdataurl = "http://localhost:8080" + smartdataurl;
        }

        if (collection == null) {
            rob.setStatus(Response.Status.BAD_REQUEST);
            rob.addErrorMessage("Parameter >collection< is missing.");
            return rob.toResponse();
        }

        if (dateattr == null) {
            dateattr = "ts";
        }

        if (geoattr == null) {
            geoattr = "pos_altitude";
        }

        LocalDateTime endDT;
        if (end != null) {
            endDT = LocalDateTime.parse(end);
        } else {
            endDT = LocalDateTime.now();
        }

        LocalDateTime startDT;
        if (start != null) {
            startDT = LocalDateTime.parse(start);
        } else {
            startDT = LocalDateTime.now().minusDays(30);
        }

        JsonArray data;

        try {
            data = acc.fetchData(smartdataurl, collection, storage, geoattr, filters, dateattr, startDT, endDT, dateattr, null);
        } catch (SmartDataAccessorException ex) {
            rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            rob.addErrorMessage("Could not calculate altitude difference because of error: " + ex.getLocalizedMessage());
            return rob.toResponse();
        }

        double totalAltitude = 0;
        Double prevAltitude = null;

        // Walk trough sets
        for (JsonValue curObj : data) {
            JsonObject dateobj = curObj.asJsonObject();

            if (dateobj.getJsonNumber(geoattr) == null) {
                continue;
            }

            Double altitude = dateobj.getJsonNumber(geoattr).doubleValue();

            if (prevAltitude != null) {
                double result = prevAltitude - altitude;
                if (result < 0) {
                    result *= -1;
                }
                totalAltitude += result;
            }
            prevAltitude = altitude;
        }

        rob.add("altitudeMeters", totalAltitude);
        rob.setStatus(Response.Status.OK);
        return rob.toResponse();
    }

    @GET
    @Path("neargeometries")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @SmartUserAuth
    @Operation(summary = "Near geometries",
            description = "Deliver all datasets, where the geometries withinin a distance of x meters or less from each other.")
    @APIResponse(
            responseCode = "200",
            description = "List of datasets with near geometries.")
    @APIResponse(
            responseCode = "404",
            description = "Collection could not be found")
    @APIResponse(
            responseCode = "500",
            description = "Internal error")
    public Response neargeometries(
            @Parameter(description = "SmartData URL", required = true, example = "/SmartData") @QueryParam("smartdataurl") String smartdataurl,
            @Parameter(description = "Storage1 name", schema = @Schema(type = STRING, defaultValue = "public")) @QueryParam("storage1") String storage1,
            @Parameter(description = "Collection 1 name", example = "col1") @QueryParam("collection1") String collection1,
            @Parameter(description = "Geometry attribute 1 name", schema = @Schema(type = STRING, defaultValue = "geom")) @QueryParam("geomattr1") String geomattr1,
            @Parameter(description = "Storage2 name", schema = @Schema(type = STRING, defaultValue = "public")) @QueryParam("storage2") String storage2,
            @Parameter(description = "Collection 2 name", example = "col1") @QueryParam("collection2") String collection2,
            @Parameter(description = "Geometry attribute 2 name", schema = @Schema(type = STRING, defaultValue = "geom")) @QueryParam("geomattr2") String geomattr2,
            @Parameter(description = "Maximum distance", schema = @Schema(type = STRING, defaultValue = "geom")) @QueryParam("distance") String distance) {

        ResponseObjectBuilder rob = new ResponseObjectBuilder();

        SmartDataAccessor acc = new SmartDataAccessor(smartdataurl);
        Connection con = acc.getConnection();
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        String result = "";
        try {
            String sql = "SELECT json_agg(json_build_object( 'a', a.id, 'b', b.id, 'dist', ST_Distance(ST_Transform(a.coordinates, 3857), ST_Transform(b.coordinates, 3857)) )) FROM \""+storage1+"\".\""+collection1+"\" a JOIN \""+storage2+"\".\""+collection2+"\" b ON ST_DWithin(ST_Transform(a."+geomattr1+", 3857), ST_Transform(b."+geomattr2+", 3857), "+distance+") WHERE a.id < b.id;";
            pstmt = con.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                result = rs.getString(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            rob.addErrorMessage("Could not recive data: " + e.getLocalizedMessage());
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (pstmt != null) {
                    pstmt.close();
                }
                if (con != null) {
                    con.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        rob.add("list", result);
        rob.setStatus(Response.Status.OK);
        return rob.toResponse();
    }

    @GET
    @Path("triangulate")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @SmartUserAuth
    @Operation(summary = "Distance",
            description = "Calculates the distance between all the datasets that are delivered")
    @APIResponse(
            responseCode = "200",
            description = "Distance from first to last point")
    @APIResponse(
            responseCode = "404",
            description = "Collection could not be found")
    @APIResponse(
            responseCode = "500",
            description = "Internal error")
    public Response triangulate(
            @Parameter(description = "SmartData URL", required = true, example = "/SmartData") @QueryParam("smartdataurl") String smartdataurl,
            @Parameter(description = "Collections name", example = "col1") @QueryParam("collection") String collection,
            @Parameter(description = "Storage name", schema = @Schema(type = STRING, defaultValue = "public")) @QueryParam("storage") String storage,
            @Parameter(description = "Any filter statement accepted by SmartData") @QueryParam("filter") List<String> filters,
            @Parameter(description = "Date attribute (default: ts)", example = "ts") @QueryParam("dateattribute") String dateattribute,
            @Parameter(description = "Start date (default: now - 30 days)", example = "2020-12-24T18:00") @QueryParam("start") String start,
            @Parameter(description = "End date (default: now)", example = "2020-12-24T19:00") @QueryParam("end") String end,
            @Parameter(description = "Geo attribute", example = "point") @QueryParam("geocolumn") String geoattribute) {

        ResponseObjectBuilder rob = new ResponseObjectBuilder();

        SmartDataAccessor acc = new SmartDataAccessor();

        if (smartdataurl == null) {
            rob.setStatus(Response.Status.BAD_REQUEST);
            rob.addErrorMessage("Parameter >smartdataurl< is missing.");
            return rob.toResponse();
        }

        if (smartdataurl.startsWith("/")) {
            smartdataurl = "http://localhost:8080" + smartdataurl;
        }

        if (collection == null) {
            rob.setStatus(Response.Status.BAD_REQUEST);
            rob.addErrorMessage("Parameter >collection< is missing.");
            return rob.toResponse();
        }

        if (dateattribute == null) {
            dateattribute = "ts";
        }

        LocalDateTime endDT;
        if (end != null) {
            endDT = LocalDateTime.parse(end);
        } else {
            endDT = LocalDateTime.now();
        }

        LocalDateTime startDT;
        if (start != null) {
            startDT = LocalDateTime.parse(start);
        } else {
            startDT = LocalDateTime.now().minusDays(30);
        }

        JsonArray data;
        try {
            data = acc.fetchData(smartdataurl, collection, storage, geoattribute, filters, dateattribute, startDT, endDT, dateattribute, null);
        } catch (SmartDataAccessorException ex) {
            rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
            rob.addErrorMessage("Could not calculate distance because of error: " + ex.getLocalizedMessage());
            return rob.toResponse();
        }

        double totalDistance = 0;

        // Walk trough sets
        for (JsonValue curObj : data) {
            JsonObject datestr = curObj.asJsonObject();
            // Walk trough attributes
            for (Entry<String, JsonValue> curEntry : datestr.entrySet()) {
                System.out.println("TEST " + curEntry.getKey());
            }
            System.out.println("TEST " + datestr);
        }

//        for (int i = 0; i < dataPoints.size() - 1; i++) {
//            DataPoint startDataPoint = dataPoints.get(i);
//            DataPoint endDataPoint = dataPoints.get(i + 1);
//
//            LatLng startLatLng = new LatLng(Double.parseDouble(startDataPoint.getLatitude()),
//                    Double.parseDouble(startDataPoint.getLongitude()));
//
//            LatLng endLatLng = new LatLng(Double.parseDouble(endDataPoint.getLatitude()),
//                    Double.parseDouble(endDataPoint.getLongitude()));
//
//            double distance = LatLngTool.distance(startLatLng, endLatLng, LengthUnit.KILOMETER);
//
//            if (distance < 2.0) {
//                totalDistance += distance;
//            }
//        }
        rob.add("total", totalDistance);
        rob.setStatus(Response.Status.OK);
        return rob.toResponse();
    }
}
