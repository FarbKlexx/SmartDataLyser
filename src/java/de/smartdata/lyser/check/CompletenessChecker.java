package de.smartdata.lyser.check;

import de.fhbielefeld.scl.rest.util.ResponseObjectBuilder;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import static java.time.temporal.ChronoUnit.SECONDS;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import de.smartdata.lyser.data.*;
import jakarta.json.JsonArray;
import jakarta.json.JsonValue;
import jakarta.ws.rs.core.Response;

/**
 * Algorithms for checking completeness of measured values.
 *
 * @author ffehring
 */
public class CompletenessChecker {

    String smartdataRequest;
    
    /**
     * Simply checks if the collection has a set in the timerange.
     * 
     * @param smartdataurl  SmartDatas URL where to check
     * @param collection    Name of the collection to check
     * @param storage       NAme of the storage to check in
     * @param filters       Any filter statements supported by SmartData
     * @param dateattribute Name of the attribute that holds time information
     * @param start         Start date of looking range
     * @param end           End date of looking range
     * @return ResponseObjectBuilder with status 200 when there is a set, and 417 (expectation failed) when no set is there
     * @throws SmartDataAccessorException 
     */
    public ResponseObjectBuilder hasSetInTime(
            String smartdataurl,
            String collection,
            String storage,
            List<String> filters,
            String dateattribute,
            LocalDateTime start,
            LocalDateTime end) throws SmartDataAccessorException {

        SmartDataAccessor sda = new SmartDataAccessor();
        JsonArray data = sda.fetchData(smartdataurl, collection, storage, dateattribute, filters, dateattribute, start, end, null, null);

        ResponseObjectBuilder rob = new ResponseObjectBuilder();
        if(!data.isEmpty()) {
            rob.setStatus(Response.Status.OK);
        } else {
            rob.setStatus(Response.Status.EXPECTATION_FAILED);
        }
        return rob;
    }
    
    public ResponseObjectBuilder checkTimeCompleteness(
            String smartdataurl,
            String collection,
            String storage,
            List<String> filters,
            String dateattribute,
            Long measurefreq,
            Long threshold,
            LocalDateTime start,
            LocalDateTime end) throws SmartDataAccessorException {

        SmartDataAccessor sda = new SmartDataAccessor();
        JsonArray data = sda.fetchData(smartdataurl, collection, storage, dateattribute, filters, dateattribute, start, end, null, null);

        // Convert option input (string) into duration
        TemporalAmount freq = Duration.ofSeconds(measurefreq);

        // Get exepected and available entries
        long expected = this.getNumberOfExpectedEntries(start, end, freq);
        long available = data.size();

        ResponseObjectBuilder rob = new ResponseObjectBuilder();

        rob.add("requestedperiod", start + " - " + end);
        rob.add("expectedentries", expected);
        rob.add("availableentries", available);
        rob.add("missingentries", (expected - available));

        return rob;
    }

    public List<Map<String, Object>> checkMissingPeriods(
            String smartdataurl,
            String collection,
            String storage,
            List<String> filters,
            String dateattribute,
            Long measurefreq,
            Long threshold,
            LocalDateTime start,
            LocalDateTime end) throws SmartDataAccessorException {

        SmartDataAccessor sda = new SmartDataAccessor();
        JsonArray data = sda.fetchData(smartdataurl, collection, storage, dateattribute, filters, dateattribute, start, end, null, null);

        // Convert option input (string) into duration
        TemporalAmount freq = Duration.ofSeconds(measurefreq);

        List<LocalDateTime> datetimes = new ArrayList<>();
        for (JsonValue curObj : data) {
            String datestr = curObj.asJsonObject().getString(dateattribute);
            datetimes.add(LocalDateTime.parse(datestr));
        }

        // Get durations of missing data
        List<TimespanDimension> missingDurations = this.getListOfMissingPeriods(
                datetimes,
                start,
                end,
                freq);
//
//        // Filter list (nightly missing data is not important)
////        List<TimespanDimension> filterTimeSpans = new ArrayList<>();
////        TimespanDimension filterTimeSpan = new TimespanDimension(LocalDateTime.of(2015, Month.MARCH, 1, 23, 00), LocalDateTime.of(2015, Month.MARCH, 1, 2, 0));
////        filterTimeSpans.add(filterTimeSpan);
////        missingDurations = new TimespanMethods().filterPeriodsList(missingDurations, filterTimeSpans);
        // Get filtered List durations with missing data
        List<Map<String, Object>> missingperiods = new ArrayList<>();
        for (TimespanDimension currentTimespan : missingDurations) {
            Map<String, Object> missingPeriod = new HashMap<>();
            Duration currentDuration = Duration.between(currentTimespan.getFrom(), currentTimespan.getUntil());
            long currentEntries = currentDuration.getSeconds() / freq.get(SECONDS);
            if (currentEntries < (0 - threshold) || currentEntries > (0 + threshold)) {
                //System.out.println(currentTimespan + " -> Duration: " + currentDuration + " Enties: " + currentEntries);
                missingPeriod.put("missingentries", currentEntries);
                missingPeriod.put("timespan", currentTimespan.toString());
                missingPeriod.put("duration", currentDuration.toString());
                missingperiods.add(missingPeriod);
            }
        }
        this.smartdataRequest = sda.getSmartdataRequest();
        return missingperiods;
    }

    /**
     * Gets the number of expected entries
     *
     * @param start Start of period
     * @param end End of period
     * @param interval Interval in which entries are expected
     * @return Number of expected entries
     */
    public long getNumberOfExpectedEntries(LocalDateTime start, LocalDateTime end, TemporalAmount interval) {
        long secondsDifference = ChronoUnit.SECONDS.between(start, end);
        long expectedEntries = secondsDifference / interval.get(SECONDS);
        return expectedEntries;
    }

    /**
     * Gets a list of periods with missing data
     *
     * @param data Data where to search missing Timeperiods in
     * @param start Start of period
     * @param end End of period
     * @param interval Interval in which entries are expected
     * @return List of periods with missing data
     */
    public List<TimespanDimension> getListOfMissingPeriods(List<LocalDateTime> data, LocalDateTime start, LocalDateTime end, TemporalAmount interval) {
        List<TimespanDimension> periods = new ArrayList<>();
        // Setup needed variables for periods finding
        LocalDateTime lastTime = null;
        long availableEntries = data.size();
        long remainingEntries = availableEntries;
        long doneEntries = 0;

        // Sort available data
        Collections.sort(data);

        long startexecution = System.currentTimeMillis();
        long starttickexecution = startexecution;
        for (LocalDateTime currentDateTime : data) {
            // Get missing period at beginning of time
            if (lastTime == null && currentDateTime.minus(interval) != start) {
                periods.add(new TimespanDimension(start, currentDateTime.minus(interval)));
            } // Get missing period from middle to end of searched period
            else if (lastTime != null && currentDateTime.isAfter(lastTime.plus(interval))) {
                periods.add(new TimespanDimension(lastTime.plus(interval), currentDateTime.minus(interval)));
            }
            lastTime = currentDateTime;

            // Get remaining time
            long endexecution = System.currentTimeMillis();
//            System.out.println("End: " + endexecution);
            long neededexecutionSTick = endexecution - starttickexecution;
            if (neededexecutionSTick > 1000) {
                System.out.print(".");
                starttickexecution = endexecution;
                long neededTime = endexecution - startexecution;
                System.out.println("Needed time: " + neededTime + " for " + doneEntries);

                long neededTimeForOne = neededTime / doneEntries;
                long expectedTimeForRemaining = neededTimeForOne * remainingEntries;
                System.out.println(remainingEntries + " left. Expected time to complete: " + expectedTimeForRemaining / 1000);
            }

            remainingEntries--;
            doneEntries++;
//            double neededForOne = neededexecution / (availableEntries - remainingEntries);
//            System.out.println(neededexecution + "For one: " + neededForOne);
//            Double remaining = (neededForOne * remainingEntries)/1000;
//            DecimalFormat format = new DecimalFormat("###0.##");
//            System.out.println("Remaining time: " + format.format(remaining) + " seconds for " + remainingEntries + " remaining entries.");
        }

        return periods;
    }
    
    public String getSmartdataRequest() {
        return this.smartdataRequest;
    }
}
