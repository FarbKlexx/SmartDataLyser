/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package de.smartdata.lyser.threads;

import de.fhbielefeld.scl.rest.util.ResponseObjectBuilder;
import de.smartdata.lyser.data.SmartDataAccessor;
import de.smartdata.lyser.data.SmartDataAccessorException;
import de.smartdata.lyser.rest.StatisticResource;
import jakarta.ws.rs.core.Response;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author Florian Fehring
 */
public class CountThread extends Thread {

    private String smartdataurl, collections, storage, dateattribute;
    private String start, end;
    private int lasthours;
    private boolean exact;
    
    public CountThread(String smartdataurl, String collections, String storage, String dateattribute, String start, String end, int lasthours, boolean exact) {
        this.smartdataurl = smartdataurl;
        this.collections = collections;
        this.storage = storage;
        this.dateattribute = dateattribute;
        this.start = start;
        this.end = end;
        this.lasthours = lasthours;
        this.exact = exact;
    }

    @Override
    public void run() {
        SmartDataAccessor acc = new SmartDataAccessor();

        if (this.smartdataurl.startsWith("/")) {
            this.smartdataurl = "http://localhost:8080" + this.smartdataurl;
        }
        
        LocalDateTime startDT;
        if (this.start != null) {
            startDT = LocalDateTime.parse(this.start);
        } else if(this.lasthours > 0) {
            startDT = LocalDateTime.now().minusHours(this.lasthours);
        } else {
            startDT = LocalDateTime.now().minusDays(30);
        }

        LocalDateTime endDT;
        if (this.end != null) {
            endDT = LocalDateTime.parse(this.end);
        } else {
            endDT = LocalDateTime.now();
        }

        ResponseObjectBuilder rob = new ResponseObjectBuilder();
        
        List<String> colls = new ArrayList<>();
        if (this.collections != null) {
            String[] cos = this.collections.split(",");
            for (int i = 0; i < cos.length; i++) {
                colls.add(cos[i]);
            }
        } else {
            try {
                colls = acc.fetchCollectons(smartdataurl, storage);
            } catch (SmartDataAccessorException ex) {
                rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
                rob.addErrorMessage("Could not get collections: " + ex.getLocalizedMessage());
                rob.addException(ex);
                StatisticResource.cache_count.put(this.smartdataurl + this.collections, rob);
                return;
            }
        }
        
        LocalDateTime stattime = LocalDateTime.now();
        Long count = 0L;
        Map<String, Long> counted = new HashMap<>();
        // Get count for every collection
        for (String curCol : colls) {
            // Exclude tables from SmartMonitoring
            if (curCol.startsWith("tbl_")) {
                continue;
            }
            try {
                count += acc.fetchCount(this.smartdataurl, curCol, this.storage, this.dateattribute, startDT, endDT, this.exact);
                counted.put(curCol, count);
            } catch (SmartDataAccessorException ex) {
                rob.addWarningMessage(ex.getLocalizedMessage());
            }
        }
        rob.add("count", count);
        rob.add("tables", counted);
        rob.add("time", stattime);
        rob.setStatus(Response.Status.OK);
        StatisticResource.cache_count.put(this.smartdataurl + this.collections, rob);
    }
}
