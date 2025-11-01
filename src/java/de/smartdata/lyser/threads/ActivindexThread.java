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
import java.util.List;

/**
 *
 * @author Florian Fehring
 */
public class ActivindexThread extends Thread {

    private String smartdataurl, collections, storage, dateattribute;
    private String start, end;
    private int lasthours;
    private Integer threshold;
    private boolean exact;
    
    public ActivindexThread(String smartdataurl, String collections, String storage, String dateattribute, String start, String end, int lasthours, Integer threshold, boolean exact) {
        this.smartdataurl = smartdataurl;
        this.collections = collections;
        this.storage = storage;
        this.dateattribute = dateattribute;
        this.start = start;
        this.end = end;
        this.lasthours = lasthours;
        this.threshold = threshold;
        this.exact = exact;
    }
    
    @Override
    public void run() {
        SmartDataAccessor acc = new SmartDataAccessor();
System.out.println("Activindex start");
        if (this.smartdataurl.startsWith("/")) {
            this.smartdataurl = "http://localhost:8080" + this.smartdataurl;
        }

        LocalDateTime startDT;
        if (this.start != null) {
            startDT = LocalDateTime.parse(this.start);
        } else if (this.lasthours > 0) {
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
                colls = acc.fetchCollectons(this.smartdataurl, this.storage);
            } catch (SmartDataAccessorException ex) {
                rob.setStatus(Response.Status.INTERNAL_SERVER_ERROR);
                rob.addErrorMessage("Could not get collections: " + ex.getLocalizedMessage());
                rob.addException(ex);
                StatisticResource.cache_activeindex.put(this.smartdataurl + this.collections, rob);
                return;
            }
        }

        if (this.threshold == null) {
            this.threshold = 0;
        }

        List<String> active = new ArrayList<>();
        List<String> inactive = new ArrayList<>();
        // Get count for every collection
        for (String curCol : colls) {
            // Exclude tables from SmartMonitoring
            if (curCol.startsWith("tbl_")) {
                continue;
            }
            try {
                int count = acc.fetchCount(smartdataurl, curCol, storage, dateattribute, startDT, endDT, this.exact);
                if (count > threshold) {
                    active.add(curCol);
                } else {
                    inactive.add(curCol);
                }
            } catch (SmartDataAccessorException ex) {
                rob.addWarningMessage(ex.getLocalizedMessage());
            }
        }
        System.out.println("Activeindex done");
        rob.add("active", active.size());
        rob.add("inactive", inactive.size());
        rob.add("actives", active);
        rob.add("inactives", inactive);
        rob.setStatus(Response.Status.OK);
        StatisticResource.cache_activeindex.put(this.smartdataurl + this.collections, rob);
    }
}
