package de.smartdata.lyser.rest;

import jakarta.ws.rs.core.Application;
import java.util.Set;

/**
 * Automatisch Erstellte Klasse durch Netbeans, verwaltet REST Klassen
 * @author Nils Leunig, dstarke
 */
@jakarta.ws.rs.ApplicationPath("smartdatalyser")
public class ApplicationConfig extends Application {

    @Override
    public Set<Class<?>> getClasses() {
        Set<Class<?>> resources = new java.util.HashSet<>();
        addRestResourceClasses(resources);
        resources.add(de.fhbielefeld.scl.rest.exceptions.handlers.GeneralExceptionMapper.class);
        return resources;
    }

    /**
     * Do not modify addRestResourceClasses() method.
     * It is automatically populated with
     * all resources defined in the project.
     * If required, comment out calling this method in getClasses().
     */
    private void addRestResourceClasses(Set<Class<?>> resources) {
        resources.add(de.fhbielefeld.scl.rest.exceptions.handlers.GeneralExceptionMapper.class);
        resources.add(de.fhbielefeld.scl.rest.util.CORSFilter.class);
        resources.add(de.fhbielefeld.smartuser.rest.AuthenticationFilter.class);
        resources.add(de.fhbielefeld.smartuser.rest.OwnershipFilter.class);
        resources.add(de.ngi.restutils.LoggingFilter.class);
        resources.add(de.smartdata.lyser.rest.CheckResource.class);
        resources.add(de.smartdata.lyser.rest.CompareResource.class);
        resources.add(de.smartdata.lyser.rest.DataAggregationResource.class);
        resources.add(de.smartdata.lyser.rest.GeoResource.class);
        resources.add(de.smartdata.lyser.rest.StatisticResource.class);
        resources.add(de.smartdata.lyser.rest.SystemResource.class);
    }
    
}
