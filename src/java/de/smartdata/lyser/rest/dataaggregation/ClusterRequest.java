package de.smartdata.lyser.rest.dataaggregation;

import java.util.List;
import java.util.Map;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * Request for temporal clustering
 * 
 * @author Florian
 */
@Schema(description = "Request body for temporal clustering")
public class ClusterRequest {
    @Schema(description = "Start timestamp", example = "2020-12-24T18:00")
    public String start;

    @Schema(description = "End timestamp", example = "2020-12-24T19:00")
    public String end;

    @Schema(description = "Attribute holding time information", example = "ts")
    public String date_attr;
    
    @Schema(description = "Duration of one cluster in seconds", example = "30")
    public Integer cluster_seconds;

    @Schema(description = "Enable traceability output", example = "true")
    public Boolean traceability;

    @Schema(description = "Whether to include only closed clusters", example = "false")
    public Boolean onlyclosedclusters;

    @Schema(description = "Attribute name to mark processed entries", example = "processed")
    public String processed_attr;

    @Schema(description = "Mapping configuration")
    public Map<String, MappingConfig> mapping;

    @Schema(description = "List of data source configurations")
    public List<DatasourceConfig> datasources;
}
