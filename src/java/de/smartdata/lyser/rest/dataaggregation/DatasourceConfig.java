package de.smartdata.lyser.rest.dataaggregation;

import java.util.List;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * Class for a definition of a SmartData datasource.
 * 
 * @author Florian
 */
@Schema(description = "Configuration of a single data source")
public class DatasourceConfig {
    @Schema(description = "Name of the data source", example = "sourceA")
    public String name;

    @Schema(description = "SmartData service URL", example = "/SmartData")
    public String smartdataurl;

    @Schema(description = "Collection name(s)", example = "col1,col2")
    public String collection;

    @Schema(description = "Storage name", example = "public")
    public String storage;

    @Schema(description = "Name of the date attribute", example = "ts")
    public String dateattribute;
    
    @Schema(description = "Order expression", example = "ts ASC")
    public String order;

    @Schema(description = "Attributes to include", example = "val1,val2")
    public String includes;

    @Schema(description = "Dataset filter rules", example = "[\"id,gt,10\"]")
    public List<String> filter;

    @Schema(description = "Limit for used datasets", example = "72")
    public Long limit;
}
