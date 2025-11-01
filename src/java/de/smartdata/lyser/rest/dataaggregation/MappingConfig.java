package de.smartdata.lyser.rest.dataaggregation;

import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * Class for mapping definitions of resources.
 * 
 * @author Florian
 */
@Schema(description = "Mapping configuration for a data attribute")
public class MappingConfig {
    @Schema(description = "Source attribute", example = "payload")
    public String source_attr;

    @Schema(description = "JSON pointer inside the source attribute", example = "/sensors/temp")
    public String source_pointer;

    @Schema(description = "Target attribute name", example = "temperature")
    public String target_attr;
}