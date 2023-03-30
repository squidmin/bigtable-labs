package org.squidmin.model;

import lombok.Getter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.context.config.annotation.RefreshScope;

import java.util.ArrayList;
import java.util.List;

@ConfigurationProperties(prefix = "bigtable")
@RefreshScope
@Getter
public class BigtableStructure {

    private final List<FamilyToQualifierMapping> columnFamilies = new ArrayList<>();

}
