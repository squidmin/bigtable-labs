package org.squidmin.config;


import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.squidmin.model.BigtableStructure;

import java.io.IOException;


@Configuration
@Profile("integration")
public class IntegrationTestConfig {

    @Value("${spring.cloud.gcp.project-id}")
    private String projectId;

    @Value("${bigtable.table-id}")
    private String tableId;

    @Value("${bigtable.instance-id}")
    private String instanceId;


    @Autowired
    private BigtableStructure btStructure;


    @Bean
    public BigtableClientManager btConfig() throws IOException {
        return new BigtableClientManager(projectId, instanceId, btStructure);
    }

    @Bean
    public BigtableStructure btStructure() { return btStructure; }

    @Bean
    public String tableId() { return tableId; }

}
