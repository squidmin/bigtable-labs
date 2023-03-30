package org.squidmin.config;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.squidmin.model.BigtableStructure;

import static org.mockito.Mockito.mock;

@Configuration
@Profile("integration")
public class UnitTestConfig {

    @Value("${bigtable.table-id}")
    private String tableId;

    @Autowired
    private BigtableStructure btStructure;


    @Bean
    public BigtableClientManager btConfig() { return mock(BigtableClientManager.class); }

    @Bean
    public BigtableStructure btStructure() { return btStructure; }

    @Bean
    public String tableId() { return tableId; }

    @Bean
    public BigtableDataSettings dataSettings() { return mock(BigtableDataSettings.class); }

    @Bean
    public BigtableDataClient dataClient() { return mock(BigtableDataClient.class); }

    @Bean
    public BigtableTableAdminClient tableAdminClient() {
        return mock(BigtableTableAdminClient.class);
    }

    @Bean
    public BigtableTableAdminSettings tableAdminSettings() {
        return mock(BigtableTableAdminSettings.class);
    }

}
