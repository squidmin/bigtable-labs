package org.squidmin.config;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import lombok.Getter;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.squidmin.model.BigtableStructure;
import org.squidmin.model.FamilyToQualifierMapping;

import java.io.IOException;

@Configuration
@ComponentScan(basePackages = {
    "org.squidmin.bigtable-labs"
})
@Getter
@Slf4j
public class BigtableClientManager {


    private final String projectId;
    private final String instanceId;
    private final BigtableStructure btStructure;
    private final BigtableDataSettings dataSettings;
    private final BigtableDataClient dataClient;
    private final BigtableTableAdminClient tableAdminClient;
    private final BigtableTableAdminSettings tableAdminSettings;


    @Autowired
    public BigtableClientManager(
        @Value("${spring.cloud.gcp.project-id}") String projectId,
        @Value("${bigtable.instance-id}") String instanceId,
        BigtableStructure btStructure) throws IOException {
        this.projectId = projectId;
        this.instanceId = instanceId;
        this.btStructure = btStructure;

        dataSettings = BigtableDataSettings.newBuilder()
            .setProjectId(projectId)
            .setInstanceId(instanceId)
            .build();

        tableAdminSettings = BigtableTableAdminSettings.newBuilder()
            .setProjectId(projectId)
            .setInstanceId(instanceId)
            .build();

        tableAdminClient = BigtableTableAdminClient.create(tableAdminSettings);

        dataClient = BigtableDataClient.create(projectId, instanceId);
    }

    public int getMaxQualifierLength() {
        FamilyToQualifierMapping ftqMapping = btStructure.getColumnFamilies().get(0);
        int maxQualifierLength = 0;
        for (String qualifierName : ftqMapping.getQualifierNames()) {
            maxQualifierLength = Math.max(maxQualifierLength, qualifierName.length());
        }
        return maxQualifierLength;
    }


}
