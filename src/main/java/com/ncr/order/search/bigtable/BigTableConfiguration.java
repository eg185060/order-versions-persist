package com.ncr.order.search.bigtable;

import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.spring.autoconfigure.core.GcpProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

@Slf4j
@Configuration
@EnableConfigurationProperties(BigTableProperties.class)
public class BigTableConfiguration {

    @Bean(destroyMethod = "close")
    public BigtableDataClient bigtableDataClient(
            BigTableProperties bigTableProperties,
            GcpProperties gcpProperties
    ) throws IOException {
        log.info("Creating BigtableDataClient... projectId: {}, instanceId: {}",
                gcpProperties.getProjectId(), bigTableProperties.getInstanceId());

        final BigtableDataSettings settings = BigtableDataSettings
                .newBuilder()
                .setProjectId(gcpProperties.getProjectId())
                .setInstanceId(bigTableProperties.getInstanceId())
                .build();
        return BigtableDataClient.create(settings);
    }

}
