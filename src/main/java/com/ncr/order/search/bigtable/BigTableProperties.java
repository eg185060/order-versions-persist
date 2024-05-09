package com.ncr.order.search.bigtable;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "order.search.bigtable")
public class BigTableProperties {
    private String instanceId;
    private String table;
}
