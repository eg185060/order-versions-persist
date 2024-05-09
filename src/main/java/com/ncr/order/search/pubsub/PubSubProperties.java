package com.ncr.order.search.pubsub;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "order.search.pubsub")
public class PubSubProperties {

    private String subscriptionName;

}
