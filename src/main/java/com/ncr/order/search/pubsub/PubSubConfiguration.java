package com.ncr.order.search.pubsub;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.spring.pubsub.core.PubSubOperations;
import com.google.cloud.spring.pubsub.integration.inbound.PubSubInboundChannelAdapter;
import com.ncr.order.search.bigtable.BigTableProperties;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.PublishSubscribeChannel;
import org.springframework.messaging.MessageChannel;

@Configuration
@EnableConfigurationProperties(PubSubProperties.class)
public class PubSubConfiguration {

    @Bean
    public PubSubInboundChannelAdapter messageChannelAdapter(
            @Qualifier("inputMessageChannel") MessageChannel inputChannel,
            PubSubOperations pubSubTemplate,
            PubSubProperties pubSubProperties)
    {
        final PubSubInboundChannelAdapter adapter =
                new PubSubInboundChannelAdapter(
                        pubSubTemplate,
                        pubSubProperties.getSubscriptionName());
        adapter.setOutputChannel(inputChannel);
        adapter.setPayloadType(String.class);

        return adapter;
    }

    @Bean
    public MessageChannel inputMessageChannel() {
        return new PublishSubscribeChannel();
    }

    @Bean
    public PubSubConsumer pubSubConsumer(
            ObjectMapper objectMapper,
            BigtableDataClient bigtableDataClient,
            BigTableProperties bigTableProperties
    ) {
        return new PubSubConsumer(
                objectMapper,
                bigtableDataClient,
                bigTableProperties
        );
    }
}
