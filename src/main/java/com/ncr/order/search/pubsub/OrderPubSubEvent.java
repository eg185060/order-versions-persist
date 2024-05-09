package com.ncr.order.search.pubsub;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.Data;

import java.time.OffsetDateTime;

@Data
public class OrderPubSubEvent {
    private String organization;
    private String sourceOrganization;
    private String createdBy;
    private String correlationId;
    private String orderId;
    private OffsetDateTime updatedTimestamp;
    private JsonNode previousOrder;
    private JsonNode updatedOrder;
    private Boolean publish;
    private String eventType;
    private Boolean expired;
    private Boolean statusChange;
}
