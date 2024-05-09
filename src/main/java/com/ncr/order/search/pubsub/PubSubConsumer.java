package com.ncr.order.search.pubsub;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.zjsonpatch.JsonDiff;
import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.data.v2.models.TableId;
import com.google.protobuf.ByteString;
import com.ncr.order.search.bigtable.BigTableProperties;
import com.ncr.order.search.bigtable.OrderVersionData;
import lombok.extern.slf4j.Slf4j;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class PubSubConsumer {
    private final ObjectMapper objectMapper;
    private final BigtableDataClient bigtableDataClient;
    private final TableId tableId;
    private final JsonNode emptyJsonNode;

    private static final short MAX_VERSIONS = Short.MAX_VALUE;
    private static final String COLUMN_FAMILY = "ver";
    private static final String COLUMN = "v";
    private static final byte[] COLUMN_BYTES = COLUMN.getBytes();
    private static final String ROW_KEY_DELIM = "!";

    public PubSubConsumer(
            ObjectMapper objectMapper,
            BigtableDataClient bigtableDataClient,
            BigTableProperties bigTableProperties
    ) {
        this.objectMapper = objectMapper;
        this.emptyJsonNode = objectMapper.createObjectNode();
        this.bigtableDataClient = bigtableDataClient;
        this.tableId = TableId.of(bigTableProperties.getTable());
    }

    @ServiceActivator(inputChannel = "inputMessageChannel")
    public void receive(Message<String> message) {
        final OrderPubSubEvent orderPubSubEvent;
        try {
            orderPubSubEvent = objectMapper.readValue(message.getPayload(), OrderPubSubEvent.class);
        } catch (IOException e) {
            // bad json is non-recoverable so ACK and move on
            log.error(
                    "Failed to parse payload for message with headers {}",
                    removeOriginalMessage(message.getHeaders()),
                    e);
            return;
        }

        String org = orderPubSubEvent.getOrganization();
        String orderId = orderPubSubEvent.getOrderId();

        // TODO: on create set the diff to all add, because compare will show nothing
        JsonNode diffJson;
        if ("Create".equals(orderPubSubEvent.getEventType())) {
            diffJson =
                    JsonDiff.asJson(
                            orderPubSubEvent.getPreviousOrder(),
                            orderPubSubEvent.getUpdatedOrder());
        } else {
            diffJson =
                    JsonDiff.asJson(
                            emptyJsonNode,
                            orderPubSubEvent.getUpdatedOrder());
        }

        String diffString = "";
        try {
            diffString = objectMapper.writeValueAsString(diffJson);
        } catch (IOException e) {
            log.error("Failed to serialize diffJson for org: {} orderId: {}", org, orderId, e);
        }

        OrderVersionData orderVersionData = new OrderVersionData();
        orderVersionData.setOrg(org);
        orderVersionData.setOrderId(orderId);
        orderVersionData.setSourceOrg(orderPubSubEvent.getSourceOrganization());
        orderVersionData.setDiff(diffString);
        orderVersionData.setDateUpdated(orderPubSubEvent.getUpdatedTimestamp());
        orderVersionData.setEventType(orderPubSubEvent.getEventType());

        // TODO: Probably need to add these to the order event pubsub from Order API itself
        /*
        private String deviceId;
        private String updatingOrg;
        private String user;
        */

        String rowKeyPrefix = org + ROW_KEY_DELIM + orderId + ROW_KEY_DELIM;
        String rangeStart = rowKeyPrefix + "0";
        String rangeEnd = rowKeyPrefix + MAX_VERSIONS;

        Query query =
                Query.create(tableId)
                        .limit(1)
                        .range(rangeStart, rangeEnd);
        ServerStream<Row> rows = bigtableDataClient.readRows(query);

        short newVersion = MAX_VERSIONS;
        for (Row row : rows) {
            log.trace("Found version: {}", row);

            ByteString rowKeyByteString = row.getKey();
            String rowKey = rowKeyByteString.toString();

            String[] rowKeyTokens = rowKey.split(ROW_KEY_DELIM);

            String oldNewestVersionString = rowKeyTokens[2];

            short oldNewestVersion = Short.parseShort(oldNewestVersionString);

            newVersion = (short) (oldNewestVersion - 1);
        }

        if (newVersion < 0) {
            log.warn("Order already has {} versions, skipping... - org: {}, orderId: {}", MAX_VERSIONS, org, orderId);
            return;
        }

        orderVersionData.setId(newVersion);

        String orderVersionDataJsonString;
        try {
            orderVersionDataJsonString = objectMapper.writeValueAsString(orderVersionData);
        } catch (IOException e) {
            log.error("Failed to serialize orderVersionData: {}", orderVersionData, e);
            return;
        }

        final String rowKey = rowKeyPrefix + newVersion;
        byte[] orderVersionDataJsonBytes = orderVersionDataJsonString.getBytes();

        final RowMutation rowMutation = RowMutation.create(tableId, rowKey)
                .setCell(
                        COLUMN_FAMILY,
                        ByteString.copyFrom(COLUMN_BYTES),
                        ByteString.copyFrom(orderVersionDataJsonBytes));

        bigtableDataClient.mutateRow(rowMutation);
    }

    private static Map<String, String> removeOriginalMessage(MessageHeaders headers) {
        if (headers == null) {
            return null;
        } else {
            return !headers.containsKey("gcp_pubsub_original_message") ?
                    headers.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, (v) -> v.getValue().toString()))
                    : headers.entrySet().stream().filter((e) -> !"gcp_pubsub_original_message".equals(e.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, (v) -> v.getValue().toString()));
        }
    }
}
