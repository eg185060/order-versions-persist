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
import java.util.Collections;
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
    private static final String PUBSUB_EVENT_TYPE_CREATE = "Create";

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
        if (PUBSUB_EVENT_TYPE_CREATE.equals(orderPubSubEvent.getEventType())) {
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

        // plus one since range query is exclusive
        String rangeEnd = rowKeyPrefix + (MAX_VERSIONS + 1);

        Query query =
                Query.create(tableId)
                        .limit(1)
                        .range(rangeStart, rangeEnd);
        ServerStream<Row> rows = bigtableDataClient.readRows(query);

        short newVersion = MAX_VERSIONS;
        for (Row row : rows) {
            ByteString rowKeyByteString = row.getKey();
            String rowKey = rowKeyByteString.toStringUtf8();

            log.info("Found version with rowKey: {}", rowKey);
            String[] rowKeyTokens = rowKey.split(ROW_KEY_DELIM);

            String oldNewestVersionString = rowKeyTokens[2];

            short oldNewestVersion = Short.parseShort(oldNewestVersionString);

            newVersion = (short) (oldNewestVersion - 1);
        }

        if (newVersion < 0) {
            log.warn("Order already has max {} versions, skipping... - org: {}, orderId: {}", MAX_VERSIONS, org, orderId);
            return;
        }

        // the "human-readable" versions count up from zero even though we do the inverse in bigtable rowkey
        short jsonNewVersion = (short) (MAX_VERSIONS - newVersion);
        orderVersionData.setId(jsonNewVersion);

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

        log.info("calling BigTable mutateRow({}) (real version: {})...", rowKey, jsonNewVersion);
        bigtableDataClient.mutateRow(rowMutation);
    }

    private static Map<String, String> removeOriginalMessage(MessageHeaders headers) {
        if (headers == null) {
            return Collections.emptyMap();
        } else {
            return !headers.containsKey("gcp_pubsub_original_message") ?
                    headers.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, v -> v.getValue().toString()))
                    : headers.entrySet().stream().filter(e -> !"gcp_pubsub_original_message".equals(e.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, v -> v.getValue().toString()));
        }
    }
}
