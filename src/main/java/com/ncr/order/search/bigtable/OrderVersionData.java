package com.ncr.order.search.bigtable;

import lombok.Data;

import java.time.OffsetDateTime;

/**
 * {
 *     "lastPage": null,
 *     "orderId": "String",
 *     "pageNumber": 23,
 *     "sourceOrganization": "String",
 *     "totalPages": 61,
 *     "totalResults": 46,
 *     "versions": [
 *         {
 *             "dateUpdated": "2023-12-13T23:24:59.574Z",
 *             "deviceId": "String",
 *             "diff": {},
 *             "eventType": "String",
 *             "id": "String",
 *             "updatingOrganization": "String",
 *             "user": "String"
 *         }
 *     ]
 * }
 */
@Data
public class OrderVersionData {
    private String org;
    private String orderId;
    private String sourceOrg;

    private OffsetDateTime dateUpdated;
    private String deviceId;
    private String diff;
    private String eventType;
    private short id;
    private String updatingOrg;
    private String user;
}
