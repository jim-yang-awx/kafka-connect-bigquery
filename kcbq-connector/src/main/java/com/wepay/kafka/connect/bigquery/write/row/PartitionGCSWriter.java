package com.wepay.kafka.connect.bigquery.write.row;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;

import java.time.Clock;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class PartitionGCSWriter extends GCSToBQWriter {
    /**
     * Initializes a batch GCS writer with a full list of rows to write.
     *
     * @param storage     GCS Storage
     * @param bigQuery    {@link BigQuery} Object used to perform upload
     * @param retries     Maximum number of retries
     * @param retryWaitMs Minimum number of milliseconds to wait before retrying
     */
    public PartitionGCSWriter(Storage storage, BigQuery bigQuery, int retries, long retryWaitMs) {
        super(storage, bigQuery, retries, retryWaitMs);
    }

    protected BlobId getBlobId(TableId tableId, String buckName, String blobName) {

        int index = blobName.lastIndexOf("/");
        String prefix = null;
        String fileName = blobName;

        if (index != -1) {
            prefix = blobName.substring(0, index);
            fileName = blobName.substring(index + 1);
        }

        String blob = tableId.getDataset() + "/" + tableId.getTable() + "/" + dayPartition() + "/" + fileName;

        if (prefix != null) {
            blob = prefix + "/" + blob;
        }
        return BlobId.of(buckName, blob);
    }


    private String dayPartition() {
        return LocalDate.now(UTC_CLOCK).format(DateTimeFormatter.BASIC_ISO_DATE);
    }

    private static final Clock UTC_CLOCK = Clock.systemUTC();
}
