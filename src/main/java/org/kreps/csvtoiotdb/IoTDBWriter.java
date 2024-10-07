package org.kreps.csvtoiotdb;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.kreps.csvtoiotdb.configs.iotdb.IoTDBDevice;
import org.kreps.csvtoiotdb.configs.iotdb.IoTDBMeasurement;
import org.kreps.csvtoiotdb.converter.RowData;

/**
 * Writes converted data to IoTDB.
 */
public class IoTDBWriter {
    private final IoTDBClientManager clientManager;
    private final int maxRetriesCount;
    private final long retryIntervalInMs;

    /**
     * Constructs an IoTDBWriter instance.
     *
     * @param clientManager The IoTDBClientManager managing IoTDB sessions.
     */
    public IoTDBWriter(IoTDBClientManager clientManager, int maxRetriesCount, long retryIntervalInMs) {
        this.clientManager = clientManager;
        this.maxRetriesCount = maxRetriesCount;
        this.retryIntervalInMs = retryIntervalInMs;
    }

    /**
     * Writes data to IoTDB.
     *
     * @param deviceDataMap     A map of device IDs to their corresponding list of
     *                          RowData.
     * @param ioTDBSettingsList The list of IoTDB settings corresponding to the
     *                          devices.
     */
    public void writeData(Map<String, List<RowData>> deviceDataMap, List<IoTDBDevice> ioTDBSettingsList) {
        for (IoTDBDevice ioTDBSettings : ioTDBSettingsList) {
            String deviceId = ioTDBSettings.getDeviceId();
            List<RowData> rows = deviceDataMap.get(deviceId);
            if (rows == null || rows.isEmpty()) {
                continue;
            }

            Tablet tablet = createTablet(ioTDBSettings);
            for (int i = 0; i < rows.size(); i++) {
                RowData row = rows.get(i);
                tablet.addTimestamp(i, row.getTimestamp());
                for (IoTDBMeasurement measurement : ioTDBSettings.getMeasurements()) {
                    Object value = row.getMeasurements().get(measurement.getName());
                    tablet.addValue(measurement.getName(), i, value);
                }
            }

            // Attempt to insert the tablet with retry logic
            boolean success = false;
            int attempt = 0;
            Session session = null;

            while (!success && attempt <= this.maxRetriesCount) {
                try {
                    session = clientManager.acquireSession();
                    session.insertTablet(tablet);
                    success = true; 
                } catch (IoTDBConnectionException | StatementExecutionException e) {
                    attempt++;
                    // Replace with proper logging
                    System.err.println("Failed to insert tablet on attempt " + attempt + ": " + e.getMessage());

                    // Close the faulty session
                    if (session != null) {
                        try {
                            session.close();
                        } catch (Exception closeException) {
                            // Replace with proper logging
                            closeException.printStackTrace();
                        }
                    }

                    // If max retries reached, log and continue
                    if (attempt > this.maxRetriesCount) {
                        System.err.println("Max retries reached. Skipping this tablet.");
                        break;
                    }

                    // Optionally, implement backoff strategy here
                    try {
                        Thread.sleep(this.retryIntervalInMs * attempt);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        System.err.println("Thread interrupted during backoff.");
                        break;
                    }
                } catch (InterruptedException e) {
                    // Handle thread interruption
                    Thread.currentThread().interrupt();
                    System.err.println("Thread interrupted while acquiring session.");
                    break;
                } finally {
                    // Release the session back to the pool if it's still open
                    if (session != null) {
                        clientManager.releaseSession(session);
                    }
                }
            }
        }
    }

    /**
     * Creates a Tablet instance based on the IoTDB settings.
     *
     * @param ioTDBSettings The IoTDB settings for the device.
     * @return A Tablet configured for the device.
     */
    private Tablet createTablet(IoTDBDevice ioTDBSettings) {
        List<IoTDBMeasurement> measurements = ioTDBSettings.getMeasurements();
        List<MeasurementSchema> schemas = measurements.stream()
                .map(measurement -> new MeasurementSchema(measurement.getName(), measurement.getDataType(),
                        measurement.getEncoding(), measurement.getCompression()))
                .collect(Collectors.toList());
        Tablet tablet = new Tablet(ioTDBSettings.getDeviceId(), schemas);
        return tablet;
    }
}
