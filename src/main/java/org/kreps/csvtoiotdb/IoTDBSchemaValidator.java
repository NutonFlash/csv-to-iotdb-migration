package org.kreps.csvtoiotdb;

import java.util.ArrayList;
import java.util.List;

import org.apache.iotdb.isession.pool.SessionDataSetWrapper;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Field;
import org.kreps.csvtoiotdb.configs.iotdb.IoTDBDevice;
import org.kreps.csvtoiotdb.configs.iotdb.IoTDBMeasurement;

public class IoTDBSchemaValidator {

    private final IoTDBClientManager iotdbClientManager;

    public IoTDBSchemaValidator(IoTDBClientManager iotdbClientManager) {
        this.iotdbClientManager = iotdbClientManager;
    }

    /**
     * Validate all devices and their measurements. If a timeseries doesn't exist,
     * create it.
     *
     * @param devices List of IoTDBDevice objects.
     * @throws Exception if validation or creation of timeseries fails.
     */
    public void validateAndCreateTimeseriesForDevices(List<IoTDBDevice> devices) throws Exception {
        for (IoTDBDevice device : devices) {
            String deviceId = device.getDeviceId();
            String pathColumn = device.getPathColumn();

            if (pathColumn == null || pathColumn.isEmpty()) {
                if (Boolean.TRUE.equals(device.getIsAlignedTimeseries())) {
                    createAlignedTimeseries(deviceId, device.getMeasurements());
                } else {
                    for (IoTDBMeasurement measurement : device.getMeasurements()) {
                        TSDataType dataType = measurement.getDataType();
                        TSEncoding encoding = measurement.getEncoding();
                        CompressionType compression = measurement.getCompression();

                        // Ensure the timeseries exists or create it
                        ensureTimeseries(deviceId, measurement.getName(), dataType, encoding, compression);
                    }
                }
            } else {
                // For devices with pathColumn, we can't create timeseries here
                // as we don't know the specific path values
                System.out.println("Skipping schema validation for device with pathColumn: " + deviceId);
            }
        }
    }

    /**
     * Ensure that the schema for the specified device and measurement is correct.
     *
     * @param deviceId    The IoTDB device ID.
     * @param measurement The measurement (timeseries) name.
     * @param dataType    The expected data type.
     * @param encoding    The expected encoding.
     * @param compression The expected compression method.
     */
    public void ensureTimeseries(String deviceId, String measurement, TSDataType dataType,
            TSEncoding encoding, CompressionType compression) throws Exception {

        String timeseriesPath = String.format("%s.%s", deviceId, measurement);

        if (checkTimeseriesExists(timeseriesPath)) {
            validateExistingTimeseriesSchema(timeseriesPath, dataType, encoding, compression);
        } else {
            createTimeseries(timeseriesPath, dataType, encoding, compression);
        }
    }

    /**
     * Check if a timeseries exists in the IoTDB instance.
     *
     * @param timeseriesPath The full path of the timeseries (e.g.,
     *                       "root.device1.sensorTag").
     * @return True if the timeseries exists, false otherwise.
     * @throws IoTDBConnectionException
     */
    private boolean checkTimeseriesExists(String timeseriesPath)
            throws IoTDBConnectionException, StatementExecutionException {
        String sql = String.format("SHOW TIMESERIES %s", timeseriesPath);

        SessionPool session = this.iotdbClientManager.acquireSession();

        try (SessionDataSetWrapper dataSet = session.executeQueryStatement(sql)) {
            return dataSet.hasNext();
        } catch (IoTDBConnectionException |

                StatementExecutionException e) {
            throw new IoTDBConnectionException("Error checking if timeseries exists: " + timeseriesPath, e);
        }
    }

    /**
     * Validate the schema of an existing timeseries.
     *
     * @param timeseriesPath      The full path of the timeseries.
     * @param expectedDataType    The expected data type.
     * @param expectedEncoding    The expected encoding.
     * @param expectedCompression The expected compression method.
     * @throws Exception If the existing schema doesn't match the expected schema.
     */
    private void validateExistingTimeseriesSchema(String timeseriesPath, TSDataType expectedDataType,
            TSEncoding expectedEncoding, CompressionType expectedCompression) throws Exception {

        String sql = String.format("SHOW TIMESERIES %s", timeseriesPath);

        SessionPool sessionPool = this.iotdbClientManager.acquireSession();
        try (SessionDataSetWrapper dataSet = sessionPool.executeQueryStatement(sql)) {
            if (dataSet.hasNext()) {
                List<Field> schemaInfo = dataSet.next().getFields();

                TSDataType actualDataType = TSDataType.valueOf(schemaInfo.get(3).getStringValue());
                TSEncoding actualEncoding = TSEncoding.valueOf(schemaInfo.get(4).getStringValue());
                CompressionType actualCompression = CompressionType.valueOf(schemaInfo.get(5).getStringValue());

                if (!expectedDataType.equals(actualDataType) ||
                        !expectedEncoding.equals(actualEncoding) ||
                        !expectedCompression.equals(actualCompression)) {
                    throw new Exception(String.format("Timeseries %s exists, but the schema does not match: " +
                            "Expected [dataType=%s, encoding=%s, compression=%s], " +
                            "but found [dataType=%s, encoding=%s, compression=%s].",
                            timeseriesPath, expectedDataType, expectedEncoding, expectedCompression,
                            actualDataType, actualEncoding, actualCompression));
                }
            } else {
                throw new Exception("Timeseries not found: " + timeseriesPath);
            }
        } catch (Exception e) {
            throw new Exception("Error validating timeseries schema: " + timeseriesPath, e);
        }
    }

    /**
     * Create a new timeseries in the IoTDB instance.
     *
     * @param timeseriesPath The full path of the timeseries.
     * @param dataType       The data type.
     * @param encoding       The encoding method.
     * @param compression    The compression method.
     * @throws IoTDBConnectionException
     */
    private void createTimeseries(String timeseriesPath, TSDataType dataType, TSEncoding encoding,
            CompressionType compression) throws IoTDBConnectionException {
        SessionPool sessionPool = this.iotdbClientManager.acquireSession();
        try {
            sessionPool.createTimeseries(timeseriesPath, dataType, encoding, compression);
            System.out.println("Created new timeseries: " + timeseriesPath);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new IoTDBConnectionException("Error creating timeseries: " + timeseriesPath, e);
        }
    }

    private void createAlignedTimeseries(String deviceId, List<IoTDBMeasurement> measurements) throws Exception {
        List<String> measurementNames = new ArrayList<>();
        List<TSDataType> dataTypes = new ArrayList<>();
        List<TSEncoding> encodings = new ArrayList<>();
        List<CompressionType> compressionTypes = new ArrayList<>();

        for (IoTDBMeasurement measurement : measurements) {
            measurementNames.add(measurement.getName());
            dataTypes.add(measurement.getDataType());
            encodings.add(measurement.getEncoding());
            compressionTypes.add(measurement.getCompression());
        }

        SessionPool sessionPool = this.iotdbClientManager.acquireSession();
        try {
            sessionPool.createAlignedTimeseries(deviceId, measurementNames, dataTypes, encodings, compressionTypes,
                    null);
            System.out.println("Created aligned timeseries for device: " + deviceId);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new Exception("Error creating aligned timeseries for device: " + deviceId, e);
        }
    }
}