package org.kreps.csvtoiotdb;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;

import java.util.List;
import java.util.Optional;

import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.StatementExecutionException;
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
            for (IoTDBMeasurement measurement : device.getMeasurements()) {
                TSDataType dataType = measurement.getDataType();
                TSEncoding encoding = measurement.getEncoding();
                CompressionType compression = measurement.getCompression();

                // Ensure the timeseries exists or create it
                ensureTimeseries(deviceId, measurement.getName(), dataType, encoding, compression);
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
    private boolean checkTimeseriesExists(String timeseriesPath) throws IoTDBConnectionException {
        String sql = String.format("SHOW TIMESERIES %s", timeseriesPath);

        try (Session session = this.iotdbClientManager.acquireSession()) {
            SessionDataSet dataSet = session.executeQueryStatement(sql);
            return dataSet.hasNext();
        } catch (InterruptedException | IoTDBConnectionException | StatementExecutionException e) {
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

        try (Session session = this.iotdbClientManager.acquireSession()) {
            SessionDataSet dataSet = session.executeQueryStatement(sql);

            if (dataSet.hasNext()) {
                List<Field> schemaInfo = dataSet.next().getFields();

                // Use indexes to extract the data type, encoding, and compression
                // Assuming the positions are: 2 = dataType, 3 = encoding, 4 = compression

                TSDataType actualDataType = TSDataType.valueOf(schemaInfo.get(3).getStringValue());
                TSEncoding actualEncoding = TSEncoding.valueOf(schemaInfo.get(4).getStringValue());
                CompressionType actualCompression = CompressionType.valueOf(schemaInfo.get(5).getStringValue());

                // Compare the extracted values with the expected values
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
        try (Session session = this.iotdbClientManager.acquireSession()) {
            session.createTimeseries(timeseriesPath, dataType, encoding, compression);
            System.out.println("Created new timeseries: " + timeseriesPath);
        } catch (InterruptedException | IoTDBConnectionException | StatementExecutionException e) {
            throw new IoTDBConnectionException("Error creating timeseries: " + timeseriesPath, e);
        }
    }
}