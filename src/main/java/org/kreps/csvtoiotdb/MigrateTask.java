package org.kreps.csvtoiotdb;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.kreps.csvtoiotdb.configs.csv.CsvSettings;
import org.kreps.csvtoiotdb.configs.iotdb.IoTDBDevice;
import org.kreps.csvtoiotdb.converter.RowData;

/**
 * Represents a migration task that processes CSV files and writes data to
 * IoTDB.
 */
public class MigrateTask implements Runnable {
    private final BlockingQueue<CsvSettings> csvSettingsQueue;
    private final List<IoTDBDevice> iotdbSettingsList;
    private final Converter converter;
    private final IoTDBWriter writer;
    private final int batchSize;

    /**
     * Constructs a MigrateTask instance.
     *
     * @param csvSettingsQueue The queue of CsvSettings to process.
     * @param converter        The Converter instance for data conversion.
     * @param writer           The IoTDBWriter instance for writing data.
     * @param batchSize        The number of rows to process in each batch.
     */
    public MigrateTask(BlockingQueue<CsvSettings> csvSettingsQueue, List<IoTDBDevice> iotdbSettingsList,
            Converter converter, IoTDBWriter writer, int batchSize) {
        this.csvSettingsQueue = csvSettingsQueue;
        this.iotdbSettingsList = iotdbSettingsList;
        this.converter = converter;
        this.writer = writer;
        this.batchSize = batchSize;
    }

    /**
     * Executes the migration task.
     */
    @Override
    public void run() {
        while (true) {
            CsvSettings csvSettings;
            try {
                csvSettings = csvSettingsQueue.poll();
                if (csvSettings == null) {
                    break;
                }
            } catch (Exception e) {
                e.printStackTrace();
                continue;
            }

            try (CSVReader csvReader = new CSVReader(csvSettings, this.batchSize)) {
                List<Map<String, Object>> batch;
                while ((batch = csvReader.readBatch()) != null) {
                    Map<String, List<RowData>> deviceDataMap = converter.convert(batch);
                    writer.writeData(deviceDataMap, this.iotdbSettingsList);
                }
            } catch (IOException e) {
                e.printStackTrace(); // Replace with proper logging
            }
        }
    }
}
