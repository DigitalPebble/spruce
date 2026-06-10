package com.digitalpebble.spruce.modules.focus;

import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.EnrichmentModule;
import com.digitalpebble.spruce.FOCUSColumn;
import com.digitalpebble.spruce.Utils;
import org.apache.spark.sql.Row;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import static com.digitalpebble.spruce.SpruceColumn.*;

public class SKU implements EnrichmentModule {

    private Map<String, double[]> impacts_per_sku = new HashMap<String, double[]>();

    private static void populateMapFromCSV(String resourceFileName, Map<String, double[]> map) throws IOException {
        // Use the class loader to get the resource as an InputStream
        try (InputStream inputStream = Utils.class.getClassLoader().getResourceAsStream(resourceFileName)) {
            if (inputStream == null) {
                throw new IOException("Resource file not found: " + resourceFileName);
            }

            // TODO handle compressed files
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.startsWith("#")) {continue;}
                    String[] parts = line.split(",", -1);
                    if (parts.length == 0) continue;
                    String sku = parts[0].trim();
                    double[] values = new double[3];
                    for (int i = 0; i < 3; i++) {
                        if (i + 1 < parts.length && !parts[i + 1].isBlank()) {
                            values[i] = Double.parseDouble(parts[i + 1].trim());
                        }
                    }
                    map.put(sku, values);
                }
            }
        }
    }

    @Override
    public void init(Map<String, Object> params) {
        // this module is used for FOCUS which can be used for any of the provider
        // we probably want to separate the resource files per provider and load
        // them on demand based on the value of the first provider listed in a row
        // but for now we'll just assume a single file

        // TODO handle name better
        String resourceFileName = "focus/aws_skus_impacts.csv";
        try {
            populateMapFromCSV(resourceFileName, impacts_per_sku);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Column[] columnsNeeded() {
        return new Column[]{FOCUSColumn.SKU_ID};
    }

    @Override
    public Column[] columnsAdded() {
        return new Column[]{ENERGY_USED, EMBODIED_EMISSIONS, EMBODIED_ADP};
    }

    @Override
    public void enrich(Row row, Map<Column, Object> enrichedValues) {
        // get the sku value for the row
        String sku = FOCUSColumn.SKU_ID.getString(row);
        if (sku == null) {
            return;
        }
        double[] impacts = impacts_per_sku.get(sku);
        if (impacts == null) {
            return;
        }
        // add the impacts to the row
        enrichedValues.put(ENERGY_USED, impacts[0]);
        enrichedValues.put(EMBODIED_EMISSIONS, impacts[1]);
        enrichedValues.put(EMBODIED_ADP, impacts[2]);
    }
}
