# Write a New Module

An `EnrichmentModule` is the unit of extension in SPRUCE. Each module reads columns from
the CUR input row and/or from values set by earlier modules, then writes its results into a
shared map. The pipeline materialises one output row per CUR row at the end, avoiding
per-module row copies.

## Implement the interface

Create a class that implements `com.digitalpebble.spruce.EnrichmentModule`:

```java
package com.example.spruce.modules;

import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.EnrichmentModule;
import org.apache.spark.sql.Row;

import java.util.Map;

import static com.digitalpebble.spruce.CURColumn.LINE_ITEM_PRODUCT_CODE;
import static com.digitalpebble.spruce.CURColumn.USAGE_AMOUNT;
import static com.digitalpebble.spruce.SpruceColumn.ENERGY_USED;

public class MyServiceEnergy implements EnrichmentModule {

    private double coefficient = 0.005; // kWh per usage unit

    @Override
    public void init(Map<String, Object> params) {
        Double value = (Double) params.get("coefficient");
        if (value != null) {
            coefficient = value;
        }
    }

    @Override
    public Column[] columnsNeeded() {
        return new Column[]{LINE_ITEM_PRODUCT_CODE, USAGE_AMOUNT};
    }

    @Override
    public Column[] columnsAdded() {
        return new Column[]{ENERGY_USED};
    }

    @Override
    public void enrich(Row row, Map<Column, Object> enrichedValues) {
        String productCode = LINE_ITEM_PRODUCT_CODE.getString(row);
        if (!"MyServiceCode".equals(productCode)) return;

        double amount = USAGE_AMOUNT.getDouble(row);
        enrichedValues.put(ENERGY_USED, amount * coefficient);
    }
}
```

### `columnsNeeded()` and `columnsAdded()`

These declare the module's dependencies and outputs. SPRUCE uses them to validate the schema
at startup — it will fail fast if a required column is missing from the CUR report.

`columnsNeeded()` should list every column you read, whether from the original CUR row
(`CURColumn`) or from the shared enrichment map (`SpruceColumn`).

### `init()`

Called once per partition before any rows are processed. Use it to load configuration
values passed via the JSON config, or to read resource files. The `params` map is
`null` when no `config` block is present in the JSON.

### `enrich()`

Called once per usage row (tax, discount and fee rows are filtered out by the pipeline).
Two sources of data are available:

**Reading CUR input columns** — use the typed getters on `CURColumn`:

```java
String productCode = LINE_ITEM_PRODUCT_CODE.getString(row);
double amount      = USAGE_AMOUNT.getDouble(row);
boolean missing    = USAGE_AMOUNT.isNullAt(row);

// Optional field — returns null instead of throwing if absent from schema:
String region = PRODUCT_REGION_CODE.getString(row, /* optional= */ true);
```

**Reading values set by earlier modules** — use the typed getters on `SpruceColumn`:

```java
Double energyUsed = ENERGY_USED.getDouble(enrichedValues);
String region     = REGION.getString(enrichedValues);
```

Both getters return `null` when the value is absent, so always null-check before using them.

**Writing results** — put values into the shared map:

```java
enrichedValues.put(ENERGY_USED, computedValue);
```

If a module only runs for certain rows, simply return early without putting anything into
the map. Columns not written remain `null` in the output row.

<div class="warning">
Do not call <code>row.fieldIndex()</code> directly. Use <code>CURColumn</code> and
<code>SpruceColumn</code> getters, which cache the field index per schema and avoid
repeated lookups across rows in the same partition.
</div>

## Register the module

Modules are registered in a JSON config file. Copy `default-config.json` from the JAR
(or from the repository) as a starting point, then add your module:

```json
{
  "modules": [
    { "className": "com.digitalpebble.spruce.modules.RegionExtraction" },
    { "className": "com.example.spruce.modules.MyServiceEnergy",
      "config": { "coefficient": 0.007 } },
    { "className": "com.digitalpebble.spruce.modules.PUE" },
    { "className": "com.digitalpebble.spruce.modules.electricitymaps.AverageCarbonIntensity" },
    { "className": "com.digitalpebble.spruce.modules.OperationalEmissions" }
  ]
}
```

See instructions on [Configure the modules](../howto/config_modules.md).

Pass the config file to the Spark job with `-c`:

```
spark-submit --class com.digitalpebble.spruce.SparkJob ./target/spruce-*.jar \
  -i ./curs -o ./output -c ./my-config.json
```

<div class="warning">
Module order matters. A module can only read values from the enrichment map that were
written by modules listed earlier in the chain. For example, <code>PUE</code> must run
after any module that populates <code>operational_energy_kwh</code>, and
<code>OperationalEmissions</code> must run last.
</div>

## Include your module in the build

If your module lives outside the SPRUCE source tree, build it as a JAR and add it to the
Spark job's classpath:

```
spark-submit --class com.digitalpebble.spruce.SparkJob \
  --jars ./my-module.jar \
  ./target/spruce-*.jar \
  -i ./curs -o ./output -c ./my-config.json
```

If you are adding the module directly to the SPRUCE source tree, place it under
`src/main/java/com/digitalpebble/spruce/modules/` and run `mvn package` to include it
in the fat JAR.

See [Contribute to SPRUCE](contributing.md) if you would like to share the module with
the community.
