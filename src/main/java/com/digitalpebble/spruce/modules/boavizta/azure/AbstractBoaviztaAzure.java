// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.boavizta.azure;

import com.digitalpebble.spruce.Column;
import com.digitalpebble.spruce.Provider;
import com.digitalpebble.spruce.modules.boavizta.AbstractBoaviztaModule;
import org.apache.spark.sql.Row;

import static com.digitalpebble.spruce.CURColumn.*;

/**
 * Azure-specific extraction of the Boavizta instance type from a row.
 *
 * <p>Subclasses only need to plug in the lookup variant via {@link #lookupImpacts(String)}.
 */
abstract class AbstractBoaviztaAzure extends AbstractBoaviztaModule {

    private static final Column[] COLUMNS_NEEDED = new Column[]{
// TODO
};

    AbstractBoaviztaAzure() {
        // The class is AZURE-specific by definition; default the provider so callers that bypass
        // the provider-aware init still get correct behaviour.
        this.provider = Provider.AZURE;
    }

    @Override
    public final Column[] columnsNeeded() {
        return COLUMNS_NEEDED;
    }

    @Override
    protected final String extractInstanceType(Row row) {
        // TODO extraction logic
        return null;
    }
}
