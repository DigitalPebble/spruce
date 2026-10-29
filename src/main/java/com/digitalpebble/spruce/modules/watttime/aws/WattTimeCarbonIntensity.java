// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.watttime.aws;

import com.digitalpebble.spruce.modules.watttime.AbstractWattTimeCarbonIntensity;
import org.apache.spark.sql.Row;

import static com.digitalpebble.spruce.CURColumn.LINE_ITEM_USAGE_END_DATE;
import static com.digitalpebble.spruce.CURColumn.LINE_ITEM_USAGE_START_DATE;

/** Reads the usage period of the line items of CUR reports. */
public class WattTimeCarbonIntensity extends AbstractWattTimeCarbonIntensity {

    @Override
    protected UsagePeriod nativeUsagePeriod(Row row) {
        return UsagePeriod.of(LINE_ITEM_USAGE_START_DATE.getInstant(row), LINE_ITEM_USAGE_END_DATE.getInstant(row));
    }
}
