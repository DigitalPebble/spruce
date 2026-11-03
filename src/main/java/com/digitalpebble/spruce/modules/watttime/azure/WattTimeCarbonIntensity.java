// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.watttime.azure;

import com.digitalpebble.spruce.modules.watttime.AbstractWattTimeCarbonIntensity;
import org.apache.spark.sql.Row;

import java.time.Duration;
import java.time.Instant;

import static com.digitalpebble.spruce.AzureColumn.DATE;

/**
 * Reads the usage period of the line items of Azure cost details, which carry the day of usage
 * only: a line item covers the whole day in UTC.
 */
public class WattTimeCarbonIntensity extends AbstractWattTimeCarbonIntensity {

    @Override
    protected UsagePeriod nativeUsagePeriod(Row row) {
        Instant day = DATE.getInstant(row);
        return day == null ? null : UsagePeriod.of(day, day.plus(Duration.ofDays(1)));
    }
}
