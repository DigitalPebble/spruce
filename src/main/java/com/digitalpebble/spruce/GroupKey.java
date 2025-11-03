// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce;

import java.io.Serializable;
import java.util.Objects;

/** USed by split jobs to group by resource ID within a time frame **/
public class GroupKey implements Serializable {
    private final String date;
    private final String resourceId;

    public GroupKey(String date, String resourceId) {
        this.date = date;
        this.resourceId = resourceId;
    }

    public String getDate() {
        return date;
    }

    public String getResourceId() {
        return resourceId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof GroupKey groupKey)) return false;
        return Objects.equals(date, groupKey.date) && Objects.equals(resourceId, groupKey.resourceId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(date, resourceId);
    }

    @Override
    public String toString() {
        return "GroupKey{" + "date='" + date + '\'' + ", resourceId='" + resourceId + '\'' + '}';
    }
}
