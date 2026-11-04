// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.boavizta;

import com.digitalpebble.spruce.modules.boavizta.aws.BoaviztAPIstatic;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Covers the sharing of the tables parsed from the bundled impacts CSVs: the Spark tasks of one
 * executor each configure their own module instance, and those instances must end up looking up
 * the one table rather than parsing the file again.
 */
class StaticImpactsLoadedOnceTest {

    @Test
    void instancesBackedByTheSameFileShareOneTable() {
        var first = new BoaviztAPIstatic();
        first.init(new HashMap<>());
        var second = new BoaviztAPIstatic();
        second.init(new HashMap<>());

        assertFalse(first.staticImpacts.isEmpty(), "The bundled CSV should have been parsed");
        assertSame(first.staticImpacts, second.staticImpacts);
    }

    @Test
    void differentFilesGetDifferentTables() {
        var aws = new BoaviztAPIstatic();
        aws.init(new HashMap<>());
        var azure = new com.digitalpebble.spruce.modules.boavizta.azure.BoaviztAPIstatic();
        azure.init(new HashMap<>());

        assertFalse(azure.staticImpacts.isEmpty(), "The bundled CSV should have been parsed");
        assertNotSame(aws.staticImpacts, azure.staticImpacts);
    }
}
