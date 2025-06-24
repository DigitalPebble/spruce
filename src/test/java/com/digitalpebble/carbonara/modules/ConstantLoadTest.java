/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package com.digitalpebble.carbonara.modules;

import com.digitalpebble.carbonara.CarbonaraColumn;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import scala.collection.JavaConverters;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ConstantLoadTest {

    private ConstantLoad load = new ConstantLoad();

    @Test
    void process() {
        String ddl = CarbonaraColumn.CPU_LOAD.getLabel()+" DOUBLE";
        StructType schema =  StructType.fromDDL(ddl);
        Object[] values = new Object[] {null};
        Row row = new GenericRowWithSchema(values, schema);
        Row enriched = load.process(row);
        assertEquals(50d, enriched.getDouble(0));
    }
}