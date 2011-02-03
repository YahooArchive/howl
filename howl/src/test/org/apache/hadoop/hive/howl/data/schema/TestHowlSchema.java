/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.howl.data.schema;

import junit.framework.TestCase;
import org.apache.hadoop.hive.howl.common.HowlException;

import java.util.ArrayList;
import java.util.List;

public class TestHowlSchema extends TestCase {
  public void testCannotAddFieldMoreThanOnce() throws HowlException {
    List<HowlFieldSchema> fieldSchemaList = new ArrayList<HowlFieldSchema>();
    fieldSchemaList.add(new HowlFieldSchema("name", HowlFieldSchema.Type.STRING, "What's your handle?"));
    fieldSchemaList.add(new HowlFieldSchema("age", HowlFieldSchema.Type.INT, "So very old"));

    HowlSchema schema = new HowlSchema(fieldSchemaList);

    assertTrue(schema.getFieldNames().contains("age"));
    assertEquals(2, schema.getFields().size());

    try {
      schema.append(new HowlFieldSchema("age", HowlFieldSchema.Type.INT, "So very old"));
      fail("Was able to append field schema with same name");
    } catch(HowlException he) {
      assertTrue(he.getMessage().contains("Attempt to append HowlFieldSchema with already existing name: age."));
    }

    assertTrue(schema.getFieldNames().contains("age"));
    assertEquals(2, schema.getFields().size());

    // Should also not be able to add fields of different types with same name
    try {
      schema.append(new HowlFieldSchema("age", HowlFieldSchema.Type.STRING, "Maybe spelled out?"));
      fail("Was able to append field schema with same name");
    } catch(HowlException he) {
      assertTrue(he.getMessage().contains("Attempt to append HowlFieldSchema with already existing name: age."));
    }

    assertTrue(schema.getFieldNames().contains("age"));
    assertEquals(2, schema.getFields().size());
  }

  public void testCannotInstantiateSchemaWithRepeatedFieldNames() throws HowlException {
      List<HowlFieldSchema> fieldSchemaList = new ArrayList<HowlFieldSchema>();

      fieldSchemaList.add(new HowlFieldSchema("memberID", HowlFieldSchema.Type.INT, "as a number"));
      fieldSchemaList.add(new HowlFieldSchema("location", HowlFieldSchema.Type.STRING, "there's Waldo"));

      // No duplicate names.  This should be ok
      HowlSchema schema = new HowlSchema(fieldSchemaList);

      fieldSchemaList.add(new HowlFieldSchema("memberID", HowlFieldSchema.Type.STRING, "as a String"));

      // Now a duplicated field name.  Should fail
      try {
        HowlSchema schema2 = new HowlSchema(fieldSchemaList);
        fail("Able to add duplicate field name");
      } catch (IllegalArgumentException iae) {
        assertTrue(iae.getMessage().contains("Field named memberID already exists"));
      }
  }
}
