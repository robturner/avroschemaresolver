/**
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
package org.apache.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class TestSchemaResolver  {

  public static class Timer {
    private long start;
    private long last = 0;
    
    protected void starting() {
      start = System.currentTimeMillis();
    }

    public void finished(String description){
      long end = System.currentTimeMillis();
      long l = end - start;
      long faster = 0;
      String report = "           ";
      if (last != 0) {
        faster = (l - last) * 100 / ((l == 0) ? 1 : l);
        last = 0;
        report = faster + "% faster";
      } else 
        last = l;
      System.out.println("Test " +  " " + description + " " + report + " " + name 
          + " took " + l + "ms " + last);
    }
  }

  private Timer writeTimer = new Timer();
  private Timer readTimer = new Timer();

  private static enum SERDE {
    BINARY,
    BLOCKING,
    JSON
  }

  private static final int COUNT = 10;
  private static String name;
  
  private SERDE serde;
  private boolean usingSchemaResolution;
  private Schema schema;
  private byte[] bytes;

  public TestSchemaResolver(Schema schema, SERDE serde, boolean useSchemaResolution) {
    this.serde = serde;
    this.usingSchemaResolution = useSchemaResolution;
    this.schema = schema;
    name = serde + " " + useSchemaResolution + " " + schema;
  }

  @Parameters
  public static List<Object[]> parameters() {
    Object[][] parameters = new Object[][] {
        { SERDE.BINARY, true },
        { SERDE.BINARY, false },
        { SERDE.BLOCKING, true },
        { SERDE.BLOCKING, false },
        { SERDE.JSON, true },
        { SERDE.JSON, false }
    };
    Object[][] result = new Object[schemas.length * parameters.length][3];
    int i = 0;
    for (Schema schema : schemas){
      for (Object[] parameter : parameters) {
        result[i][0] = schema;
        result[i][1] = parameter[0];
        result[i][2] = parameter[1];
        i++;
      }
    }
    return Arrays.asList(result);
  }
  
  @Before
  public void getBytes() throws IOException, InterruptedException {
      writeTimer.starting();
      GenericDatumWriter<Object> datumWriter = new GenericDatumWriter<Object>(schema);
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      Encoder encoder;
      switch (serde) {
      case BINARY:
        encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
        break;
      case BLOCKING:
        encoder = EncoderFactory.get().blockingBinaryEncoder(outputStream, null);
        break;
      case JSON:
        if (usingSchemaResolution)
          encoder = EncoderFactory.get().jsonBasicEncoder(outputStream);
        else
          encoder = EncoderFactory.get().jsonEncoder(schema, outputStream);
        break;
      default:
        throw new RuntimeException("Incorrect SERDE value");
      }
      for (Object object : new RandomData(schema, COUNT, 0)) {
        datumWriter.write(object, encoder);
      }
      encoder.flush();
      bytes = outputStream.toByteArray();
      writeTimer.finished("writer");
  }

  @Test
  public void test() throws IOException {
    readTimer.starting();
    GenericDatumReader<Object> datumReader = new GenericDatumReader<Object>(schema);
    datumReader.setUsingSchemaResolution(usingSchemaResolution);
    Decoder decoder = null;
    String input = new String(bytes, "UTF-8");
    Object object = null;
      DecoderFactory decoderFactory = DecoderFactory.get();
      switch (serde) {
      case BINARY:
      case BLOCKING:
        decoder = decoderFactory.binaryDecoder(bytes, (BinaryDecoder) decoder);
        break;
      case JSON:
        if (usingSchemaResolution)
          decoder = decoderFactory.jsonBasicDecoder(input);
        else
          decoder = decoderFactory.jsonDecoder(schema, input);
        break;
      default:
        throw new RuntimeException("Incorrect SERDE value");
      }
      for (int i = 0; i < COUNT; i++) {
        object = datumReader.read(object, decoder);
      }
      readTimer.finished("reader");
  }

  private static Schema[] simpleSchemas = {
    Schema.create(Type.NULL),
    Schema.create(Type.INT),
    Schema.create(Type.LONG),
    Schema.create(Type.FLOAT),
    Schema.create(Type.DOUBLE),
    Schema.create(Type.BOOLEAN),
    Schema.create(Type.STRING),
    Schema.createFixed("FIXED", "", "", 1),
    Schema.createEnum("ENUM", "", "", Arrays.asList("e0", "e1"))
  };

  private static Schema[] complexSchemas(Schema[] schemas) {
    List<Schema> complex = new ArrayList<Schema>();
    for (Schema schema : schemas) {
      complex.add(schema);
      complex.add(Schema.createArray(schema));
      complex.add(Schema.createMap(schema));
      Schema record = Schema.createRecord("RECORD_" + schema.getType(), "", "", false);
      record.setFields(Arrays.asList(
          new Field("f0", schema, "", null), 
          new Field("f1", schema, "", null),
          new Field("f2", schema, "", null),
          new Field("f3", schema, "", null)));
      complex.add(record);
      if (schema.getType() != Type.UNION)
        complex.add(Schema.createUnion(Arrays.asList(schema)));
    }
    return complex.toArray(new Schema[0]);
  }
  
  private static Schema[] schemas = complexSchemas(complexSchemas(simpleSchemas));
}
