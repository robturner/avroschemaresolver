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
package org.apache.avro.io;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.avro.util.Utf8;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.util.DefaultPrettyPrinter;
import org.codehaus.jackson.util.MinimalPrettyPrinter;

/** An {@link Encoder} for Avro's JSON data encoding. 
 * </p>
 * Construct using {@link EncoderFactory}.
 * </p>
 * JsonEncoder buffers output, and data may not appear on the output
 * until {@link Encoder#flush()} is called.
 * </p>
 * JsonEncoder is not thread-safe.
 * */
public class JsonBasicEncoder extends Encoder {
  private static final String LINE_SEPARATOR = System.getProperty("line.separator");
  private JsonGenerator out;

  JsonBasicEncoder(OutputStream out) throws IOException {
    this(getJsonGenerator(out, false));
  }

  JsonBasicEncoder(OutputStream out, boolean pretty) throws IOException {
    this(getJsonGenerator(out, pretty));
  }

  JsonBasicEncoder(JsonGenerator out) throws IOException {
    configure(out);
  }

  @Override
  public void flush() throws IOException {
    if (out != null) {
      out.flush();
    }
  }

  // by default, one object per line.
  // with pretty option use default pretty printer with root line separator.
  private static JsonGenerator getJsonGenerator(OutputStream out, boolean pretty)
      throws IOException {
    if (null == out)
      throw new NullPointerException("OutputStream cannot be null"); 
    JsonGenerator g
      = new JsonFactory().createJsonGenerator(out, JsonEncoding.UTF8);
    if (pretty) {
      DefaultPrettyPrinter pp = new DefaultPrettyPrinter() {
        //@Override
        public void writeRootValueSeparator(JsonGenerator jg)
            throws IOException
        {
          jg.writeRaw(LINE_SEPARATOR);
        }
      };
      g.setPrettyPrinter(pp);
    } else {
      MinimalPrettyPrinter pp = new MinimalPrettyPrinter();
      pp.setRootValueSeparator(LINE_SEPARATOR);
      g.setPrettyPrinter(pp);
    }
    return g;
  }
  
  /**
   * Reconfigures this JsonEncoder to use the output stream provided.
   * <p/>
   * If the OutputStream provided is null, a NullPointerException is thrown.
   * <p/>
   * Otherwise, this JsonEncoder will flush its current output and then
   * reconfigure its output to use a default UTF8 JsonGenerator that writes
   * to the provided OutputStream.
   * 
   * @param out
   *          The OutputStream to direct output to. Cannot be null.
   * @throws IOException
   * @return this JsonEncoder
   */
  public JsonBasicEncoder configure(OutputStream out) throws IOException {
    this.configure(getJsonGenerator(out, false));
    return this;
  }
  
  /**
   * Reconfigures this JsonEncoder to output to the JsonGenerator provided.
   * <p/>
   * If the JsonGenerator provided is null, a NullPointerException is thrown.
   * <p/>
   * Otherwise, this JsonEncoder will flush its current output and then
   * reconfigure its output to use the provided JsonGenerator.
   * 
   * @param generator
   *          The JsonGenerator to direct output to. Cannot be null.
   * @throws IOException
   * @return this JsonEncoder
   */
  public JsonBasicEncoder configure(JsonGenerator generator) throws IOException {
    if (null == generator)
      throw new NullPointerException("JsonGenerator cannot be null");
    this.out = generator;
    return this;
  }

  @Override
  public void writeNull() throws IOException {
    out.writeNull();
  }

  @Override
  public void writeBoolean(boolean b) throws IOException {
    out.writeBoolean(b);
  }

  @Override
  public void writeInt(int n) throws IOException {
    out.writeNumber(n);
  }

  @Override
  public void writeLong(long n) throws IOException {
    out.writeNumber(n);
  }

  @Override
  public void writeFloat(float f) throws IOException {
    out.writeNumber(f);
  }

  @Override
  public void writeDouble(double d) throws IOException {
    out.writeNumber(d);
  }

  @Override
  public void writeString(Utf8 utf8) throws IOException {
    writeString(utf8.toString());
  }
  
  @Override 
  public void writeString(String str) throws IOException {
      out.writeString(str);
  }

  @Override
  public void writeBytes(ByteBuffer bytes) throws IOException {
    if (bytes.hasArray()) {
      writeBytes(bytes.array(), bytes.position(), bytes.remaining());
    } else {
      byte[] b = new byte[bytes.remaining()];
      for (int i = 0; i < b.length; i++) {
        b[i] = bytes.get();
      }
      writeBytes(b);
    }
  }

  @Override
  public void writeBytes(byte[] bytes, int start, int len) throws IOException {
    writeByteArray(bytes, start, len);
  }

  private void writeByteArray(byte[] bytes, int start, int len)
    throws IOException {
    out.writeString(
        new String(bytes, start, len, JsonBasicDecoder.CHARSET));
  }

  @Override
  public void writeFixed(byte[] bytes, int start, int len) throws IOException {
    writeByteArray(bytes, start, len);
  }

  @Override
  public void writeEnum(int e, String eTag) throws IOException {
    out.writeString(eTag);
  }

  @Override
  public void writeArrayStart() throws IOException {
    out.writeStartArray();
  }

  @Override
  public void writeArrayEnd() throws IOException {
    out.writeEndArray();
  }

  @Override
  public void writeMapStart() throws IOException {
    out.writeStartObject();
  }

  @Override
  public void writeMapEnd() throws IOException {
    out.writeEndObject();
  }

  @Override
  public void startItem() throws IOException {
  }

  @Override
  public void writeIndex(int unionIndex, String unionTag) throws IOException {
    out.writeFieldName(unionTag);
  }

  @Override
  public void setItemCount(long itemCount) throws IOException {
  }

  @Override
  public void writeEnum(int e) throws IOException {
    throw new UnsupportedOperationException("JsonEncoder cannot write integer enum indexes");
  }

  @Override
  public void writeIndex(int unionIndex) throws IOException {
    throw new UnsupportedOperationException("JsonEncoder cannot write integer union indexes");
  }

  @Override
  public void writeUnionStart() throws IOException {
    out.writeStartObject();
  }

  @Override
  public void writeUnionEnd() throws IOException {
    out.writeEndObject();
  }

  @Override
  public void writeRecordStart() throws IOException {
    out.writeStartObject();
  }

  @Override
  public void writeRecordEnd() throws IOException{
    out.writeEndObject();
  }

  @Override
  public void startField(String name) throws IOException {
    out.writeFieldName(name);
  }

  @Override
  public void writeMapKey(String key) throws IOException {
    out.writeFieldName(key);
  }  
}

