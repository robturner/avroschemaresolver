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
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.avro.AvroTypeException;
import org.apache.avro.util.Utf8;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;

/** A {@link Decoder} for Avro's JSON data encoding. 
 * </p>
 * Construct using {@link DecoderFactory}.
 * </p>
 * JsonDecoder is not thread-safe.
 * */
public class JsonBasicDecoder extends Decoder {
  private JsonParser in;
  private static JsonFactory jsonFactory = new JsonFactory();

  static final String CHARSET = "ISO-8859-1";

  JsonBasicDecoder(InputStream in) throws IOException {
    super();
    configure(in);
  }
  
  JsonBasicDecoder(String in) throws IOException {
    super();
    configure(in);
  }

  /**
   * Reconfigures this JsonDecoder to use the InputStream provided.
   * <p/>
   * If the InputStream provided is null, a NullPointerException is thrown.
   * <p/>
   * Otherwise, this JsonDecoder will reset its state and then
   * reconfigure its input.
   * @param in
   *   The IntputStream to read from. Cannot be null.
   * @throws IOException
   * @return this JsonDecoder
   */
  public JsonBasicDecoder configure(InputStream in) throws IOException {
    if (null == in) {
      throw new NullPointerException("InputStream to read from cannot be null!");
    }
    this.in = jsonFactory.createJsonParser(in);
    this.in.nextToken();
    return this;
  }
  
  /**
   * Reconfigures this JsonDecoder to use the String provided for input.
   * <p/>
   * If the String provided is null, a NullPointerException is thrown.
   * <p/>
   * Otherwise, this JsonDecoder will reset its state and then
   * reconfigure its input.
   * @param in
   *   The String to read from. Cannot be null.
   * @throws IOException
   * @return this JsonDecoder
   */
  public JsonBasicDecoder configure(String in) throws IOException {
    if (null == in) {
      throw new NullPointerException("String to read from cannot be null!");
    }
    this.in = new JsonFactory().createJsonParser(in);
    this.in.nextToken();
    return this;
  }

  @Override
  public void readNull() throws IOException {
    if (in.getCurrentToken() == JsonToken.VALUE_NULL) {
      in.nextToken();
    } else {
      throw error("null");
    }
  }

  @Override
  public boolean readBoolean() throws IOException {
    JsonToken t = in.getCurrentToken(); 
    if (t == JsonToken.VALUE_TRUE || t == JsonToken.VALUE_FALSE) {
      in.nextToken();
      return t == JsonToken.VALUE_TRUE;
    } else {
      throw error("boolean");
    }
  }

  @Override
  public int readInt() throws IOException {
    if (in.getCurrentToken().isNumeric()) {
      int result = in.getIntValue();
      in.nextToken();
      return result;
    } else {
      throw error("int");
    }
  }
    
  @Override
  public long readLong() throws IOException {
    if (in.getCurrentToken().isNumeric()) {
      long result = in.getLongValue();
      in.nextToken();
      return result;
    } else {
      throw error("long");
    }
  }

  @Override
  public float readFloat() throws IOException {
    if (in.getCurrentToken().isNumeric()) {
      float result = in.getFloatValue();
      in.nextToken();
      return result;
    } else {
      throw error("float");
    }
  }

  @Override
  public double readDouble() throws IOException {
    if (in.getCurrentToken().isNumeric()) {
      double result = in.getDoubleValue();
      in.nextToken();
      return result;
    } else {
      throw error("double");
    }
  }
    
  @Override
  public Utf8 readString(Utf8 old) throws IOException {
    return new Utf8(readString());
  }

  @Override
  public String readString() throws IOException {
    if (in.getCurrentToken() != JsonToken.VALUE_STRING) {
      throw error("string");
    }
    String result = in.getText();
    in.nextToken();
    return result;
  }

  @Override
  public void skipString() throws IOException {
    if (in.getCurrentToken() != JsonToken.VALUE_STRING) {
      throw error("string");
    }
    in.nextToken();
  }

  @Override
  public ByteBuffer readBytes(ByteBuffer old) throws IOException {
    if (in.getCurrentToken() == JsonToken.VALUE_STRING) {
      byte[] result = readByteArray();
      in.nextToken();
      return ByteBuffer.wrap(result);
    } else {
      throw error("bytes");
    }
  }

  private byte[] readByteArray() throws IOException {
    byte[] result = in.getText().getBytes(CHARSET);
    return result;
  }

  @Override
  public void skipBytes() throws IOException {
    if (in.getCurrentToken() == JsonToken.VALUE_STRING) {
      in.nextToken();
    } else {
      throw error("bytes");
    }
  }

  @Override
  public void readFixed(byte[] bytes, int start, int len) throws IOException {
    if (in.getCurrentToken() == JsonToken.VALUE_STRING) {
      byte[] result = readByteArray();
      in.nextToken();
      if (result.length != len) {
        throw new AvroTypeException("Expected fixed length " + len
            + ", but got " + result.length);
      }
      System.arraycopy(result, 0, bytes, start, len);
    } else {
      throw error("fixed");
    }
  }

  @Override
  public void skipFixed(int length) throws IOException {
    doSkipFixed(length);
  }

  private void doSkipFixed(int length) throws IOException {
    if (in.getCurrentToken() == JsonToken.VALUE_STRING) {
      byte[] result = readByteArray();
      in.nextToken();
      if (result.length != length) {
        throw new AvroTypeException("Expected fixed length " + length
            + ", but got" + result.length);
      }
    } else {
      throw error("fixed");
    }
  }

  @Override
  public String readEnumTag() throws IOException {
    if (in.getCurrentToken() == JsonToken.VALUE_STRING) {
      String enumSymbol = in.getText();
      in.nextToken();
      return enumSymbol;
    } else {
      throw error("enum");
    }
  }

  @Override
  public long readArrayStart() throws IOException {
    if (in.getCurrentToken() == JsonToken.START_ARRAY) {
      in.nextToken();
      return doArrayNext();
    } else {
      throw error("array-start");
    }
  }

  @Override
  public long arrayNext() throws IOException {
    return doArrayNext();
  }

  private long doArrayNext() throws IOException {
    if (in.getCurrentToken() == JsonToken.END_ARRAY) {
      in.nextToken();
      return 0;
    } else {
      return 1;
    }
  }

  @Override
  public long skipArray() throws IOException {
    if (in.getCurrentToken() == JsonToken.START_ARRAY) {
      in.skipChildren();
      in.nextToken();
    } else {
      throw error("array-start");
    }
    return 0;
  }

  @Override
  public long readMapStart() throws IOException {
    if (in.getCurrentToken() == JsonToken.START_OBJECT) {
      in.nextToken();
      return doMapNext();
    } else {
      throw error("map-start");
    }
  }

  @Override
  public long mapNext() throws IOException {
    return doMapNext();
  }

  private long doMapNext() throws IOException {
    if (in.getCurrentToken() == JsonToken.END_OBJECT) {
      in.nextToken();
      return 0;
    } else {
      if (in.getCurrentToken() != JsonToken.FIELD_NAME) {
        throw error("map-key");
      }      
      return 1;
    }
  }

  @Override
  public long skipMap() throws IOException {
    if (in.getCurrentToken() == JsonToken.START_OBJECT) {
      in.skipChildren();
      in.nextToken();
    } else {
      throw error("map-start");
    }
    return 0;
  }
  
  @Override
  public void readUnionStart() throws IOException {
  }

  @Override
  public String readUnionTag() throws IOException {
    String label;
    if (in.getCurrentToken() == JsonToken.VALUE_NULL) {
      label = "null";
    } else if (in.getCurrentToken() == JsonToken.START_OBJECT &&
               in.nextToken() == JsonToken.FIELD_NAME) {
      label = in.getText();
      in.nextToken();
    } else {
      throw error("start-union");
    }
    return label;
  }

  @Override
  public void readUnionEnd() throws IOException {
    if (in.getCurrentToken() == JsonToken.END_OBJECT) {
      in.nextToken();
    } else {
      throw error("end-union");
    }
  }

  private AvroTypeException error(String type) {
    return new AvroTypeException("Expected " + type +
        ". Got " + in.getCurrentToken());
  }

  @Override
  public void readRecordStart() throws IOException {
    if (in.getCurrentToken() == JsonToken.START_OBJECT) {
      in.nextToken();
    } else {
      throw error("record-start");
    }
  }

  @Override
  public String fieldNext() throws IOException {
    if (in.getCurrentToken() == JsonToken.FIELD_NAME) {
      String result = in.getText();
      in.nextToken();
      return result;
    } else {
      throw error("field-start");
    }
  }

  @Override
  public void readRecordEnd() throws IOException {
    if (in.getCurrentToken() == JsonToken.END_OBJECT) {
      in.nextToken();
    } else {
      throw error("record-end");
    }
  }

  @Override
  public int readEnum() throws IOException {
    throw new UnsupportedOperationException("JsonDecoder cannot read integer enums");
  }

  @Override
  public int readIndex() throws IOException {
    throw new UnsupportedOperationException("JsonDecoder cannot read integer union indexes");
  }

  @Override
  public String readMapKey() throws IOException {
    if (in.getCurrentToken() == JsonToken.FIELD_NAME) {
      String result = in.getText();
      in.nextToken();
      return result;
    } else {
      throw error("map-key");
    }
  }
}

