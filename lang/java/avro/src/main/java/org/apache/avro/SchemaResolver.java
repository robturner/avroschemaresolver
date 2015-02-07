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

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.SeenPair;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonBasicDecoder;
import org.codehaus.jackson.JsonNode;

/**
 * The class that generates a resolving grammar to resolve between two
 * schemas.
 */
public class SchemaResolver {
  /**
   * Resolves the writer schema <tt>writer</tt> and the reader schema
   * <tt>reader</tt> and returns the start Schema for the grammar generated. 
   * @param writer    The schema used by the writer
   * @param reader    The schema used by the reader
   * @return          The start Schema for the resolving grammar
   * @throws IOException 
   */
  public static final Schema resolve(Schema writer, Schema reader)
      throws IOException {
    if (writer == reader)
      return writer;
    return resolve(writer, reader, new HashMap<SeenPair, Schema>());
  }

  /**
   * Resolves the writer schema <tt>writer</tt> and the reader schema
   * <tt>reader</tt> and returns the start Schema for the grammar generated.
   * If there is already a Schema in the map <tt>seen</tt> for resolving the
   * two schemas, then that Schema is returned. Otherwise a new Schema is
   * generated and returned. 
   * @param writer    The schema used by the writer
   * @param reader    The schema used by the reader
   * @param seen      The &lt;reader-schema, writer-schema&gt; to Schema
   * map of start Schemas of resolving grammars so far.
   * @return          The start Schema for the resolving grammar
   * @throws IOException 
   */
  public static final Schema resolve(Schema writer, Schema reader,
      Map<SeenPair, Schema> seen) throws IOException
  {
    final Schema.Type writerType = writer.getType();
    final Schema.Type readerType = reader.getType();

    if (writerType == readerType) {
      switch (writerType) {
      case NULL:
      case BOOLEAN:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case STRING:
      case BYTES:
        return Schema.create(writerType);
      case FIXED:
        if (writer.getFullName().equals(reader.getFullName())
            && writer.getFixedSize() == reader.getFixedSize()) {
          return Schema.createFixed(writer.getName(), writer.getDoc(), writer.getNamespace(), writer.getFixedSize());
        }
        break;

      case ENUM:
        if (writer.getFullName() == null
                || writer.getFullName().equals(reader.getFullName())) {
          return Schema.createEnum(writer.getFullName(), writer.getDoc(), writer.getNamespace(), 
                  resolveEnum(writer.getEnumSymbols(), reader.getEnumSymbols()));
        }
        break;

      case ARRAY:
        return Schema.createArray(resolve(writer.getElementType(),
                reader.getElementType(), seen));
      
      case MAP:
        return Schema.createMap(resolve(writer.getValueType(),
                reader.getValueType(), seen));
      case RECORD:
        return resolveRecords(writer, reader, seen);
      case UNION:
        return resolveUnion(writer, reader, seen);
      default:
        throw new AvroRuntimeException("Unkown type for schema: " + writerType);
      }
    } else {  // writer and reader are of different types
      if (writerType == Schema.Type.UNION) {
        return resolveUnion(writer, reader, seen);
      }

      switch (readerType) {
      case LONG:
        switch (writerType) {
        case INT:
          return Schema.create(writerType).setReader(reader);
        }
        break;

      case FLOAT:
        switch (writerType) {
        case INT:
        case LONG:
          return Schema.create(writerType).setReader(reader);
        }
        break;

      case DOUBLE:
        switch (writerType) {
        case INT:
        case LONG:
        case FLOAT:
          return Schema.create(writerType).setReader(reader);
        }
        break;

      case UNION:
        int j = bestBranch(reader, writer);
        if (j >= 0) {
          return resolve(writer, reader.getTypes().get(j), seen);
        }
        break;
      case NULL:
      case BOOLEAN:
      case INT:
      case STRING:
      case BYTES:
      case ENUM:
      case ARRAY:
      case MAP:
      case RECORD:
        break;
      default:
        throw new AvroRuntimeException("Unexpected schema type: " + readerType);
      }
    }
    throw new AvroTypeException("Found " + writer.getFullName()
                        + ", expecting " + reader.getFullName());
  }

  private static Schema resolveUnion(Schema writer, Schema reader,
       Map<SeenPair, Schema> seen) throws IOException {
    List<Schema> schemas = new ArrayList<Schema>();
    /**
     * We construct a new UnionSchema with resolved writer types.
     */
    int resolvedCount = 0;
    for (Schema writerType : writer.getTypes()) {
      try {
        schemas.add(resolve(writerType, reader, seen));
        resolvedCount++;
      } catch (AvroTypeException e) {
        schemas.add(resolve(writerType, writerType, seen).setReader(null));
      }
    }
    if (resolvedCount == 0) {
      throw new AvroTypeException("No resolved types from union " + writer.getFullName()
          + " to " + reader.getFullName());
    }
    
    return Schema.createUnion(schemas).setReader(reader);
  }

  private static Field cloneField(Field f, int position, Schema schema) {
//    System.out.println(f.name() + " " + position + " " + f.defaultValue());
    return new Field(f.name(), position, schema, f.doc(), f.defaultValue(), f.order());
  }

  private static Schema resolveRecords(Schema writer, Schema reader,
      Map<SeenPair, Schema> seen) throws IOException {
    Schema result = seen.get(writer);
    if (result == null) {
      List<Field> wfields = writer.getFields();
      List<Field> rfields = reader.getFields();
      /*
       * Every field in read-record with no default value must be in write-record.
       * Write record may have additional fields, which will be skipped during read.
       */
      List<Field> fields = new ArrayList<Field>();

      for (Field writerField : wfields) {
        Field readerField = reader.getField(writerField.name());
        if (readerField != null) {
          fields.add(cloneField(readerField, readerField.pos(), 
              resolve(writerField.schema(), readerField.schema(), seen)));
        } else {
          fields.add(cloneField(writerField, Field.SKIPPED_FIELD, 
              resolve(writerField.schema(), writerField.schema(), seen)));
        }
      }

      for (Field readerField : rfields) {
        if (writer.getField(readerField.name()) == null) {
          if (readerField.defaultValue() == null) {
            throw new AvroTypeException("Reader record " + reader.getFullName() + " has field " + 
                readerField.name() + " with no default value and there is no matching writer field");
          } else {
            fields.add(cloneField(readerField, readerField.pos(), readerField.schema()).
                setDefaultObject(getDefaultObject(readerField)));
          }
        }
      }

      result = Schema.createRecord(writer.getFullName(), writer.getDoc(), "", writer.isError());
      result.setFields(fields);
      result.setReader(reader);

      seen.put(new SeenPair(reader, writer), result);
    }
    return result;
  }

  private static Object getDefaultObject(Field field)
      throws IOException {
    Schema defaultSchema = field.schema();
    JsonNode jsonNode = field.defaultValue();
    GenericDatumReader<Object> reader = new GenericDatumReader<Object>(defaultSchema);
    reader.setUsingSchemaResolution(true);
    JsonBasicDecoder jsonDecoder = DecoderFactory.get().jsonBasicDecoder(jsonNode.toString());
    System.out.println("DEF Schema " + defaultSchema);
    System.out.println("DEF JSON " + jsonNode.toString());
    Object defaultValue = null;
    try {
      defaultValue = reader.read(defaultValue, jsonDecoder);
    } catch (EOFException eofException) {
    }
    return defaultValue;
  }

  private static List<String> resolveEnum(List<String> wSymbols,
      List<String> rSymbols){
    Object[] adjustments = new Object[wSymbols.size()];
    for (int i = 0; i < adjustments.length; i++) {
      int j = rSymbols.indexOf(wSymbols.get(i));
      adjustments[i] = (j == -1 ? "No match for " + wSymbols.get(i)
                                : new Integer(j));
    }
    return wSymbols;
  }

  private static int bestBranch(Schema readerUnion, Schema writer) {
    Schema.Type writerType = writer.getType();
      // first scan for exact match
      int j = 0;
      for (Schema readerType : readerUnion.getTypes()) {
        if (writerType == readerType.getType())
          if (writerType == Schema.Type.RECORD || writerType == Schema.Type.ENUM || 
              writerType == Schema.Type.FIXED) {
            String writerName = writer.getFullName();
            String readerName = readerType.getFullName();
            if ((writerName != null && writerName.equals(readerName))
                || writerName == readerName && writerType == Schema.Type.RECORD)
              return j;
          } else
            return j;
        j++;
      }

      // then scan match via numeric promotion
      j = 0;
      for (Schema readerType : readerUnion.getTypes()) {
        switch (writerType) {
        case INT:
          switch (readerType.getType()) {
          case LONG: 
          case DOUBLE:
            return j;
          }
          break;
        case LONG:
        case FLOAT:
          switch (readerType.getType()) {
          case DOUBLE:
            return j;
          }
          break;
        }
        j++;
      }
      return -1;
  }
}

