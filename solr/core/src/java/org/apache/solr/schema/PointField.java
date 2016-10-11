/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.schema;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.solr.common.SolrException;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.search.QParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides field types to support for Lucene's {@link
 * org.apache.lucene.document.IntPoint}, {@link org.apache.lucene.document.LongPoint}, {@link org.apache.lucene.document.FloatPoint} and
 * {@link org.apache.lucene.document.DoublePoint}.
 * See {@link org.apache.lucene.search.PointRangeQuery} for more details.
 * It supports integer, float, long, double and date types.
 */
public abstract class PointField extends PrimitiveFieldType {
  
  private final int typeByteLength;

  public enum PointTypes {
    INTEGER,
    LONG,
    FLOAT,
    DOUBLE,
    DATE
  }
  
  protected PointTypes type;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  protected PointField(int typeByteLength) {
    this.typeByteLength = typeByteLength;
  }
  
  @Override
  public boolean isPointField() {
    return true;
  }
  
  @Override
  public final ValueSource getSingleValueSource(MultiValueSelector choice, SchemaField field, QParser parser) {
    // trivial base case
    if (!field.multiValued()) {
      // single value matches any selector
      return getValueSource(field, parser);
    }

    // Point fields don't support UninvertingReader. See SOLR-9202
    if (!field.hasDocValues()) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                              "docValues='true' is required to select '" + choice.toString() +
                              "' value from multivalued field ("+ field.getName() +") at query time");
    }
    
    // multivalued Point fields all use SortedSetDocValues, so we give a clean error if that's
    // not supported by the specified choice, else we delegate to a helper
    SortedSetSelector.Type selectorType = choice.getSortedSetSelectorType();
    if (null == selectorType) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                              choice.toString() + " is not a supported option for picking a single value"
                              + " from the multivalued field: " + field.getName() +
                              " (type: " + this.getTypeName() + ")");
    }
    
    return getSingleValueSource(selectorType, field);
  }

  /**
   * Helper method that will only be called for multivalued Point fields that have doc values.
   * Default impl throws an error indicating that selecting a single value from this multivalued 
   * field is not supported for this field type
   *
   * @param choice the selector Type to use, will never be null
   * @param field the field to use, garunteed to be multivalued.
   * @see #getSingleValueSource(MultiValueSelector,SchemaField,QParser) 
   */
  protected abstract ValueSource getSingleValueSource(SortedSetSelector.Type choice, SchemaField field);

  @Override
  public boolean isTokenized() {
    return false;
  }

  @Override
  public boolean multiValuedFieldCache() {
    return false;
  }

  /**
   * @return the type of this field
   */
  public PointTypes getType() {
    return type;
  }

//  @Override
//  public FieldType.LegacyNumericType getNumericType() {
//    switch (type) {
//      case INTEGER:
//        return FieldType.LegacyNumericType.INT;
//      case LONG:
//      case DATE:
//        return FieldType.LegacyNumericType.LONG;
//      case FLOAT:
//        return FieldType.LegacyNumericType.FLOAT;
//      case DOUBLE:
//        return FieldType.LegacyNumericType.DOUBLE;
//      default:
//        throw new AssertionError();
//    }
//  }
  
  @Override
  public Query getFieldQuery(QParser parser, SchemaField field, String externalVal) {
    if (!field.indexed() && field.hasDocValues()) {
      // currently implemented as singleton range
      return getRangeQuery(parser, field, externalVal, externalVal, true, true);
    } else {
      return getExactQuery(parser, field, externalVal);
    }
  }

  protected abstract Query getExactQuery(QParser parser, SchemaField field, String externalVal);

  @Override
  public String storedToReadable(IndexableField f) {
    return toExternal(f);
  }

  @Override
  public String toInternal(String val) {
    return toInternalByteRef(val).utf8ToString();
  }
  
  public BytesRef toInternalByteRef(String val) {
    final BytesRefBuilder bytes = new BytesRefBuilder();
    readableToIndexed(val, bytes);
    return bytes.get();
  }
  
  @Override
  public void write(TextResponseWriter writer, String name, IndexableField f) throws IOException {
    writer.writeVal(name, f.numericValue());
  }

  @Override
  public String storedToIndexed(IndexableField f) {
    return storedToIndexedByteRef(f).utf8ToString();
  }
  
  @Override
  public CharsRef indexedToReadable(BytesRef indexedForm, CharsRefBuilder charsRef) {
    final String value = indexedToReadable(indexedForm);
    charsRef.grow(value.length());
    charsRef.setLength(value.length());
    value.getChars(0, charsRef.length(), charsRef.chars(), 0);
    return charsRef.get();
  }
  
  @Override
  public String indexedToReadable(String indexedForm) {
    return indexedToReadable(new BytesRef(indexedForm));
  }
  
  protected abstract String indexedToReadable(BytesRef indexedForm);
  
  protected abstract BytesRef storedToIndexedByteRef(IndexableField f);

  protected boolean isFieldUsed(SchemaField field) {
    boolean indexed = field.indexed();
    boolean stored = field.stored();
    boolean docValues = field.hasDocValues();

    if (!indexed && !stored && !docValues) {
      if (log.isTraceEnabled()) {
        log.trace("Ignoring unindexed/unstored field: " + field);
      }
      return false;
    }
    return true;
  }

  @Override
  public List<IndexableField> createFields(SchemaField sf, Object value, float boost) {
    if (!(sf.hasDocValues() || sf.stored())) {
      return Collections.singletonList(createField(sf, value, boost));
    }
    List<IndexableField> fields = new ArrayList<>();
    final IndexableField field = createField(sf, value, boost);
    fields.add(field);
    
    if (sf.hasDocValues()) {
      if (sf.multiValued()) {
        fields.add(new SortedSetDocValuesField(sf.getName(), storedToIndexedByteRef(field)));
      } else {
        final long bits;
        if (field.numericValue() instanceof Integer || field.numericValue() instanceof Long) {
          bits = field.numericValue().longValue();
        } else if (field.numericValue() instanceof Float) {
          bits = Float.floatToIntBits(field.numericValue().floatValue());
        } else {
          assert field.numericValue() instanceof Double;
          bits = Double.doubleToLongBits(field.numericValue().doubleValue());
        }
        fields.add(new NumericDocValuesField(sf.getName(), bits));
      }
    }
    if (sf.stored()) {
      fields.add(getStoredField(sf, value));
    }
    return fields;
  }

  protected abstract StoredField getStoredField(SchemaField sf, Object value);

  @Override
  public void checkSchemaField(final SchemaField field) {
    // PointFields support DocValues
  }
}
