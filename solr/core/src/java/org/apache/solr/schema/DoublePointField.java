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
import java.util.Map;

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.legacy.LegacyNumericType;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.DoubleDocValues;
import org.apache.lucene.queries.function.valuesource.DoubleFieldSource;
import org.apache.lucene.queries.function.valuesource.SortedSetFieldSource;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.mutable.MutableValue;
import org.apache.lucene.util.mutable.MutableValueDouble;
import org.apache.solr.search.QParser;
import org.apache.solr.uninverting.UninvertingReader.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DoublePointField extends PointField implements DoubleValueFieldType {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public DoublePointField() {
    super(Double.BYTES);
  }

  {
    type = PointTypes.DOUBLE;
  }

  @Override
  public Object toNativeType(Object val) {
    if (val == null) return null;
    if (val instanceof Number) return ((Number) val).doubleValue();
    if (val instanceof String) return Double.parseDouble((String) val);
    return super.toNativeType(val);
  }

  public Query getRangeQuery(QParser parser, SchemaField field, String min, String max, boolean minInclusive,
      boolean maxInclusive) {
    double actualMin, actualMax;
    if (min == null) {
      actualMin = Double.NEGATIVE_INFINITY;
    } else {
      actualMin = Double.parseDouble(min);
      if (!minInclusive) {
        actualMin = Math.nextUp(actualMin);
      }
    }
    if (max == null) {
      actualMax = Double.POSITIVE_INFINITY;
    } else {
      actualMax = Double.parseDouble(max);
      if (!maxInclusive) {
        actualMax = Math.nextDown(actualMax);
      }
    }
    return DoublePoint.newRangeQuery(field.getName(), actualMin, actualMax);
  }

  @Override
  protected ValueSource getSingleValueSource(SortedSetSelector.Type choice, SchemaField f) {
    return new SortedSetFieldSource(f.getName(), choice) {
      @Override
      public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
        SortedSetFieldSource thisAsSortedSetFieldSource = this; // needed for nested anon class ref

        SortedSetDocValues sortedSet = DocValues.getSortedSet(readerContext.reader(), field);
        SortedDocValues view = SortedSetSelector.wrap(sortedSet, selector);

        return new DoubleDocValues(thisAsSortedSetFieldSource) {
          private int lastDocID;

          private boolean setDoc(int docID) throws IOException {
            if (docID < lastDocID) {
              throw new IllegalArgumentException("docs out of order: lastDocID=" + lastDocID + " docID=" + docID);
            }
            if (docID > view.docID()) {
              return docID == view.advance(docID);
            } else {
              return docID == view.docID();
            }
          }
          
          @Override
          public double doubleVal(int doc) throws IOException {
            if (setDoc(doc)) {
              BytesRef bytes = view.binaryValue();
              assert bytes.length > 0;
              return DoublePoint.decodeDimension(bytes.bytes, bytes.offset);
            } else {
              return 0D;
            }
          }

          @Override
          public boolean exists(int doc) throws IOException {
            return setDoc(doc);
          }

          @Override
          public ValueFiller getValueFiller() {
            return new ValueFiller() {
              private final MutableValueDouble mval = new MutableValueDouble();
              
              @Override
              public MutableValue getValue() {
                return mval;
              }
              
              @Override
              public void fillValue(int doc) throws IOException {
                if (setDoc(doc)) {
                  BytesRef bytes = view.binaryValue();
                  mval.exists = true;
                  mval.value = DoublePoint.decodeDimension(bytes.bytes, bytes.offset);
                } else {
                  mval.exists = false;
                  mval.value = 0;
                }
              }
            };
          }
        };
      }
    };
  }

  @Override
  public Object toObject(SchemaField sf, BytesRef term) {
    return DoublePoint.decodeDimension(term.bytes, term.offset);
  }

  @Override
  protected Query getExactQuery(QParser parser, SchemaField field, String externalVal) {
    // TODO: better handling of string->int conversion
    return DoublePoint.newExactQuery(field.getName(), Double.parseDouble(externalVal));
  }

  @Override
  protected String indexedToReadable(BytesRef indexedForm) {
    return Double.toString(DoublePoint.decodeDimension(indexedForm.bytes, indexedForm.offset));
  }

  @Override
  public void readableToIndexed(CharSequence val, BytesRefBuilder result) {
    result.grow(Double.BYTES);
    result.setLength(Double.BYTES);
    DoublePoint.encodeDimension(Double.parseDouble(val.toString()), result.bytes(), 0);
  }
  
  @Override
  protected BytesRef storedToIndexedByteRef(IndexableField f) {
    BytesRef bytes = new BytesRef(new byte[Double.BYTES], 0, Double.BYTES);
    DoublePoint.encodeDimension(f.numericValue().doubleValue(), bytes.bytes, 0);
    return bytes;
  }
  

  @Override
  public SortField getSortField(SchemaField field, boolean top) {
    field.checkSortability();

    Object missingValue = null;
    boolean sortMissingLast = field.sortMissingLast();
    boolean sortMissingFirst = field.sortMissingFirst();

    if (sortMissingLast) {
      missingValue = top ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
    } else if (sortMissingFirst) {
      missingValue = top ? Double.POSITIVE_INFINITY : Double.NEGATIVE_INFINITY;
    }
    SortField sf = new SortField(field.getName(), SortField.Type.DOUBLE, top);
    sf.setMissingValue(missingValue);
    return sf;
  }

  @Override
  public Type getUninversionType(SchemaField sf) {
    if (sf.multiValued()) {
      return Type.SORTED_SET_DOUBLE;
    } else {
      return Type.DOUBLE_POINT;
    }
  }

  @Override
  public ValueSource getValueSource(SchemaField field, QParser qparser) {
    field.checkFieldCacheSource();
    return new DoubleFieldSource(field.getName());
  }

  @Override
  public LegacyNumericType getNumericType() {
    return LegacyNumericType.DOUBLE;
  }

  @Override
  public IndexableField createField(SchemaField field, Object value, float boost) {
    if (!isFieldUsed(field)) return null;

    if (boost != 1.0 && log.isTraceEnabled()) {
      log.trace("Can't use document/field boost for PointField. Field: " + field.getName() + ", boost: " + boost);
    }
    double doubleValue = (value instanceof Number) ? ((Number) value).doubleValue() : Double.parseDouble(value.toString());
    return new DoublePoint(field.getName(), doubleValue);
  }

  @Override
  protected StoredField getStoredField(SchemaField sf, Object value) {
    return new StoredField(sf.getName(), (Double) this.toNativeType(value));
  }
}
