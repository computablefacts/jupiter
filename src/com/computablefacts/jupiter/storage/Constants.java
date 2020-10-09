/*
 * Copyright (c) 2011-2020 MNCC
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
 * associated documentation files (the "Software"), to deal in the Software without restriction,
 * including without limitation the rights to use, copy, modify, merge, publish, distribute,
 * sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or
 * substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 * @author http://www.mncc.fr
 */
package com.computablefacts.jupiter.storage;

import java.util.Collections;
import java.util.Iterator;

import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

import com.computablefacts.nona.helpers.Codecs;
import com.computablefacts.nona.types.Span;
import com.computablefacts.nona.types.SpanSequence;

final public class Constants {

  public static final char SEPARATOR_NUL = '\0';
  public static final char SEPARATOR_PIPE = '|';
  public static final char SEPARATOR_CURRENCY_SIGN = '¤';
  public static final byte[] EMPTY_BYTES = new byte[0];
  public static final Text TEXT_EMPTY = new Text();
  public static final Text TEXT_CACHE = new Text("cache");
  public static final Value VALUE_EMPTY = new Value();
  public static final Value VALUE_ONE = new Value("1");
  public static final Value VALUE_ANONYMIZED = new Value("{\"is_anonymized\":\"true\"}");
  public static final ColumnVisibility VIZ_EMPTY = new ColumnVisibility();
  public static final Iterator ITERATOR_EMPTY = Collections.EMPTY_LIST.iterator();
  public static final Span SPAN_EMPTY = Codecs.SPAN_EMPTY;
  public static final SpanSequence SPAN_SEQUENCE_EMPTY = Codecs.SPAN_SEQUENCE_EMPTY;
  public static final String STRING_ADM = "ADM";
  public static final String STRING_RAW_DATA = "RAW_DATA";
  public static final Authorizations AUTH_ADM = new Authorizations(STRING_ADM);
}
