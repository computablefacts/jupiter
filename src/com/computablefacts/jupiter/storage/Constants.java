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
  public static final String STRING_RAW_FILE = "RAW_FILE";
  public static final Authorizations AUTH_ADM = new Authorizations(STRING_ADM);
  public static final int NB_QUERY_THREADS = 3;
}
