package com.computablefacts.jupiter.storage.termstore;

import static com.computablefacts.jupiter.storage.Constants.TEXT_EMPTY;

import java.nio.charset.StandardCharsets;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Sets;

import nl.jqno.equalsverifier.EqualsVerifier;

public class FieldCountTest {

  @Test(expected = NullPointerException.class)
  public void testNullDataset() {
    FieldCount fc = new FieldCount(null, "field", Term.TYPE_UNKNOWN, Sets.newHashSet(), 0);
  }

  @Test(expected = NullPointerException.class)
  public void testNullField() {
    FieldCount fc = new FieldCount("dataset", null, Term.TYPE_UNKNOWN, Sets.newHashSet(), 0);
  }

  @Test(expected = NullPointerException.class)
  public void testNullLabels() {
    FieldCount fc = new FieldCount("dataset", "field", Term.TYPE_UNKNOWN, null, 0);
  }

  @Test
  public void testHashcodeAndEquals() {
    EqualsVerifier.forClass(FieldCount.class).verify();
  }

  @Test
  public void testNewMutation() {

    byte[] row = "first_name\u00001".getBytes(StandardCharsets.UTF_8);
    byte[] cf = "my_dataset_CNT".getBytes(StandardCharsets.UTF_8);
    byte[] cq = TEXT_EMPTY.getBytes();
    byte[] cv = new ColumnVisibility("ADM|MY_DATASET_CNT").getExpression();
    byte[] val = "11".getBytes(StandardCharsets.UTF_8);

    Mutation expected = new Mutation(row);
    expected.put(new Text(cf), new Text(cq), new ColumnVisibility(cv), new Value(val));

    Mutation actual = FieldCount.newMutation("my_dataset", "first_name", Term.TYPE_STRING, 11);

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testFromKeyValue() {

    byte[] row = "first_name\u00001".getBytes(StandardCharsets.UTF_8);
    byte[] cf = "my_dataset_CNT".getBytes(StandardCharsets.UTF_8);
    byte[] cq = TEXT_EMPTY.getBytes();
    byte[] cv = new ColumnVisibility("ADM|MY_DATASET_CNT").getExpression();
    byte[] val = "11".getBytes(StandardCharsets.UTF_8);

    Key key = new Key(row, cf, cq, cv);
    Value value = new Value(val);
    FieldCount fieldCount = FieldCount.fromKeyValue(key, value);

    Assert.assertTrue(fieldCount.isString());
    Assert.assertEquals("my_dataset", fieldCount.dataset());
    Assert.assertEquals("first_name", fieldCount.field());
    Assert.assertEquals(Sets.newHashSet("ADM", "MY_DATASET_CNT"), fieldCount.labels());
    Assert.assertEquals(11L, fieldCount.count());
  }
}
