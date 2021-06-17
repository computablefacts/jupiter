package com.computablefacts.jupiter.storage.termstore;

import static com.computablefacts.jupiter.storage.Constants.TEXT_EMPTY;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Sets;

import nl.jqno.equalsverifier.EqualsVerifier;

public class FieldLastUpdateTest {

  @Test(expected = NullPointerException.class)
  public void testNullDataset() {
    FieldLastUpdate fl = new FieldLastUpdate(null, "field", Term.TYPE_UNKNOWN, Sets.newHashSet(),
        DateTimeFormatter.ISO_INSTANT.format(Instant.now()));
  }

  @Test(expected = NullPointerException.class)
  public void testNullField() {
    FieldLastUpdate fl = new FieldLastUpdate("dataset", null, Term.TYPE_UNKNOWN, Sets.newHashSet(),
        DateTimeFormatter.ISO_INSTANT.format(Instant.now()));
  }

  @Test(expected = NullPointerException.class)
  public void testNullLabels() {
    FieldLastUpdate fl = new FieldLastUpdate("dataset", "field", Term.TYPE_UNKNOWN, null,
        DateTimeFormatter.ISO_INSTANT.format(Instant.now()));
  }

  @Test
  public void testHashcodeAndEquals() {
    EqualsVerifier.forClass(FieldLastUpdate.class).verify();
  }

  @Test
  public void testNewMutation() {

    Instant instant = Instant.now();

    byte[] row = "_\u0000first_name\u00001".getBytes(StandardCharsets.UTF_8);
    byte[] cf = "my_dataset_LU".getBytes(StandardCharsets.UTF_8);
    byte[] cq = TEXT_EMPTY.getBytes();
    byte[] cv = new ColumnVisibility("ADM|MY_DATASET_LU").getExpression();
    byte[] val = DateTimeFormatter.ISO_INSTANT.format(instant).getBytes(StandardCharsets.UTF_8);

    Mutation expected = new Mutation(row);
    expected.put(new Text(cf), new Text(cq), new ColumnVisibility(cv), new Value(val));

    String dataset = "my_dataset";
    Mutation actual = FieldLastUpdate.newMutation(dataset, "first_name", Term.TYPE_STRING, instant);

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testFromKeyValue() {

    Instant instant = Instant.now();

    byte[] row = "_\u0000first_name\u00001".getBytes(StandardCharsets.UTF_8);
    byte[] cf = "my_dataset_LU".getBytes(StandardCharsets.UTF_8);
    byte[] cq = TEXT_EMPTY.getBytes();
    byte[] cv = new ColumnVisibility("ADM|MY_DATASET_LU").getExpression();
    byte[] val = DateTimeFormatter.ISO_INSTANT.format(instant).getBytes(StandardCharsets.UTF_8);

    Key key = new Key(row, cf, cq, cv);
    Value value = new Value(val);
    FieldLastUpdate fieldLastUpdate = FieldLastUpdate.fromKeyValue(key, value);

    Assert.assertTrue(fieldLastUpdate.isString());
    Assert.assertEquals("my_dataset", fieldLastUpdate.dataset());
    Assert.assertEquals("first_name", fieldLastUpdate.field());
    Assert.assertEquals(Sets.newHashSet("ADM", "MY_DATASET_LU"), fieldLastUpdate.labels());
    Assert.assertEquals(DateTimeFormatter.ISO_INSTANT.format(instant),
        fieldLastUpdate.lastUpdate());
  }
}
