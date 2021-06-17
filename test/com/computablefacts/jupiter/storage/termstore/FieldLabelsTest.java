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

import com.beust.jcommander.internal.Lists;
import com.google.common.collect.Sets;

import nl.jqno.equalsverifier.EqualsVerifier;

public class FieldLabelsTest {

  @Test(expected = NullPointerException.class)
  public void testNullDataset() {
    FieldLabels fl =
        new FieldLabels(null, "field", Term.TYPE_UNKNOWN, Sets.newHashSet(), Sets.newHashSet());
  }

  @Test(expected = NullPointerException.class)
  public void testNullField() {
    FieldLabels fl =
        new FieldLabels("dataset", null, Term.TYPE_UNKNOWN, Sets.newHashSet(), Sets.newHashSet());
  }

  @Test(expected = NullPointerException.class)
  public void testNullLabelsAccumulo() {
    FieldLabels fl =
        new FieldLabels("dataset", "field", Term.TYPE_UNKNOWN, null, Sets.newHashSet());
  }

  @Test(expected = NullPointerException.class)
  public void testNullLabelsTerm() {
    FieldLabels fl =
        new FieldLabels("dataset", "field", Term.TYPE_UNKNOWN, Sets.newHashSet(), null);
  }

  @Test
  public void testHashcodeAndEquals() {
    EqualsVerifier.forClass(FieldLabels.class).verify();
  }

  @Test
  public void testNewMutation() {

    byte[] row = "_\u0000first_name\u00001".getBytes(StandardCharsets.UTF_8);
    byte[] cf = "my_dataset_VIZ".getBytes(StandardCharsets.UTF_8);
    byte[] cq = TEXT_EMPTY.getBytes();
    byte[] cv = new ColumnVisibility("ADM|MY_DATASET_VIZ").getExpression();
    byte[] val = "ADM\0MY_DATASET_FIRST_NAME".getBytes(StandardCharsets.UTF_8);

    Mutation expected = new Mutation(row);
    expected.put(new Text(cf), new Text(cq), new ColumnVisibility(cv), new Value(val));

    Mutation actual = FieldLabels.newMutation("my_dataset", "first_name", Term.TYPE_STRING,
        Sets.newLinkedHashSet(Lists.newArrayList("ADM", "MY_DATASET_FIRST_NAME")));

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testFromKeyValue() {

    byte[] row = "_\u0000first_name\u00001".getBytes(StandardCharsets.UTF_8);
    byte[] cf = "my_dataset_VIZ".getBytes(StandardCharsets.UTF_8);
    byte[] cq = TEXT_EMPTY.getBytes();
    byte[] cv = new ColumnVisibility("ADM|MY_DATASET_VIZ").getExpression();
    byte[] val = "ADM\0MY_DATASET_FIRST_NAME".getBytes(StandardCharsets.UTF_8);

    Key key = new Key(row, cf, cq, cv);
    Value value = new Value(val);
    FieldLabels fieldLabels = FieldLabels.fromKeyValue(key, value);

    Assert.assertTrue(fieldLabels.isString());
    Assert.assertEquals("my_dataset", fieldLabels.dataset());
    Assert.assertEquals("first_name", fieldLabels.field());
    Assert.assertEquals(Sets.newHashSet("ADM", "MY_DATASET_VIZ"), fieldLabels.labels());
    Assert.assertEquals(Sets.newHashSet("ADM", "MY_DATASET_FIRST_NAME"), fieldLabels.termLabels());
  }
}
