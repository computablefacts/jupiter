package com.computablefacts.jupiter.storage.termstore;

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

public class TermCountTest {

  @Test(expected = NullPointerException.class)
  public void testNullDataset() {
    TermCount tc = new TermCount(null, "field", Term.TYPE_STRING, "term", Sets.newHashSet(), 0);
  }

  @Test(expected = NullPointerException.class)
  public void testNullField() {
    TermCount tc = new TermCount("dataset", null, Term.TYPE_STRING, "term", Sets.newHashSet(), 0);
  }

  @Test(expected = NullPointerException.class)
  public void testNullTerm() {
    TermCount tc = new TermCount("dataset", "field", Term.TYPE_UNKNOWN, null, Sets.newHashSet(), 0);
  }

  @Test(expected = NullPointerException.class)
  public void testNullLabels() {
    TermCount tc = new TermCount("dataset", "field", Term.TYPE_STRING, "term", null, 0);
  }

  @Test
  public void testHashcodeAndEquals() {
    EqualsVerifier.forClass(TermCount.class).verify();
  }

  @Test
  public void testNewForwardMutation() {

    byte[] row = "john".getBytes(StandardCharsets.UTF_8);
    byte[] cf = "my_dataset_FCNT".getBytes(StandardCharsets.UTF_8);
    byte[] cq = "first_name\u00001".getBytes(StandardCharsets.UTF_8);
    byte[] cv = new ColumnVisibility().getExpression();
    byte[] val = "11".getBytes(StandardCharsets.UTF_8);

    Mutation expected = new Mutation(row);
    expected.put(new Text(cf), new Text(cq), new ColumnVisibility(cv), new Value(val));

    Mutation actual = TermCount.newForwardMutation("my_dataset", "first_name", Term.TYPE_STRING,
        "john", 11, Sets.newHashSet());

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testNewBackwardMutation() {

    byte[] row = "nhoj".getBytes(StandardCharsets.UTF_8);
    byte[] cf = "my_dataset_BCNT".getBytes(StandardCharsets.UTF_8);
    byte[] cq = "first_name\u00001".getBytes(StandardCharsets.UTF_8);
    byte[] cv = new ColumnVisibility().getExpression();
    byte[] val = "11".getBytes(StandardCharsets.UTF_8);

    Mutation expected = new Mutation(row);
    expected.put(new Text(cf), new Text(cq), new ColumnVisibility(cv), new Value(val));

    Mutation actual = TermCount.newBackwardMutation("my_dataset", "first_name", Term.TYPE_STRING,
        "john", 11, Sets.newHashSet());

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testFromForwardKeyValue() {

    byte[] row = "john".getBytes(StandardCharsets.UTF_8);
    byte[] cf = "my_dataset_FCNT".getBytes(StandardCharsets.UTF_8);
    byte[] cq = "first_name\u00001".getBytes(StandardCharsets.UTF_8);
    byte[] cv = new ColumnVisibility().getExpression();
    byte[] val = "11".getBytes(StandardCharsets.UTF_8);

    Key key = new Key(row, cf, cq, cv);
    Value value = new Value(val);
    TermCount termCount = TermCount.fromKeyValue(key, value);

    Assert.assertTrue(termCount.isString());
    Assert.assertEquals("my_dataset", termCount.dataset());
    Assert.assertEquals("first_name", termCount.field());
    Assert.assertEquals("john", termCount.term());
    Assert.assertEquals(Sets.newHashSet(), termCount.labels());
    Assert.assertEquals(11L, termCount.count());
  }

  @Test
  public void testFromBackwardKeyValue() {

    byte[] row = "nhoj".getBytes(StandardCharsets.UTF_8);
    byte[] cf = "my_dataset_BCNT".getBytes(StandardCharsets.UTF_8);
    byte[] cq = "first_name\u00001".getBytes(StandardCharsets.UTF_8);
    byte[] cv = new ColumnVisibility().getExpression();
    byte[] val = "11".getBytes(StandardCharsets.UTF_8);

    Key key = new Key(row, cf, cq, cv);
    Value value = new Value(val);
    TermCount termCount = TermCount.fromKeyValue(key, value);

    Assert.assertTrue(termCount.isString());
    Assert.assertEquals("my_dataset", termCount.dataset());
    Assert.assertEquals("first_name", termCount.field());
    Assert.assertEquals("john", termCount.term());
    Assert.assertEquals(Sets.newHashSet(), termCount.labels());
    Assert.assertEquals(11L, termCount.count());
  }
}
