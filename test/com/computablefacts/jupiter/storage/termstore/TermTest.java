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

public class TermTest {

  @Test(expected = NullPointerException.class)
  public void testNullDataset() {
    Term term = new Term(null, "bucketId", "field", Term.TYPE_STRING, "term", Sets.newHashSet(), 0);
  }

  @Test(expected = NullPointerException.class)
  public void testNullDocId() {
    Term term = new Term("dataset", null, "field", Term.TYPE_STRING, "term", Sets.newHashSet(), 0);
  }

  @Test(expected = NullPointerException.class)
  public void testNullField() {
    Term term =
        new Term("dataset", "bucketId", null, Term.TYPE_STRING, "term", Sets.newHashSet(), 0);
  }

  @Test(expected = NullPointerException.class)
  public void testNullTerm() {
    Term term =
        new Term("dataset", "bucketId", "field", Term.TYPE_UNKNOWN, null, Sets.newHashSet(), 0);
  }

  @Test(expected = NullPointerException.class)
  public void testNullLabels() {
    Term term = new Term("dataset", "bucketId", "field", Term.TYPE_STRING, "term", null, 0);
  }

  @Test
  public void testHashcodeAndEquals() {
    EqualsVerifier.forClass(Term.class).verify();
  }

  @Test
  public void testNewForwardMutation() {

    byte[] row = "john".getBytes(StandardCharsets.UTF_8);
    byte[] cf = "my_dataset_FIDX".getBytes(StandardCharsets.UTF_8);
    byte[] cq = "123456\0first_name\u00001".getBytes(StandardCharsets.UTF_8);
    byte[] cv = new ColumnVisibility().getExpression();
    byte[] val = "11".getBytes(StandardCharsets.UTF_8);

    Mutation expected = new Mutation(row);
    expected.put(new Text(cf), new Text(cq), new ColumnVisibility(cv), new Value(val));

    Mutation actual = Term.newForwardMutation("my_dataset", "123456", "first_name",
        Term.TYPE_STRING, "john", 11, Sets.newHashSet());

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testNewBackwardMutation() {

    byte[] row = "nhoj".getBytes(StandardCharsets.UTF_8);
    byte[] cf = "my_dataset_BIDX".getBytes(StandardCharsets.UTF_8);
    byte[] cq = "123456\0first_name\u00001".getBytes(StandardCharsets.UTF_8);
    byte[] cv = new ColumnVisibility().getExpression();
    byte[] val = "11".getBytes(StandardCharsets.UTF_8);

    Mutation expected = new Mutation(row);
    expected.put(new Text(cf), new Text(cq), new ColumnVisibility(cv), new Value(val));

    Mutation actual = Term.newBackwardMutation("my_dataset", "123456", "first_name",
        Term.TYPE_STRING, "john", 11, Sets.newHashSet());

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testFromForwardKeyValue() {

    byte[] row = "john".getBytes(StandardCharsets.UTF_8);
    byte[] cf = "my_dataset_FIDX".getBytes(StandardCharsets.UTF_8);
    byte[] cq = "123456\0first_name\u00001".getBytes(StandardCharsets.UTF_8);
    byte[] cv = new ColumnVisibility().getExpression();
    byte[] val = "11".getBytes(StandardCharsets.UTF_8);

    Key key = new Key(row, cf, cq, cv);
    Value value = new Value(val);
    Term term = Term.fromKeyValue(key, value);

    Assert.assertTrue(term.isString());
    Assert.assertEquals("my_dataset", term.dataset());
    Assert.assertEquals("first_name", term.field());
    Assert.assertEquals("john", term.term());
    Assert.assertEquals("123456", term.bucketId());
    Assert.assertEquals(Sets.newHashSet(), term.labels());
    Assert.assertEquals(11L, term.count());
  }

  @Test
  public void testFromBackwardKeyValue() {

    byte[] row = "nhoj".getBytes(StandardCharsets.UTF_8);
    byte[] cf = "my_dataset_BIDX".getBytes(StandardCharsets.UTF_8);
    byte[] cq = "123456\0first_name\u00001".getBytes(StandardCharsets.UTF_8);
    byte[] cv = new ColumnVisibility().getExpression();
    byte[] val = "11".getBytes(StandardCharsets.UTF_8);

    Key key = new Key(row, cf, cq, cv);
    Value value = new Value(val);
    Term term = Term.fromKeyValue(key, value);

    Assert.assertTrue(term.isString());
    Assert.assertEquals("my_dataset", term.dataset());
    Assert.assertEquals("first_name", term.field());
    Assert.assertEquals("123456", term.bucketId());
    Assert.assertEquals("john", term.term());
    Assert.assertEquals(Sets.newHashSet(), term.labels());
    Assert.assertEquals(11L, term.count());
  }
}
