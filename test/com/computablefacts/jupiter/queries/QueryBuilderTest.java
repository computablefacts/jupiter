package com.computablefacts.jupiter.queries;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Assert;
import org.junit.Test;

import com.google.errorprone.annotations.Var;

public class QueryBuilderTest {

  @Test(expected = NullPointerException.class)
  public void testParseNull() {
    TerminalNode node = (TerminalNode) QueryBuilder.build(null);
  }

  @Test
  public void testSimpleInflectional() {

    TerminalNode node1 = (TerminalNode) QueryBuilder.build("andré");

    Assert.assertEquals(TerminalNode.eTermForms.Inflectional, node1.form());
    assertEquals("", node1.key());
    assertEquals("andré", node1.value());
    assertEquals("andré", node1.toString());

    InternalNode node2 = (InternalNode) QueryBuilder.build("john doe");

    assertEquals(InternalNode.eConjunctionTypes.And, node2.conjunction());

    Assert.assertEquals(TerminalNode.eTermForms.Inflectional,
        ((TerminalNode) node2.child1()).form());
    assertEquals("", ((TerminalNode) node2.child1()).key());
    assertEquals("john", ((TerminalNode) node2.child1()).value());

    Assert.assertEquals(TerminalNode.eTermForms.Inflectional,
        ((TerminalNode) node2.child2()).form());
    assertEquals("", ((TerminalNode) node2.child2()).key());
    assertEquals("doe", ((TerminalNode) node2.child2()).value());

    assertEquals("(john And doe)", node2.toString());

    TerminalNode node3 = (TerminalNode) QueryBuilder.build("john*doe");

    Assert.assertEquals(TerminalNode.eTermForms.Inflectional, node3.form());
    assertEquals("", node3.key());
    assertEquals("john*doe", node3.value());

    assertEquals("john*doe", node3.toString());

    TerminalNode node4 = (TerminalNode) QueryBuilder.build("john?doe");

    Assert.assertEquals(TerminalNode.eTermForms.Inflectional, node4.form());
    assertEquals("", node4.key());
    assertEquals("john?doe", node4.value());

    assertEquals("john?doe", node4.toString());
  }

  @Test
  public void testComplexInflectional() {

    TerminalNode node1 = (TerminalNode) QueryBuilder.build("name:andré");

    Assert.assertEquals(TerminalNode.eTermForms.Inflectional, node1.form());
    assertEquals("name", node1.key());
    assertEquals("andré", node1.value());
    assertEquals("name:andré", node1.toString());

    InternalNode node2 = (InternalNode) QueryBuilder.build("name:john doe");

    assertEquals(InternalNode.eConjunctionTypes.And, node2.conjunction());

    Assert.assertEquals(TerminalNode.eTermForms.Inflectional,
        ((TerminalNode) node2.child1()).form());
    assertEquals("name", ((TerminalNode) node2.child1()).key());
    assertEquals("john", ((TerminalNode) node2.child1()).value());

    Assert.assertEquals(TerminalNode.eTermForms.Inflectional,
        ((TerminalNode) node2.child2()).form());
    assertEquals("", ((TerminalNode) node2.child2()).key());
    assertEquals("doe", ((TerminalNode) node2.child2()).value());

    assertEquals("(name:john And doe)", node2.toString());

    TerminalNode node3 = (TerminalNode) QueryBuilder.build("name:john*doe");

    Assert.assertEquals(TerminalNode.eTermForms.Inflectional, node3.form());
    assertEquals("name", node3.key());
    assertEquals("john*doe", node3.value());

    assertEquals("name:john*doe", node3.toString());

    TerminalNode node4 = (TerminalNode) QueryBuilder.build("name:john?doe");

    Assert.assertEquals(TerminalNode.eTermForms.Inflectional, node4.form());
    assertEquals("name", node4.key());
    assertEquals("john?doe", node4.value());

    assertEquals("name:john?doe", node4.toString());
  }

  @Test
  public void testSimpleLiteral() {

    TerminalNode node1 = (TerminalNode) QueryBuilder.build("\"andré\"");

    Assert.assertEquals(TerminalNode.eTermForms.Literal, node1.form());
    assertEquals("", node1.key());
    assertEquals("andré", node1.value());
    assertEquals("\"andré\"", node1.toString());

    TerminalNode node2 = (TerminalNode) QueryBuilder.build("\"john doe\"");

    Assert.assertEquals(TerminalNode.eTermForms.Literal, node2.form());
    assertEquals("", node2.key());
    assertEquals("john doe", node2.value());
    assertEquals("\"john doe\"", node2.toString());

    TerminalNode node3 = (TerminalNode) QueryBuilder.build("\"j?hn*d?e\"");

    Assert.assertEquals(TerminalNode.eTermForms.Literal, node3.form());
    assertEquals("", node3.key());
    assertEquals("j?hn*d?e", node3.value());
    assertEquals("\"j?hn*d?e\"", node3.toString());
  }

  @Test
  public void testComplexLiteral() {

    TerminalNode node1 = (TerminalNode) QueryBuilder.build("name:\"andré\"");

    Assert.assertEquals(TerminalNode.eTermForms.Literal, node1.form());
    assertEquals("name", node1.key());
    assertEquals("andré", node1.value());
    assertEquals("name:\"andré\"", node1.toString());

    TerminalNode node2 = (TerminalNode) QueryBuilder.build("name:\"john doe\"");

    Assert.assertEquals(TerminalNode.eTermForms.Literal, node2.form());
    assertEquals("name", node2.key());
    assertEquals("john doe", node2.value());
    assertEquals("name:\"john doe\"", node2.toString());

    TerminalNode node3 = (TerminalNode) QueryBuilder.build("name:\"j?hn*d?e\"");

    Assert.assertEquals(TerminalNode.eTermForms.Literal, node3.form());
    assertEquals("name", node3.key());
    assertEquals("j?hn*d?e", node3.value());
    assertEquals("name:\"j?hn*d?e\"", node3.toString());
  }

  @Test
  public void testSimpleThesaurus() {

    TerminalNode node1 = (TerminalNode) QueryBuilder.build("~andré");

    Assert.assertEquals(TerminalNode.eTermForms.Thesaurus, node1.form());
    assertEquals("", node1.key());
    assertEquals("andré", node1.value());
    assertEquals("~andré", node1.toString());

    TerminalNode node2 = (TerminalNode) QueryBuilder.build("~\"andré\"");

    Assert.assertEquals(TerminalNode.eTermForms.Literal, node2.form());
    assertEquals("", node2.key());
    assertEquals("andré", node2.value());
    assertEquals("\"andré\"", node2.toString());

    InternalNode node3 = (InternalNode) QueryBuilder.build("~john doe");

    assertEquals(InternalNode.eConjunctionTypes.And, node3.conjunction());

    Assert.assertEquals(TerminalNode.eTermForms.Thesaurus, ((TerminalNode) node3.child1()).form());
    assertEquals("", ((TerminalNode) node3.child1()).key());
    assertEquals("john", ((TerminalNode) node3.child1()).value());

    Assert.assertEquals(TerminalNode.eTermForms.Inflectional,
        ((TerminalNode) node3.child2()).form());
    assertEquals("", ((TerminalNode) node3.child2()).key());
    assertEquals("doe", ((TerminalNode) node3.child2()).value());

    assertEquals("(~john And doe)", node3.toString());

    TerminalNode node4 = (TerminalNode) QueryBuilder.build("~\"j?hn*d?e\"");

    Assert.assertEquals(TerminalNode.eTermForms.Literal, node4.form());
    assertEquals("", node4.key());
    assertEquals("j?hn*d?e", node4.value());
    assertEquals("\"j?hn*d?e\"", node4.toString());
  }

  @Test
  public void testComplexThesaurus() {

    TerminalNode node1 = (TerminalNode) QueryBuilder.build("name:~andré");

    Assert.assertEquals(TerminalNode.eTermForms.Thesaurus, node1.form());
    assertEquals("name", node1.key());
    assertEquals("andré", node1.value());
    assertEquals("name:~andré", node1.toString());

    TerminalNode node2 = (TerminalNode) QueryBuilder.build("name:~\"andré\"");

    Assert.assertEquals(TerminalNode.eTermForms.Literal, node2.form());
    assertEquals("name", node2.key());
    assertEquals("andré", node2.value());
    assertEquals("name:\"andré\"", node2.toString());

    InternalNode node3 = (InternalNode) QueryBuilder.build("name:~john doe");

    assertEquals(InternalNode.eConjunctionTypes.And, node3.conjunction());

    Assert.assertEquals(TerminalNode.eTermForms.Thesaurus, ((TerminalNode) node3.child1()).form());
    assertEquals("name", ((TerminalNode) node3.child1()).key());
    assertEquals("john", ((TerminalNode) node3.child1()).value());

    Assert.assertEquals(TerminalNode.eTermForms.Inflectional,
        ((TerminalNode) node3.child2()).form());
    assertEquals("", ((TerminalNode) node3.child2()).key());
    assertEquals("doe", ((TerminalNode) node3.child2()).value());

    assertEquals("(name:~john And doe)", node3.toString());

    TerminalNode node4 = (TerminalNode) QueryBuilder.build("name:~\"j?hn*d?e\"");

    Assert.assertEquals(TerminalNode.eTermForms.Literal, node4.form());
    assertEquals("name", node4.key());
    assertEquals("j?hn*d?e", node4.value());
    assertEquals("name:\"j?hn*d?e\"", node4.toString());
  }

  @Test
  public void testNotAndNotFixUp() {
    assertNull(QueryBuilder.build("NOT username:andré AND NOT name:andré"));
    assertNull(
        QueryBuilder.build("NOT username:andré AND NOT (firstname:andré OR lastname:andré)"));
    assertNull(
        QueryBuilder.build("NOT username:andré AND NOT (firstname:andré AND lastname:andré)"));
  }

  @Test
  public void testNotOrNotFixUp() {
    assertNull(QueryBuilder.build("NOT username:andré OR NOT name:andré"));
    assertNull(QueryBuilder.build("NOT username:andré OR NOT (firstname:andré OR lastname:andré)"));
    assertNull(
        QueryBuilder.build("NOT username:andré OR NOT (firstname:andré AND lastname:andré)"));
  }

  @Test
  public void testNotFixUp() {
    assertNull(QueryBuilder.build("NOT name:andré"));
    assertNull(QueryBuilder.build("- name:andré"));
  }

  @Test
  public void testNotAndFixUp() {
    assertEquals("(name:andré And Not(username:andré))",
        QueryBuilder.build("NOT username:andré AND name:andré").toString());
    assertEquals("(name:andré And Not(username:andré))",
        QueryBuilder.build("- username:andré AND name:andré").toString());
  }

  @Test
  public void testAndNotFixUp() {
    assertEquals("(name:andré And Not(username:andré))",
        QueryBuilder.build("name:andré NOT username:andré").toString());
    assertEquals("(name:andré And Not(username:andré))",
        QueryBuilder.build("name:andré - username:andré").toString());
  }

  @Test
  public void testAndImplicit() {
    assertEquals("(firstname:john And lastname:doe)",
        QueryBuilder.build("firstname:john lastname:doe").toString());
    assertEquals("(firstname:\"j?hn\" And lastname:\"d?e\")",
        QueryBuilder.build("firstname:\"j?hn\" lastname:\"d?e\"").toString());
  }

  @Test
  public void testAndExplicit() {

    assertEquals("(firstname:john And lastname:doe)",
        QueryBuilder.build("firstname:john AND lastname:doe").toString());
    assertEquals("(firstname:john And lastname:\"doe\")",
        QueryBuilder.build("firstname:john + lastname:doe").toString());

    assertEquals("(firstname:\"j?hn\" And lastname:\"d?e\")",
        QueryBuilder.build("firstname:\"j?hn\" AND lastname:\"d?e\"").toString());
    assertEquals("(firstname:\"j?hn\" And lastname:\"d?e\")",
        QueryBuilder.build("firstname:\"j?hn\" + lastname:\"d?e\"").toString());
  }

  @Test
  public void testOr() {
    assertEquals("(firstname:john Or lastname:doe)",
        QueryBuilder.build("firstname:john OR lastname:doe").toString());
    assertEquals("(firstname:\"j?hn\" Or lastname:\"d?e\")",
        QueryBuilder.build("firstname:\"j?hn\" OR lastname:\"d?e\"").toString());
  }

  @Test
  public void testParseArrayPredicate() {

    TerminalNode node = (TerminalNode) QueryBuilder.build("Actors[0]¤children[0]:\"Suri\"");

    Assert.assertEquals("Actors[0]¤children[0]:\"Suri\"", node.toString());
    Assert.assertEquals(TerminalNode.eTermForms.Literal, node.form());
    Assert.assertEquals("Actors[0]¤children[0]", node.key());
    Assert.assertEquals("Suri", node.value());
  }

  @Test
  public void testParseArrayPredicateWithWildcard() {

    TerminalNode node = (TerminalNode) QueryBuilder.build("Actors[*]¤children[*]:\"Suri\"");

    Assert.assertEquals("Actors[*]¤children[*]:\"Suri\"", node.toString());
    Assert.assertEquals(TerminalNode.eTermForms.Literal, node.form());
    Assert.assertEquals("Actors[*]¤children[*]", node.key());
    Assert.assertEquals("Suri", node.value());
  }

  @Test
  public void testParseArrayPredicateWithoutValue() {

    AbstractNode node = QueryBuilder.build("children[0]");

    Assert.assertNull(node);
  }

  @Test
  public void testParseBooleanQueryNoNegationNoGroup() {

    @Var
    AbstractNode actual = QueryBuilder.build("A AND B AND C AND D");
    @Var
    String expected = "(((A And B) And C) And D)";

    Assert.assertEquals(expected, actual.toString());

    actual = QueryBuilder.build("A OR B OR C OR D");
    expected = "(((A Or B) Or C) Or D)";

    Assert.assertEquals(expected, actual.toString());

    actual = QueryBuilder.build("A OR B AND C OR D");
    expected = "(((A Or B) And C) Or D)";

    Assert.assertEquals(expected, actual.toString());

    actual = QueryBuilder.build("A AND B OR C AND D");
    expected = "(((A And B) Or C) And D)";

    Assert.assertEquals(expected, actual.toString());
  }

  @Test
  public void testParseBooleanQueryNoNegationOneSmallGroup() {

    @Var
    AbstractNode actual = QueryBuilder.build("A AND (B AND C) AND D");
    @Var
    String expected = "((A And (B And C)) And D)";

    Assert.assertEquals(expected, actual.toString());

    actual = QueryBuilder.build("A AND (B OR C) AND D");
    expected = "((A And (B Or C)) And D)";

    Assert.assertEquals(expected, actual.toString());

    actual = QueryBuilder.build("A OR (B AND C) OR D");
    expected = "((A Or (B And C)) Or D)";

    Assert.assertEquals(expected, actual.toString());

    actual = QueryBuilder.build("A OR (B OR C) OR D");
    expected = "((A Or (B Or C)) Or D)";

    Assert.assertEquals(expected, actual.toString());
  }

  @Test
  public void testParseBooleanQueryNoNegationOneLargeGroup() {

    @Var
    AbstractNode actual = QueryBuilder.build("A AND (B AND C AND D)");
    @Var
    String expected = "(A And ((B And C) And D))";

    Assert.assertEquals(expected, actual.toString());

    actual = QueryBuilder.build("A AND (B OR C AND D)");
    expected = "(A And ((B Or C) And D))";

    Assert.assertEquals(expected, actual.toString());

    actual = QueryBuilder.build("A OR (B AND C OR D)");
    expected = "(A Or ((B And C) Or D))";

    Assert.assertEquals(expected, actual.toString());

    actual = QueryBuilder.build("A OR (B OR C OR D)");
    expected = "(A Or ((B Or C) Or D))";

    Assert.assertEquals(expected, actual.toString());
  }

  @Test
  public void testParseBooleanQueryOneNegatedSmallGroup() {

    @Var
    AbstractNode actual = QueryBuilder.build("A AND NOT(B AND C) AND D");
    @Var
    String expected = "((A And Not(B And C)) And D)";

    Assert.assertEquals(expected, actual.toString());

    actual = QueryBuilder.build("A AND NOT(B OR C) AND D");
    expected = "((A And Not(B Or C)) And D)";

    Assert.assertEquals(expected, actual.toString());

    actual = QueryBuilder.build("A OR NOT(B AND C) OR D");
    expected = "((A Or Not(B And C)) Or D)";

    Assert.assertEquals(expected, actual.toString());

    actual = QueryBuilder.build("A OR NOT(B OR C) OR D");
    expected = "((A Or Not(B Or C)) Or D)";

    Assert.assertEquals(expected, actual.toString());
  }

  @Test
  public void testParseBooleanQueryOneNegatedLargeGroup() {

    @Var
    AbstractNode actual = QueryBuilder.build("A AND NOT(B AND C AND D)");
    @Var
    String expected = "(A And Not((B And C) And D))";

    Assert.assertEquals(expected, actual.toString());

    actual = QueryBuilder.build("A AND NOT(B OR C AND D)");
    expected = "(A And Not((B Or C) And D))";

    Assert.assertEquals(expected, actual.toString());

    actual = QueryBuilder.build("A OR NOT(B AND C OR D)");
    expected = "(A Or Not((B And C) Or D))";

    Assert.assertEquals(expected, actual.toString());

    actual = QueryBuilder.build("A OR NOT(B OR C OR D)");
    expected = "(A Or Not((B Or C) Or D))";

    Assert.assertEquals(expected, actual.toString());
  }

  @Test
  public void testParseBooleanQueryTwoSmallGroupsOneNegated() {

    @Var
    AbstractNode actual = QueryBuilder.build("NOT(A AND B) AND (C AND D)");
    @Var
    String expected = "((C And D) And Not(A And B))";

    Assert.assertEquals(expected, actual.toString());

    actual = QueryBuilder.build("NOT(A AND B) OR (C AND D)");
    expected = "((C And D) Or Not(A And B))";

    Assert.assertEquals(expected, actual.toString());

    actual = QueryBuilder.build("NOT(A OR B) AND (C OR D)");
    expected = "((C Or D) And Not(A Or B))";

    Assert.assertEquals(expected, actual.toString());

    actual = QueryBuilder.build("NOT(A OR B) OR (C OR D)");
    expected = "((C Or D) Or Not(A Or B))";

    Assert.assertEquals(expected, actual.toString());
  }

  @Test
  public void testParseBooleanQueryDoubleNegation() {

    @Var
    AbstractNode actual = QueryBuilder.build("A AND NOT(B AND NOT(C) AND D)");
    @Var
    String expected = "(A And ((C Or Not(B)) Or Not(D)))";

    Assert.assertEquals(expected, actual.toString());

    actual = QueryBuilder.build("A AND NOT(B OR NOT(C) AND D)");
    expected = "(A And ((C And Not(B)) Or Not(D)))";

    Assert.assertEquals(expected, actual.toString());

    actual = QueryBuilder.build("A OR NOT(B AND NOT(C) OR D)");
    expected = "(A Or ((C Or Not(B)) And Not(D)))";

    Assert.assertEquals(expected, actual.toString());

    actual = QueryBuilder.build("A OR NOT(B OR NOT(C) OR D)");
    expected = "(A Or ((C And Not(B)) And Not(D)))";

    Assert.assertEquals(expected, actual.toString());
  }

  @Test
  public void testParseBooleanQueryTripleNegation() {

    @Var
    AbstractNode actual = QueryBuilder.build("NOT(A AND NOT(B AND NOT(C) AND D))");
    @Var
    String expected = "(((B And Not(C)) And D) Or Not(A))";

    Assert.assertEquals(expected, actual.toString());

    actual = QueryBuilder.build("NOT(A AND NOT(B OR NOT(C) AND D))");
    expected = "(((B Or Not(C)) And D) Or Not(A))";

    Assert.assertEquals(expected, actual.toString());

    actual = QueryBuilder.build("NOT(A OR NOT(B AND NOT(C) OR D))");
    expected = "(((B And Not(C)) Or D) And Not(A))";

    Assert.assertEquals(expected, actual.toString());

    actual = QueryBuilder.build("NOT(A OR NOT(B OR NOT(C) OR D))");
    expected = "(((B Or Not(C)) Or D) And Not(A))";

    Assert.assertEquals(expected, actual.toString());
  }

  @Test
  public void testParseBooleanQueryDoubleNegation2() {

    AbstractNode actual = QueryBuilder.build("NOT(NOT A AND NOT B)");
    String expected = "(A Or B)";

    Assert.assertEquals(expected, actual.toString());
  }

  @Test
  public void testParseEmptyRangeWithPredicate() {

    AbstractNode actual = QueryBuilder.build("age:[]");

    Assert.assertNull(actual);
  }

  @Test
  public void testParseEmptyRangeWithoutPredicate() {

    AbstractNode actual = QueryBuilder.build("[]");

    Assert.assertNull(actual);
  }

  @Test
  public void testParseRangeWithPredicate() {

    @Var
    AbstractNode actual = QueryBuilder.build("age:[1 TO 10.5]");
    @Var
    String expected = "age:[1 TO 10.5]";

    Assert.assertEquals(expected, actual.toString());

    actual = QueryBuilder.build("age:[* TO 10.5]");
    expected = "age:[* TO 10.5]";

    Assert.assertEquals(expected, actual.toString());

    actual = QueryBuilder.build("age:[1 TO *]");
    expected = "age:[1 TO *]";

    Assert.assertEquals(expected, actual.toString());

    actual = QueryBuilder.build("age:[1 TO]");

    Assert.assertNull(actual);

    actual = QueryBuilder.build("age:[TO 10.5]");

    Assert.assertNull(actual);

    actual = QueryBuilder.build("age:[* TO *]");

    Assert.assertNull(actual);

    actual = QueryBuilder.build("age:[1]");

    Assert.assertNull(actual);
  }

  @Test
  public void testParseRangeWithoutPredicate() {

    @Var
    AbstractNode actual = QueryBuilder.build("[1 TO 10.5]");
    @Var
    String expected = "[1 TO 10.5]";

    Assert.assertEquals(expected, actual.toString());

    actual = QueryBuilder.build("[* TO 10.5]");
    expected = "[* TO 10.5]";

    Assert.assertEquals(expected, actual.toString());

    actual = QueryBuilder.build("[1 TO *]");
    expected = "[1 TO *]";

    Assert.assertEquals(expected, actual.toString());

    actual = QueryBuilder.build("[1 TO]");

    Assert.assertNull(actual);

    actual = QueryBuilder.build("[TO 10.5]");

    Assert.assertNull(actual);

    actual = QueryBuilder.build("[* TO *]");

    Assert.assertNull(actual);

    actual = QueryBuilder.build("[1]");

    Assert.assertNull(actual);
  }
}
