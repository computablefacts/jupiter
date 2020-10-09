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
package com.computablefacts.jupiter.queries;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Sets;

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

    assertEquals("john And doe", node2.toString());

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

    assertEquals("name:john And doe", node2.toString());

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

    assertEquals("~john And doe", node3.toString());

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

    assertEquals("name:~john And doe", node3.toString());

    TerminalNode node4 = (TerminalNode) QueryBuilder.build("name:~\"j?hn*d?e\"");

    Assert.assertEquals(TerminalNode.eTermForms.Literal, node4.form());
    assertEquals("name", node4.key());
    assertEquals("j?hn*d?e", node4.value());
    assertEquals("name:\"j?hn*d?e\"", node4.toString());
  }

  @Test
  public void testNotFixUp() {
    assertNull(QueryBuilder.build("NOT name:andré"));
    assertNull(QueryBuilder.build("- name:andré"));
  }

  @Test
  public void testNotOrFixUp() {
    assertEquals("name:andré", QueryBuilder.build("NOT username:andré OR name:andré").toString());
    assertEquals("name:andré", QueryBuilder.build("- username:andré OR name:andré").toString());
  }

  @Test
  public void testOrNotFixUp() {
    assertEquals("username:andré",
        QueryBuilder.build("username:andré OR NOT name:andré").toString());
    assertEquals("username:andré", QueryBuilder.build("username:andré OR - name:andré").toString());
  }

  @Test
  public void testNotAndFixUp() {
    assertEquals("name:andré And Not(username:andré)",
        QueryBuilder.build("NOT username:andré AND name:andré").toString());
    assertEquals("name:andré And Not(username:andré)",
        QueryBuilder.build("- username:andré AND name:andré").toString());
  }

  @Test
  public void testAndNotFixUp() {
    assertEquals("name:andré And Not(username:andré)",
        QueryBuilder.build("name:andré NOT username:andré").toString());
    assertEquals("name:andré And Not(username:andré)",
        QueryBuilder.build("name:andré - username:andré").toString());
  }

  @Test
  public void testAndImplicit() {
    assertEquals("firstname:john And lastname:doe",
        QueryBuilder.build("firstname:john lastname:doe").toString());
    assertEquals("firstname:\"j?hn\" And lastname:\"d?e\"",
        QueryBuilder.build("firstname:\"j?hn\" lastname:\"d?e\"").toString());
  }

  @Test
  public void testAndExplicit() {

    assertEquals("firstname:john And lastname:doe",
        QueryBuilder.build("firstname:john AND lastname:doe").toString());
    assertEquals("firstname:john And lastname:\"doe\"",
        QueryBuilder.build("firstname:john + lastname:doe").toString());

    assertEquals("firstname:\"j?hn\" And lastname:\"d?e\"",
        QueryBuilder.build("firstname:\"j?hn\" AND lastname:\"d?e\"").toString());
    assertEquals("firstname:\"j?hn\" And lastname:\"d?e\"",
        QueryBuilder.build("firstname:\"j?hn\" + lastname:\"d?e\"").toString());
  }

  @Test
  public void testOr() {
    assertEquals("firstname:john Or lastname:doe",
        QueryBuilder.build("firstname:john OR lastname:doe").toString());
    assertEquals("firstname:\"j?hn\" Or lastname:\"d?e\"",
        QueryBuilder.build("firstname:\"j?hn\" OR lastname:\"d?e\"").toString());
  }

  @Test
  public void testOrAnd() {
    assertEquals("username:jdoe Or (firstname:john And lastname:doe)",
        QueryBuilder.build("username:jdoe OR (firstname:john AND lastname:doe)").toString());
  }

  @Test
  public void testAndOr() {
    assertEquals("(firstname:john And lastname:doe) Or username:jdoe",
        QueryBuilder.build("(firstname:john AND lastname:doe) OR username:jdoe").toString());
  }

  @Test
  public void testOrAndTerms() {
    AbstractNode node = QueryBuilder.build("username:jdoe OR (firstname:john AND lastname:doe)");
    assertEquals(Sets.newHashSet("john", "doe", "jdoe"), node.terms());
  }

  @Test
  public void testAndOrTerms() {
    AbstractNode node = QueryBuilder.build("(firstname:john AND lastname:doe) OR username:jdoe");
    assertEquals(Sets.newHashSet("john", "doe", "jdoe"), node.terms());
  }
}
