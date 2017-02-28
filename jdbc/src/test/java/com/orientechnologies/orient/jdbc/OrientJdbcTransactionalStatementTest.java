package com.orientechnologies.orient.jdbc;

import static org.junit.Assert.*;

import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.metadata.schema.OSchema;

public class OrientJdbcTransactionalStatementTest extends OrientJdbcBaseTest {

  @Test
  public void shouldCreateTransactionalStatement() throws Exception {
    conn.setAutoCommit(false);
    Statement statement = conn.createStatement();
    assertTrue(statement instanceof OrientJdbcTransactionalStatement);
  }

  @Test
  public void shouldGetSameTransactionalStatement() throws Exception {
    conn.setAutoCommit(false);
    Statement statement1 = conn.createStatement();
    Statement statement2 = conn.createStatement();
    assertTrue(statement1 == statement2);
  }

  @Test
  public void shouldHasCommitedStatusAfterCommit() throws Exception {
    conn.setAutoCommit(false);
    conn.createStatement();
    conn.commit();
    OrientJdbcTransactionalStatement statement = (OrientJdbcTransactionalStatement) conn.createStatement();
    assertTrue(statement.isCommited());
  }

  @Test
  public void shouldClearAndGetDefaultAfterAutoCommitChange() throws Exception {
    conn.setAutoCommit(false);
    conn.createStatement();
    conn.setAutoCommit(true);
    Statement statement = conn.createStatement();
    assertNull(conn.transactionalStatement);
    assertTrue((statement instanceof OrientJdbcStatement && !(statement instanceof OrientJdbcTransactionalStatement)));
  }

  @Test
  public void shouldExecuteAllCommands() throws Exception {
    conn.setAutoCommit(false);

    OrientJdbcTransactionalStatement statement = (OrientJdbcTransactionalStatement) conn.createStatement();
    statement.execute("CREATE CLASS TestClass");
    statement.execute("CREATE CLASS TestClassEx");
    statement.execute("CREATE PROPERTY TestClassEx.name STRING (MANDATORY TRUE, NOTNULL TRUE)");
    statement.execute("INSERT INTO TestClass SET name = 'name1'");
    statement.execute("INSERT INTO TestClassEx SET name = 'name2'");
    statement.execute("INSERT INTO TestClassEx SET name = 'name3'");
    conn.commit();

    ODatabaseDocument database = conn.getDatabase();
    OSchema schema = database.getMetadata().getSchema();
    assertTrue(schema.existsClass("TestClass"));
    assertTrue(schema.existsClass("TestClassEx"));
    assertEquals(1, database.countClass("TestClass"));
    assertEquals(2, database.countClass("TestClassEx"));

    return;
  }

  @Test
  public void shouldRollbackAllTransactionCapableCommands() throws Exception {
    conn.setAutoCommit(false);
    OrientJdbcTransactionalStatement statement = (OrientJdbcTransactionalStatement) conn.createStatement();
    statement.execute("CREATE CLASS TestClass");
    statement.execute("INSERT INTO TestClass SET name = 'name1'");
    statement.execute("CREATE CLASS TestClassEx EXTENDS TestClass");
    statement.execute("CREATE PROPERTY TestClassEx.randomLink LINK (MANDATORY TRUE, NOTNULL TRUE) ");
    statement.execute("INSERT INTO TestClassEx SET name = 'name2'");
    try {
      conn.commit();
      fail("The commit should cause an exception");
    } catch (SQLException ex) {
      ODatabaseDocument database = conn.getDatabase();
      OSchema schema = database.getMetadata().getSchema();
      assertTrue(schema.existsClass("TestClass"));
      assertTrue(schema.existsClass("TestClassEx"));
      assertEquals(0, database.countClass("TestClass"));
      assertEquals(0, database.countClass("TestClassEx"));
      return;
    }
  }
}
