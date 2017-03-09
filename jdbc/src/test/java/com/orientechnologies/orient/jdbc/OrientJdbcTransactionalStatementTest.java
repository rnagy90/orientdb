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
    assertTrue((statement1 instanceof OrientJdbcTransactionalStatement));
    assertTrue((statement2 instanceof OrientJdbcTransactionalStatement));
    assertTrue(statement1 == statement2);
  }

  @Test
  public void shouldCreateNewAfterCommit() throws Exception {
    conn.setAutoCommit(false);
    OrientJdbcTransactionalStatement statement = (OrientJdbcTransactionalStatement) conn.createStatement();
    conn.commit();
    OrientJdbcTransactionalStatement newStatement = (OrientJdbcTransactionalStatement) conn.createStatement();
    assertTrue(statement != newStatement && !statement.equals(newStatement));
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
    Statement statement = conn.createStatement();
    statement.execute("CREATE CLASS TestClass");
    statement.execute("CREATE CLASS TestClassEx");
    statement.execute("CREATE PROPERTY TestClassEx.name STRING (MANDATORY TRUE, NOTNULL TRUE)");

    conn.setAutoCommit(false);
    OrientJdbcTransactionalStatement trStatement = (OrientJdbcTransactionalStatement) conn.createStatement();
    trStatement.execute("INSERT INTO TestClass SET name = 'name1'");
    trStatement.execute("INSERT INTO TestClassEx SET name = 'name2'");
    trStatement.execute("INSERT INTO TestClassEx SET name = 'name3'");
    conn.commit();

    ODatabaseDocument database = conn.getDatabase();
    OSchema schema = database.getMetadata().getSchema();
    assertEquals(1, database.countClass("TestClass"));
    assertEquals(2, database.countClass("TestClassEx"));

    return;
  }

  @Test(expected = SQLException.class)
  public void shouldRollback() throws Exception {
    Statement statement = conn.createStatement();
    statement.execute("CREATE CLASS TestClass");
    conn.setAutoCommit(false);

    OrientJdbcTransactionalStatement trStatement = (OrientJdbcTransactionalStatement) conn.createStatement();
    trStatement.execute("INSERT INTO TestClass SET name = 'name1'");
    trStatement.execute("INSERT INTO TestClassEx SET name = 'name2'");
    try {
      conn.commit();
    } catch (SQLException ex) {
      ODatabaseDocument database = conn.getDatabase();
      assertEquals(0, database.countClass("TestClass"));
      throw ex;
    }
  }

  @Test(expected = SQLException.class)
  public void shouldDenyExecute() throws Exception {
    conn.setAutoCommit(false);

    OrientJdbcTransactionalStatement trStatement = (OrientJdbcTransactionalStatement) conn.createStatement();
    try {
      trStatement.execute("CREATE CLASS TestClass");
    } catch (SQLException ex) {
      ODatabaseDocument database = conn.getDatabase();
      assertFalse(database.getMetadata().getSchema().existsClass("TestClass"));
      throw ex;
    }
  }
}
