package com.orientechnologies.orient.jdbc;

import static org.assertj.core.api.Assertions.*;
import static org.junit.Assert.*;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.metadata.OMetadata;

public class OrientJdbcDatabaseMetaDataTest extends OrientJdbcBaseTest {

  private DatabaseMetaData metaData;

  @Before
  public void setup() throws SQLException {
    metaData = conn.getMetaData();

  }

  @Test
  public void verifyDriverAndDatabaseVersions() throws SQLException {

    assertEquals("memory:OrientJdbcDatabaseMetaDataTest", metaData.getURL());
    assertEquals("admin", metaData.getUserName());
    assertEquals("OrientDB", metaData.getDatabaseProductName());
    assertEquals(OConstants.ORIENT_VERSION, metaData.getDatabaseProductVersion());
    assertEquals(2, metaData.getDatabaseMajorVersion());
    assertEquals(2, metaData.getDatabaseMinorVersion());

    assertEquals("OrientDB JDBC Driver", metaData.getDriverName());
    assertEquals("OrientDB " + OConstants.getVersion() + " JDBC Driver", metaData.getDriverVersion());
    assertEquals(2, metaData.getDriverMajorVersion());
    assertEquals(2, metaData.getDriverMinorVersion());

  }

  @Test
  public void shouldRetrievePrimaryKeysMetadata() throws SQLException {

    ResultSet primaryKeys = metaData.getPrimaryKeys(null, null, "Item");
    assertTrue(primaryKeys.next());
    assertEquals("intKey", primaryKeys.getString(4));
    assertEquals("Item.intKey", primaryKeys.getString(6));
    assertEquals(1, primaryKeys.getInt(5));

    assertTrue(primaryKeys.next());
    assertEquals("stringKey", primaryKeys.getString("COLUMN_NAME"));
    assertEquals("Item.stringKey", primaryKeys.getString("PK_NAME"));
    assertEquals(1, primaryKeys.getInt("KEY_SEQ"));

  }

  @Test
  public void shouldRetrieveTableTypes() throws SQLException {

    ResultSet tableTypes = metaData.getTableTypes();
    assertTrue(tableTypes.next());
    assertEquals("TABLE", tableTypes.getString(1));
    assertTrue(tableTypes.next());
    assertEquals("SYSTEM TABLE", tableTypes.getString(1));

    assertFalse(tableTypes.next());

  }

  @Test
  public void shouldRetrieveKeywords() throws SQLException {

    final String keywordsStr = metaData.getSQLKeywords();
    assertNotNull(keywordsStr);
    assertThat(Arrays.asList(keywordsStr.toUpperCase().split(",\\s*"))).contains("TRAVERSE");
  }

  @Test
  public void shouldRetrieveUniqueIndexInfoForTable() throws Exception {

    ResultSet indexInfo = metaData
        .getIndexInfo("OrientJdbcDatabaseMetaDataTest", "OrientJdbcDatabaseMetaDataTest", "Item", true, false);

    indexInfo.next();

    assertThat(indexInfo.getString("INDEX_NAME")).isEqualTo("Item.intKey");
    assertThat(indexInfo.getBoolean("NON_UNIQUE")).isFalse();

    indexInfo.next();

    assertThat(indexInfo.getString("INDEX_NAME")).isEqualTo("Item.stringKey");
    assertThat(indexInfo.getBoolean("NON_UNIQUE")).isFalse();

  }

  @Test
  public void getFields() throws SQLException {
    ResultSet rs = conn.createStatement().executeQuery("select from OUser");

    ResultSetMetaData rsMetaData = rs.getMetaData();

    int cc = rsMetaData.getColumnCount();
    Set<String> colset = new HashSet<String>();
    List<Map<String, Object>> columns = new ArrayList<Map<String, Object>>(cc);
    for (int i = 1; i <= cc; i++) {
      String name = rsMetaData.getColumnLabel(i);
      //      if (colset.contains(name))
      //        continue;
      colset.add(name);
      Map<String, Object> field = new HashMap<String, Object>();
      field.put("name", name);

      try {
        String catalog = rsMetaData.getCatalogName(i);
        String schema = rsMetaData.getSchemaName(i);
        String table = rsMetaData.getTableName(i);
        ResultSet rsmc = conn.getMetaData().getColumns(catalog, schema, table, name);
        while (rsmc.next()) {
          field.put("description", rsmc.getString("REMARKS"));
          break;
        }
      } catch (SQLException se) {
        se.printStackTrace();
      }
      columns.add(field);
    }

    for (Map<String, Object> c : columns) {
      System.out.println(c);
    }
  }

  @Test
  public void shouldFetchAllTables() throws SQLException {
    ResultSet rs = metaData.getTables(null, null, null, null);
    int tableCount = sizeOf(rs);

    assertThat(tableCount).isEqualTo(16);

  }

  @Test
  public void shouldFillSchemaAndCatalogWithDatabaseName() throws SQLException {
    ResultSet rs = metaData.getTables(null, null, null, null);

    while (rs.next()) {
      assertThat(rs.getString("TABLE_SCHEM")).isEqualTo("OrientJdbcDatabaseMetaDataTest");
      assertThat(rs.getString("TABLE_CAT")).isEqualTo("OrientJdbcDatabaseMetaDataTest");
    }

  }

  @Test
  public void shouldGetAllTablesFilteredByAllTypes() throws SQLException {
    ResultSet rs = metaData.getTableTypes();
    List<String> tableTypes = new ArrayList<String>(2);
    while (rs.next()) {
      tableTypes.add(rs.getString(1));
    }
    rs = metaData.getTables(null, null, null, tableTypes.toArray(new String[2]));
    int tableCount = sizeOf(rs);
    assertThat(tableCount).isEqualTo(16);
  }

  @Test
  public void shouldGetSystemTables() throws SQLException {
    final String systemTableType = "SYSTEM TABLE";

    ResultSet rs = metaData.getTableTypes();
    List<String> tableTypes = new ArrayList<String>(2);
    while (rs.next()) {
      tableTypes.add(rs.getString(1));
    }
    assertTrue("The table types should contain's a 'SYSTEM TABLE' type", tableTypes.contains(systemTableType));

    rs = metaData.getTables(null, null, null, new String[]{systemTableType});
    List<String> systemClassNames = new ArrayList<String>();
    while (rs.next()) {
      systemClassNames.add(rs.getString(4).toLowerCase());
    }
    assertTrue("The result class names should be in the ", OrientJdbcDatabaseMetaData.SYSTEM_TABLE_TYPES.containsAll(systemClassNames));
  }

  @Test
  public void shouldGetNotSystemTables() throws SQLException {
    final String tableType = "TABLE";

    ResultSet rs = metaData.getTableTypes();
    List<String> tableTypes = new ArrayList<String>(2);
    while (rs.next()) {
      tableTypes.add(rs.getString(1));
    }
    assertTrue("The table types should contains a 'TABLE' type", tableTypes.contains(tableType));

    rs = metaData.getTables(null, null, null, new String[]{tableType});
    boolean containsSystemCluster = false;
    while (rs.next()) {
      if (OMetadata.SYSTEM_CLUSTER.contains(rs.getString(4).toLowerCase())) {
        containsSystemCluster = true;
        break;
      }
    }

    assertFalse("The result shouldn't contains a system table", containsSystemCluster);
  }

  @Test
  public void getNoTablesFilteredByEmptySetOfTypes() throws SQLException {
    final ResultSet rs = metaData.getTables(null, null, null, new String[0]);
    int tableCount = sizeOf(rs);

    assertThat(tableCount).isEqualTo(0);
  }

  @Test
  public void getSingleTable() throws SQLException {
    ResultSet rs = metaData.getTables(null, null, "ouser", null);

    assertThat(sizeOf(rs)).isEqualTo(1);
  }

  @Test
  public void shouldGetSingleColumnOfArticle() throws SQLException {
    ResultSet rs = metaData.getColumns(null, null, "Article", "uuid");

    assertThat(sizeOf(rs)).isEqualTo(1);
  }

  @Test
  public void shouldGetAllColumnsOfArticle() throws SQLException {
    ResultSet rs = metaData.getColumns(null, null, "Article", null);

    assertThat(sizeOf(rs)).isEqualTo(5);
  }

  @Test
  //FIXME this is not a test: what is the target?
  public void shouldGetAllFields() throws SQLException {
    final ResultSet rsmc = conn.getMetaData().getColumns(null, null, "OUser", null);
    Set<String> fieldNames = new HashSet<String>();
    while (rsmc.next()) {
      fieldNames.add(rsmc.getString("COLUMN_NAME"));
    }

    fieldNames.removeAll(Arrays.asList("name", "password", "roles", "status"));
  }

  private int sizeOf(ResultSet rs) throws SQLException {
    int tableCount = 0;

    while (rs.next()) {
      tableCount++;
    }
    return tableCount;
  }

}
