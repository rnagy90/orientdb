/**
 * Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * For more information: http://www.orientechnologies.com
 */
package com.orientechnologies.orient.jdbc;

import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.metadata.function.OFunctionLibrary;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.sequence.OSequence;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.sql.*;
import java.util.*;

import static com.orientechnologies.orient.core.OConstants.ORIENT_VERSION_MAJOR;
import static com.orientechnologies.orient.core.OConstants.ORIENT_VERSION_MINOR;

/**
 * @author Roberto Franchini (CELI srl - franchini--at--celi.it)
 * @author Salvatore Piccione (TXT e-solutions SpA - salvo.picci@gmail.com)
 * @author Luca Garulli (Orient Technologies - l.garulli--at--orientechnologies.com)
 */
public class OrientJdbcDatabaseMetaData implements DatabaseMetaData {
  protected final static List<String> TABLE_TYPES = Arrays.asList("TABLE", "SYSTEM TABLE");
  private final OrientJdbcConnection connection;
  private final ODatabaseDocument    database;
  public static final Set<String> SYSTEM_TABLE_TYPES;
  
  static {
    Set<String> clusters = new HashSet<String>(OMetadata.SYSTEM_CLUSTER);
    clusters.addAll(Arrays.asList(OSequence.CLASS_NAME.toLowerCase(), "V".toLowerCase(), "E".toLowerCase(), "_studio"));
    SYSTEM_TABLE_TYPES = Collections.unmodifiableSet(clusters);
  }

  public OrientJdbcDatabaseMetaData(OrientJdbcConnection iConnection, ODatabaseDocument iDatabase) {
    connection = iConnection;
    database = iDatabase;
    
  }

  public boolean allProceduresAreCallable() throws SQLException {
    return true;
  }

  public boolean allTablesAreSelectable() throws SQLException {
    return true;
  }

  public String getURL() throws SQLException {
    database.activateOnCurrentThread();
    return database.getURL();
  }

  public String getUserName() throws SQLException {
    database.activateOnCurrentThread();
    return database.getUser().getName();
  }

  public boolean isReadOnly() throws SQLException {

    return false;
  }

  public boolean nullsAreSortedHigh() throws SQLException {

    return false;
  }

  public boolean nullsAreSortedLow() throws SQLException {

    return false;
  }

  public boolean nullsAreSortedAtStart() throws SQLException {

    return false;
  }

  public boolean nullsAreSortedAtEnd() throws SQLException {

    return false;
  }

  public String getDatabaseProductName() throws SQLException {
    return "OrientDB";
  }

  public String getDatabaseProductVersion() throws SQLException {
    return OConstants.getVersion();
  }

  public String getDriverName() throws SQLException {
    return "OrientDB JDBC Driver";
  }

  public String getDriverVersion() throws SQLException {
    return OrientJdbcDriver.getVersion();
  }

  public int getDriverMajorVersion() {
    return ORIENT_VERSION_MAJOR;
  }

  public int getDriverMinorVersion() {
    return ORIENT_VERSION_MINOR;
  }

  public boolean usesLocalFiles() throws SQLException {

    return false;
  }

  public boolean usesLocalFilePerTable() throws SQLException {

    return false;
  }

  public boolean supportsMixedCaseIdentifiers() throws SQLException {

    return false;
  }

  public boolean storesUpperCaseIdentifiers() throws SQLException {

    return false;
  }

  public boolean storesLowerCaseIdentifiers() throws SQLException {

    return false;
  }

  public boolean storesMixedCaseIdentifiers() throws SQLException {

    return false;
  }

  public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {

    return false;
  }

  public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {

    return false;
  }

  public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {

    return false;
  }

  public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {

    return false;
  }

  public String getIdentifierQuoteString() throws SQLException {
    return " ";
  }

  public String getSQLKeywords() throws SQLException {

    return "@rid,@class,@version,@size,@type,@this,CONTAINS,CONTAINSALL,CONTAINSKEY,"
        + "CONTAINSVALUE,CONTAINSTEXT,MATCHES,TRAVERSE";
  }

  public String getNumericFunctions() throws SQLException {

    return null;
  }

  public String getStringFunctions() throws SQLException {

    return "";
  }

  public String getSystemFunctions() throws SQLException {

    return "";
  }

  public String getTimeDateFunctions() throws SQLException {
    return "date,sysdate";
  }

  public String getSearchStringEscape() throws SQLException {

    return null;
  }

  public String getExtraNameCharacters() throws SQLException {
    return null;
  }

  public boolean supportsAlterTableWithAddColumn() throws SQLException {

    return false;
  }

  public boolean supportsAlterTableWithDropColumn() throws SQLException {

    return false;
  }

  public boolean supportsColumnAliasing() throws SQLException {

    return false;
  }

  public boolean nullPlusNonNullIsNull() throws SQLException {

    return false;
  }

  public boolean supportsConvert() throws SQLException {

    return false;
  }

  public boolean supportsConvert(int fromType, int toType) throws SQLException {

    return false;
  }

  public boolean supportsTableCorrelationNames() throws SQLException {

    return false;
  }

  public boolean supportsDifferentTableCorrelationNames() throws SQLException {

    return false;
  }

  public boolean supportsExpressionsInOrderBy() throws SQLException {

    return false;
  }

  public boolean supportsOrderByUnrelated() throws SQLException {

    return false;
  }

  public boolean supportsGroupBy() throws SQLException {

    return true;
  }

  public boolean supportsGroupByUnrelated() throws SQLException {

    return false;
  }

  public boolean supportsGroupByBeyondSelect() throws SQLException {

    return false;
  }

  public boolean supportsLikeEscapeClause() throws SQLException {

    return false;
  }

  public boolean supportsMultipleResultSets() throws SQLException {

    return false;
  }

  public boolean supportsMultipleTransactions() throws SQLException {

    return true;
  }

  public boolean supportsNonNullableColumns() throws SQLException {

    return true;
  }

  public boolean supportsMinimumSQLGrammar() throws SQLException {

    return false;
  }

  public boolean supportsCoreSQLGrammar() throws SQLException {

    return false;
  }

  public boolean supportsExtendedSQLGrammar() throws SQLException {

    return false;
  }

  public boolean supportsANSI92EntryLevelSQL() throws SQLException {

    return false;
  }

  public boolean supportsANSI92IntermediateSQL() throws SQLException {

    return false;
  }

  public boolean supportsANSI92FullSQL() throws SQLException {

    return false;
  }

  public boolean supportsIntegrityEnhancementFacility() throws SQLException {

    return false;
  }

  public boolean supportsOuterJoins() throws SQLException {

    return false;
  }

  public boolean supportsFullOuterJoins() throws SQLException {

    return false;
  }

  public boolean supportsLimitedOuterJoins() throws SQLException {

    return false;
  }

  public String getSchemaTerm() throws SQLException {

    return null;
  }

  public String getProcedureTerm() throws SQLException {
    return "Function";
  }

  public String getCatalogTerm() throws SQLException {

    return null;
  }

  public boolean isCatalogAtStart() throws SQLException {

    return false;
  }

  public String getCatalogSeparator() throws SQLException {

    return null;
  }

  public boolean supportsSchemasInDataManipulation() throws SQLException {

    return false;
  }

  public boolean supportsSchemasInProcedureCalls() throws SQLException {

    return false;
  }

  public boolean supportsSchemasInTableDefinitions() throws SQLException {

    return false;
  }

  public boolean supportsSchemasInIndexDefinitions() throws SQLException {

    return false;
  }

  public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {

    return false;
  }

  public boolean supportsCatalogsInDataManipulation() throws SQLException {

    return false;
  }

  public boolean supportsCatalogsInProcedureCalls() throws SQLException {

    return false;
  }

  public boolean supportsCatalogsInTableDefinitions() throws SQLException {

    return false;
  }

  public boolean supportsCatalogsInIndexDefinitions() throws SQLException {

    return false;
  }

  public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {

    return false;
  }

  public boolean supportsPositionedDelete() throws SQLException {

    return false;
  }

  public boolean supportsPositionedUpdate() throws SQLException {

    return false;
  }

  public boolean supportsSelectForUpdate() throws SQLException {

    return false;
  }

  public boolean supportsStoredProcedures() throws SQLException {

    return true;
  }

  public boolean supportsSubqueriesInComparisons() throws SQLException {

    return false;
  }

  public boolean supportsSubqueriesInExists() throws SQLException {

    return false;
  }

  public boolean supportsSubqueriesInIns() throws SQLException {

    return true;
  }

  public boolean supportsSubqueriesInQuantifieds() throws SQLException {

    return false;
  }

  public boolean supportsCorrelatedSubqueries() throws SQLException {

    return false;
  }

  public boolean supportsUnion() throws SQLException {

    return true;
  }

  public boolean supportsUnionAll() throws SQLException {

    return false;
  }

  public boolean supportsOpenCursorsAcrossCommit() throws SQLException {

    return false;
  }

  public boolean supportsOpenCursorsAcrossRollback() throws SQLException {

    return false;
  }

  public boolean supportsOpenStatementsAcrossCommit() throws SQLException {

    return false;
  }

  public boolean supportsOpenStatementsAcrossRollback() throws SQLException {

    return false;
  }

  public int getMaxBinaryLiteralLength() throws SQLException {

    return 0;
  }

  public int getMaxCharLiteralLength() throws SQLException {

    return 0;
  }

  public int getMaxColumnNameLength() throws SQLException {

    return 0;
  }

  public int getMaxColumnsInGroupBy() throws SQLException {

    return 0;
  }

  public int getMaxColumnsInIndex() throws SQLException {

    return 0;
  }

  public int getMaxColumnsInOrderBy() throws SQLException {

    return 0;
  }

  public int getMaxColumnsInSelect() throws SQLException {

    return 0;
  }

  public int getMaxColumnsInTable() throws SQLException {

    return 0;
  }

  public int getMaxConnections() throws SQLException {

    return 0;
  }

  public int getMaxCursorNameLength() throws SQLException {

    return 0;
  }

  public int getMaxIndexLength() throws SQLException {

    return 0;
  }

  public int getMaxSchemaNameLength() throws SQLException {
    return 0;
  }

  public int getMaxProcedureNameLength() throws SQLException {

    return 0;
  }

  public int getMaxCatalogNameLength() throws SQLException {

    return 0;
  }

  public int getMaxRowSize() throws SQLException {
    return 0;
  }

  public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {

    return false;
  }

  public int getMaxStatementLength() throws SQLException {
    return 0;
  }

  public int getMaxStatements() throws SQLException {
    return 0;
  }

  public int getMaxTableNameLength() throws SQLException {
    return 1024;
  }

  public int getMaxTablesInSelect() throws SQLException {
    return 1;
  }

  public int getMaxUserNameLength() throws SQLException {

    return 0;
  }

  public int getDefaultTransactionIsolation() throws SQLException {
    return java.sql.Connection.TRANSACTION_NONE;
  }

  public boolean supportsTransactions() throws SQLException {

    return true;
  }

  public boolean supportsTransactionIsolationLevel(int level) throws SQLException {

    return false;
  }

  public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {

    return false;
  }

  public boolean supportsDataManipulationTransactionsOnly() throws SQLException {

    return false;
  }

  public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
    return false;
  }

  public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
    return true;
  }

  public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
    database.activateOnCurrentThread();
    final List<ODocument> records = new ArrayList<ODocument>();

    OFunctionLibrary functionLibrary = database.getMetadata().getFunctionLibrary();

    for (String functionName : functionLibrary.getFunctionNames()) {

      if (OrientJdbcUtils.like(functionName, procedureNamePattern)) {
        final ODocument doc = new ODocument()
            .field("PROCEDURE_CAT", (Object) null)
            .field("PROCEDURE_SCHEM", (Object) null)
            .field("PROCEDURE_NAME", functionName)
            .field("REMARKS", "")
            .field("PROCEDURE_TYPE", procedureResultUnknown)
            .field("SPECIFIC_NAME", functionName);

        records.add(doc);
      }
    }

    return new OrientJdbcResultSet(new OrientJdbcStatement(connection), records, ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
  }

  public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern)
      throws SQLException {
    database.activateOnCurrentThread();
    final List<ODocument> records = new ArrayList<ODocument>();

    OFunctionLibrary functionLibrary = database.getMetadata().getFunctionLibrary();

    for (String functionName : functionLibrary.getFunctionNames()) {

      if (OrientJdbcUtils.like(functionName, procedureNamePattern)) {

        final OFunction f = functionLibrary.getFunction(procedureNamePattern);

        for (String p : f.getParameters()) {
          final ODocument doc = new ODocument()
              .field("PROCEDURE_CAT", database.getName())
              .field("PROCEDURE_SCHEM", database.getName())
              .field("PROCEDURE_NAME", f.getName())
              .field("COLUMN_NAME", p)
              .field("COLUMN_TYPE", procedureColumnIn)
              .field("DATA_TYPE", java.sql.Types.OTHER)
              .field("SPECIFIC_NAME", f.getName());

          records.add(doc);
        }

        final ODocument doc = new ODocument()
            .field("PROCEDURE_CAT", database.getName())
            .field("PROCEDURE_SCHEM", database.getName())
            .field("PROCEDURE_NAME", f.getName())
            .field("COLUMN_NAME", "return")
            .field("COLUMN_TYPE", procedureColumnReturn)
            .field("DATA_TYPE", java.sql.Types.OTHER)
            .field("SPECIFIC_NAME", f.getName());

        records.add(doc);
      }
    }

    return new OrientJdbcResultSet(new OrientJdbcStatement(connection), records, ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
  }

  @Override
  public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
    database.activateOnCurrentThread();
    final Collection<OClass> classes = database.getMetadata().getSchema().getClasses();
    final List<ODocument> records = new ArrayList<ODocument>();

    final List tableTypes = types != null ? Arrays.asList(types) : TABLE_TYPES;
    for (OClass cls : classes) {
      final String className = cls.getName();
      final String type;

      if (SYSTEM_TABLE_TYPES.contains(cls.getName().toLowerCase()))
        type = "SYSTEM TABLE";
      else
        type = "TABLE";

      if (tableTypes.contains(type) &&
          (tableNamePattern == null ||
              tableNamePattern.equals("%") ||
              tableNamePattern.equalsIgnoreCase(className))) {

        final ODocument doc = new ODocument()
            .field("TABLE_CAT", database.getName())
            .field("TABLE_SCHEM", database.getName())
            .field("TABLE_NAME", className)
            .field("TABLE_TYPE", type)
            .field("REMARKS", (Object) null)
            .field("TYPE_NAME", (Object) null)
            .field("REF_GENERATION", (Object) null);
        records.add(doc);
      }
    }

    return new OrientJdbcResultSet(new OrientJdbcStatement(connection), records, ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
  }

  @Override
  public ResultSet getSchemas() throws SQLException {
    database.activateOnCurrentThread();
    final List<ODocument> records = new ArrayList<ODocument>();
    records.add(new ODocument()
        .field("TABLE_SCHEM", database.getName())
        .field("TABLE_CATALOG", database.getName()));

    return new OrientJdbcResultSet(new OrientJdbcStatement(connection), records, ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
  }

  public ResultSet getCatalogs() throws SQLException {
    database.activateOnCurrentThread();
    final List<ODocument> records = new ArrayList<ODocument>();
    records.add(new ODocument().field("TABLE_CAT", database.getName()));

    return new OrientJdbcResultSet(new OrientJdbcStatement(connection), records, ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
  }

  public ResultSet getTableTypes() throws SQLException {
    database.activateOnCurrentThread();

    List<ODocument> records = new ArrayList<ODocument>();
    for (String tableType : TABLE_TYPES) {
      records.add(new ODocument().field("TABLE_TYPE", tableType));
    }

    return new OrientJdbcResultSet(new OrientJdbcStatement(connection), records, ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY,
        ResultSet.HOLD_CURSORS_OVER_COMMIT);
  }

  @Override
  public ResultSet getColumns(final String catalog, final String schemaPattern, final String tableNamePattern,
      final String columnNamePattern) throws SQLException {
    database.activateOnCurrentThread();

    final List<ODocument> records = new ArrayList<ODocument>();

    OSchema schema = database.getMetadata().getSchema();

    for (OClass clazz : schema.getClasses()) {
      if (OrientJdbcUtils.like(clazz.getName(), tableNamePattern)) {
        for (OProperty prop : clazz.properties()) {
          if (columnNamePattern == null) {
            records.add(getPropertyAsDocument(clazz, prop));
          } else {
            if (OrientJdbcUtils.like(prop.getName(), columnNamePattern)) {
              records.add(getPropertyAsDocument(clazz, prop));
            }
          }
        }

      }
    }
    return new OrientJdbcResultSet(new OrientJdbcStatement(connection), records, ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
  }

  public ResultSet getColumnPrivileges(final String catalog, final String schema, final String table,
      final String columnNamePattern)
      throws SQLException {
    return null;
  }

  public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {

    return null;
  }

  public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable)
      throws SQLException {

    return null;
  }

  public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {

    return null;
  }

  @Override
  public ResultSet getPrimaryKeys(final String catalog, final String schema, final String table) throws SQLException {
    database.activateOnCurrentThread();
    final Set<OIndex<?>> classIndexes = database.getMetadata().getIndexManager().getClassIndexes(table);

    final Set<OIndex<?>> uniqueIndexes = new HashSet<OIndex<?>>();

    for (OIndex<?> oIndex : classIndexes) {
      if (oIndex.getType().equals(INDEX_TYPE.UNIQUE.name()))
        uniqueIndexes.add(oIndex);
    }

    final List<ODocument> records = new ArrayList<ODocument>();

    for (OIndex<?> unique : uniqueIndexes) {
      int keyFiledSeq = 1;
      for (String keyFieldName : unique.getDefinition().getFields()) {
        ODocument doc = new ODocument()
            .field("TABLE_CAT", catalog)
            .field("TABLE_SCHEM", catalog)
            .field("TABLE_NAME", table)
            .field("COLUMN_NAME", keyFieldName)
            .field("KEY_SEQ", Integer.valueOf(keyFiledSeq), OType.INTEGER)
            .field("PK_NAME", unique.getName());
        keyFiledSeq++;

        records.add(doc);
      }
    }

    return new OrientJdbcResultSet(new OrientJdbcStatement(connection), records, ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
  }

  public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
    return null;
  }

  public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
    return null;
  }

  public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog,
      String foreignSchema, String foreignTable) throws SQLException {

    return null;
  }

  public ResultSet getTypeInfo() throws SQLException {
    final List<ODocument> info = new ArrayList<ODocument>();

    info.add(new ODocument()
        .field("TYPE_NAME", OType.BINARY.toString())
        .field("DATA_TYPE", Types.BINARY)
        .field("NULLABLE", DatabaseMetaData.typeNullable)
        .field("CASE_SENSITIVE", true)
        .field("SEARCHABLE", true));
    info.add(new ODocument()
        .field("TYPE_NAME", OType.BOOLEAN.toString())
        .field("DATA_TYPE", Types.BOOLEAN)
        .field("NULLABLE", DatabaseMetaData.typeNullable)
        .field("CASE_SENSITIVE", true)
        .field("SEARCHABLE", true));
    info.add(new ODocument()
        .field("TYPE_NAME", OType.BYTE.toString())
        .field("DATA_TYPE", Types.TINYINT)
        .field("NULLABLE", DatabaseMetaData.typeNullable)
        .field("CASE_SENSITIVE", true)
        .field("UNSIGNED_ATTRIBUTE", true)
        .field("SEARCHABLE", true));
    info.add(new ODocument()
        .field("TYPE_NAME", OType.DATE.toString())
        .field("DATA_TYPE", Types.DATE)
        .field("NULLABLE", DatabaseMetaData.typeNullable)
        .field("CASE_SENSITIVE", true)
        .field("SEARCHABLE", true));
    info.add(new ODocument()
        .field("TYPE_NAME", OType.DATETIME.toString())
        .field("DATA_TYPE", Types.DATE)
        .field("NULLABLE", DatabaseMetaData.typeNullable)
        .field("CASE_SENSITIVE", true)
        .field("SEARCHABLE", true));
    info.add(new ODocument()
        .field("TYPE_NAME", OType.DECIMAL.toString())
        .field("DATA_TYPE", Types.DECIMAL)
        .field("NULLABLE", DatabaseMetaData.typeNullable)
        .field("CASE_SENSITIVE", true)
        .field("UNSIGNED_ATTRIBUTE", false)
        .field("SEARCHABLE", true));
    info.add(new ODocument()
        .field("TYPE_NAME", OType.FLOAT.toString())
        .field("DATA_TYPE", Types.FLOAT)
        .field("NULLABLE", DatabaseMetaData.typeNullable)
        .field("CASE_SENSITIVE", true)
        .field("UNSIGNED_ATTRIBUTE", false)
        .field("SEARCHABLE", true));
    info.add(new ODocument()
        .field("TYPE_NAME", OType.DOUBLE.toString())
        .field("DATA_TYPE", Types.DOUBLE)
        .field("NULLABLE", DatabaseMetaData.typeNullable)
        .field("CASE_SENSITIVE", true)
        .field("UNSIGNED_ATTRIBUTE", false)
        .field("SEARCHABLE", true));
    info.add(new ODocument()
        .field("TYPE_NAME", OType.EMBEDDED.toString())
        .field("DATA_TYPE", Types.STRUCT)
        .field("NULLABLE", DatabaseMetaData.typeNullable)
        .field("CASE_SENSITIVE", true)
        .field("SEARCHABLE", true));
    info.add(new ODocument()
        .field("TYPE_NAME", OType.EMBEDDEDLIST.toString())
        .field("DATA_TYPE", Types.ARRAY)
        .field("NULLABLE", DatabaseMetaData.typeNullable)
        .field("CASE_SENSITIVE", true)
        .field("SEARCHABLE", true));
    info.add(new ODocument()
        .field("TYPE_NAME", OType.INTEGER.toString())
        .field("DATA_TYPE", Types.INTEGER)
        .field("NULLABLE", DatabaseMetaData.typeNullable)
        .field("CASE_SENSITIVE", true)
        .field("UNSIGNED_ATTRIBUTE", false)
        .field("SEARCHABLE", true));
    info.add(new ODocument()
        .field("TYPE_NAME", OType.LINKLIST.toString())
        .field("DATA_TYPE", Types.ARRAY)
        .field("NULLABLE", DatabaseMetaData.typeNullable)
        .field("CASE_SENSITIVE", true)
        .field("SEARCHABLE", true));
    info.add(new ODocument()
        .field("TYPE_NAME", OType.LONG.toString())
        .field("DATA_TYPE", Types.BIGINT)
        .field("NULLABLE", DatabaseMetaData.typeNullable)
        .field("CASE_SENSITIVE", true)
        .field("UNSIGNED_ATTRIBUTE", false)
        .field("SEARCHABLE", true));
    info.add(new ODocument()
        .field("TYPE_NAME", OType.STRING.toString())
        .field("DATA_TYPE", Types.VARCHAR)
        .field("NULLABLE", DatabaseMetaData.typeNullable)
        .field("CASE_SENSITIVE", true)
        .field("SEARCHABLE", true));
    info.add(new ODocument()
        .field("TYPE_NAME", OType.SHORT.toString())
        .field("DATA_TYPE", Types.SMALLINT)
        .field("NULLABLE", DatabaseMetaData.typeNullable)
        .field("CASE_SENSITIVE", true)
        .field("UNSIGNED_ATTRIBUTE", false)
        .field("SEARCHABLE", true));

    return new OrientJdbcResultSet(new OrientJdbcStatement(connection), info, ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
  }

  @Override
  public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate)
      throws SQLException {
    database.activateOnCurrentThread();
    OMetadata metadata = database.getMetadata();
    if (!approximate) {
      metadata.getIndexManager().reload();
    }

    final Set<OIndex<?>> classIndexes = metadata.getIndexManager().getClassIndexes(table);

    final Set<OIndex<?>> indexes = new HashSet<OIndex<?>>();

    for (OIndex<?> oIndex : classIndexes) {
      if (!unique || oIndex.getType().equals(INDEX_TYPE.UNIQUE.name()))
        indexes.add(oIndex);
    }

    final List<ODocument> records = new ArrayList<ODocument>();

    for (OIndex<?> idx : indexes) {
      boolean notUniqueIndex = !(idx.getType().equals(INDEX_TYPE.UNIQUE.name()));

      final String fieldNames = idx.getDefinition().getFields().toString();
      ODocument doc = new ODocument()
          .field("TABLE_CAT", catalog)
          .field("TABLE_SCHEM", schema)
          .field("TABLE_NAME", table)
          .field("NON_UNIQUE", notUniqueIndex)
          .field("INDEX_QUALIFIER", (Object) null)
          .field("INDEX_NAME", idx.getName())
          .field("TYPE", idx.getType())
          .field("ORDINAL_POSITION", 0)
          .field("COLUMN_NAME", fieldNames.substring(1, fieldNames.length() - 1))
          .field("ASC_OR_DESC", "ASC");

      records.add(doc);
    }

    return new OrientJdbcResultSet(new OrientJdbcStatement(connection), records, ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
  }

  public boolean supportsResultSetType(int type) throws SQLException {

    return false;
  }

  public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {

    return false;
  }

  public boolean ownUpdatesAreVisible(int type) throws SQLException {

    return false;
  }

  public boolean ownDeletesAreVisible(int type) throws SQLException {

    return false;
  }

  public boolean ownInsertsAreVisible(int type) throws SQLException {

    return false;
  }

  public boolean othersUpdatesAreVisible(int type) throws SQLException {

    return false;
  }

  public boolean othersDeletesAreVisible(int type) throws SQLException {

    return false;
  }

  public boolean othersInsertsAreVisible(int type) throws SQLException {

    return false;
  }

  public boolean updatesAreDetected(int type) throws SQLException {

    return false;
  }

  public boolean deletesAreDetected(int type) throws SQLException {
    return false;
  }

  public boolean insertsAreDetected(int type) throws SQLException {

    return false;
  }

  public boolean supportsBatchUpdates() throws SQLException {

    return false;
  }

  public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
    database.activateOnCurrentThread();
    final Collection<OClass> classes = database.getMetadata().getSchema().getClasses();
    final List<ODocument> records = new ArrayList<ODocument>();

    for (OClass cls : classes) {
      final ODocument doc = new ODocument()
          .field("TYPE_CAT", (Object) null)
          .field("TYPE_SCHEM", (Object) null)
          .field("TYPE_NAME", cls.getName())
          .field("CLASS_NAME", cls.getName())
          .field("DATA_TYPE", java.sql.Types.STRUCT)
          .field("REMARKS", (Object) null);
      records.add(doc);

    }

    return new OrientJdbcResultSet(new OrientJdbcStatement(connection), records, ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
  }

  public Connection getConnection() throws SQLException {
    return connection;
  }

  public boolean supportsSavepoints() throws SQLException {

    return false;
  }

  public boolean supportsNamedParameters() throws SQLException {

    return true;
  }

  public boolean supportsMultipleOpenResults() throws SQLException {

    return false;
  }

  public boolean supportsGetGeneratedKeys() throws SQLException {

    return false;
  }

  public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
    database.activateOnCurrentThread();
    final OClass cls = database.getMetadata().getSchema().getClass(typeNamePattern);
    final List<ODocument> records = new ArrayList<ODocument>();

    if (cls != null && cls.getSuperClass() != null) {
      final ODocument doc = new ODocument();
      doc.field("TABLE_CAT", catalog);
      doc.field("TABLE_SCHEM", catalog);
      doc.field("TABLE_NAME", cls.getName());
      doc.field("SUPERTYPE_CAT", catalog);
      doc.field("SUPERTYPE_SCHEM", catalog);
      doc.field("SUPERTYPE_NAME", cls.getSuperClass().getName());
      records.add(doc);
    }

    return new OrientJdbcResultSet(new OrientJdbcStatement(connection), records, ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
  }

  public ResultSet getSuperTables(final String catalog, final String schemaPattern, final String tableNamePattern)
      throws SQLException {
    database.activateOnCurrentThread();
    final OClass cls = database.getMetadata().getSchema().getClass(tableNamePattern);
    final List<ODocument> records = new ArrayList<ODocument>();

    if (cls != null && cls.getSuperClass() != null) {
      final ODocument doc = new ODocument()
          .field("TABLE_CAT", catalog)
          .field("TABLE_SCHEM", catalog)
          .field("TABLE_NAME", cls.getName())
          .field("SUPERTABLE_CAT", catalog)
          .field("SUPERTABLE_SCHEM", catalog)
          .field("SUPERTABLE_NAME", cls.getSuperClass().getName());
      records.add(doc);
    }

    return new OrientJdbcResultSet(new OrientJdbcStatement(connection), records, ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
  }

  public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern)
      throws SQLException {

    return null;
  }

  public boolean supportsResultSetHoldability(int holdability) throws SQLException {

    return false;
  }

  public int getResultSetHoldability() throws SQLException {

    return 0;
  }

  public int getDatabaseMajorVersion() throws SQLException {
    return Integer.valueOf(OConstants.ORIENT_VERSION.split("\\.")[0]);
  }

  public int getDatabaseMinorVersion() throws SQLException {
    return Integer.valueOf(OConstants.ORIENT_VERSION.split("\\.")[1].substring(0, 1));
  }

  public int getJDBCMajorVersion() throws SQLException {

    return 0;
  }

  public int getJDBCMinorVersion() throws SQLException {

    return 0;
  }

  public int getSQLStateType() throws SQLException {

    return 0;
  }

  public boolean locatorsUpdateCopy() throws SQLException {

    return false;
  }

  public boolean supportsStatementPooling() throws SQLException {

    return false;
  }

  public RowIdLifetime getRowIdLifetime() throws SQLException {

    return null;
  }

  public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {

    return null;
  }

  public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {

    return true;
  }

  public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
    return false;
  }

  public ResultSet getClientInfoProperties() throws SQLException {

    return null;
  }

  public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {

    database.activateOnCurrentThread();
    final List<ODocument> records = new ArrayList<ODocument>();

    for (String fName : database.getMetadata().getFunctionLibrary().getFunctionNames()) {
      final ODocument doc = new ODocument()
          .field("FUNCTION_CAT", (Object) null)
          .field("FUNCTION_SCHEM", (Object) null)
          .field("FUNCTION_NAME", fName)
          .field("REMARKS", "")
          .field("FUNCTION_TYPE", procedureResultUnknown)
          .field("SPECIFIC_NAME", fName);

      records.add(doc);
    }

    return new OrientJdbcResultSet(new OrientJdbcStatement(connection), records, ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
  }

  public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern)
      throws SQLException {
    database.activateOnCurrentThread();
    final List<ODocument> records = new ArrayList<ODocument>();

    final OFunction f = database.getMetadata().getFunctionLibrary().getFunction(functionNamePattern);

    for (String p : f.getParameters()) {
      final ODocument doc = new ODocument()
          .field("FUNCTION_CAT", (Object) null)
          .field("FUNCTION_SCHEM", (Object) null)
          .field("FUNCTION_NAME", f.getName())
          .field("COLUMN_NAME", p)
          .field("COLUMN_TYPE", procedureColumnIn)
          .field("DATA_TYPE", java.sql.Types.OTHER)
          .field("SPECIFIC_NAME", f.getName());

      records.add(doc);
    }

    final ODocument doc = new ODocument()
        .field("FUNCTION_CAT", (Object) null)
        .field("FUNCTION_SCHEM", (Object) null)
        .field("FUNCTION_NAME", f.getName())
        .field("COLUMN_NAME", "return")
        .field("COLUMN_TYPE", procedureColumnReturn)
        .field("DATA_TYPE", java.sql.Types.OTHER)
        .field("SPECIFIC_NAME", f.getName());

    records.add(doc);

    return new OrientJdbcResultSet(new OrientJdbcStatement(connection), records, ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
  }

  public ResultSet getPseudoColumns(String arg0, String arg1, String arg2, String arg3) throws SQLException {
    return null;
  }

  public boolean generatedKeyAlwaysReturned() throws SQLException {
    return false;
  }

  private ODocument getPropertyAsDocument(final OClass clazz, final OProperty prop) {
    database.activateOnCurrentThread();
    final OType type = prop.getType();
    return new ODocument()
        .field("TABLE_CAT", database.getName())
        .field("TABLE_SCHEM", database.getName())
        .field("TABLE_NAME", clazz.getName())
        .field("COLUMN_NAME", prop.getName())
        .field("DATA_TYPE", OrientJdbcResultSetMetaData.getSqlType(type))
        .field("TYPE_NAME", type.name())
        .field("COLUMN_SIZE", 1)
        .field("BUFFER_LENGTH", null, OType.INTEGER)
        .field("DECIMAL_DIGITS", null, OType.INTEGER)
        .field("NUM_PREC_RADIX", 10)
        .field("NULLABLE", !prop.isNotNull() ? columnNoNulls : columnNullable)
        .field("REMARKS", prop.getDescription())
        .field("COLUMN_DEF", prop.getDefaultValue())
        .field("SQL_DATA_TYPE", null, OType.INTEGER)
        .field("SQL_DATETIME_SUB", null, OType.INTEGER)
        .field("CHAR_OCTET_LENGTH", null, OType.INTEGER)
        .field("ORDINAL_POSITION", prop.getId(), OType.INTEGER)
        .field("IS_NULLABLE", prop.isNotNull() ? "NO" : "YES");

//
//     *  <LI><B>SCOPE_CATALOG</B> String {@code =>} catalog of table that is the scope
//        *      of a reference attribute (<code>null</code> if DATA_TYPE isn't REF)
//        *  <LI><B>SCOPE_SCHEMA</B> String {@code =>} schema of table that is the scope
//        *      of a reference attribute (<code>null</code> if the DATA_TYPE isn't REF)
//        *  <LI><B>SCOPE_TABLE</B> String {@code =>} table name that this the scope
//     *      of a reference attribute (<code>null</code> if the DATA_TYPE isn't REF)
//        *  <LI><B>SOURCE_DATA_TYPE</B> short {@code =>} source type of a distinct type or user-generated
//        *      Ref type, SQL type from java.sql.Types (<code>null</code> if DATA_TYPE
//        *      isn't DISTINCT or user-generated REF)
//        *   <LI><B>IS_AUTOINCREMENT</B> String  {@code =>} Indicates whether this column is auto incremented
//     *       <UL>
//     *       <LI> YES           --- if the column is auto incremented
//        *       <LI> NO            --- if the column is not auto incremented
//     *       <LI> empty string  --- if it cannot be determined whether the column is auto incremented
//     *       </UL>
//     *   <LI><B>IS_GENERATEDCOLUMN</B> String  {@code =>} Indicates whether this is a generated column
//     *       <UL>
//     *       <LI> YES           --- if this a generated column
//        *       <LI> NO            --- if this not a generated column
//     *       <LI> empty string  --- if it cannot be determined whether this is a generated column
//     *       </UL>
//     *  </OL>
//

  }

  public <T> T unwrap(Class<T> iface) throws SQLException {

    return null;
  }

  public boolean isWrapperFor(Class<?> iface) throws SQLException {

    return false;
  }

  public Set<String> getSYSTEM_CLUSTERS() {
    return SYSTEM_TABLE_TYPES;
  }
}
