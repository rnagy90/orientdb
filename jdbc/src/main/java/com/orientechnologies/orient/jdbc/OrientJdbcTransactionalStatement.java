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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * @author Robert Nagy (Qulto - rnagy@monguz.hu)
 *
 * This class helps to run commands in one transaction batch script.
 * Criterias:
 *   - Only commands which supported by the Orient team (e.g.: UPDATE, INSERT etc.)
 *   - When the execute command is called, just adds the command into the proper collection.
 *     The transaction capable commands are placed into the parent 'batches' field, the remaining are placed into this class's "irreversibleBatches" field.
 *   - The {@link OrientJdbcConnection#commit()} will call the {@link #executeTransaction()} to perform the script executes
 */
public class OrientJdbcTransactionalStatement extends OrientJdbcStatement {

  private final Pattern      nonWrappablePattern;
  private final List<String> nonWrappableBatches;

  private boolean            commited = false;

  public OrientJdbcTransactionalStatement(OrientJdbcConnection iConnection) {
    super(iConnection);
    nonWrappablePattern = Pattern.compile("(DROP|CREATE) (CLASS|PROPERTY|INDEX) .*");
    nonWrappableBatches = new ArrayList<String>();
  }

  @Override
  public boolean execute(String sqlCommand) throws SQLException {
    if (!"".equals(sqlCommand)) {
      addBatch(sqlCommand);
    }

    return false;
  }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    if (super.execute(sql)) {
      return resultSet;
    } else {
      return null;
    }
  }

  @Override
  public void addBatch(String sqlCommand) throws SQLException {
    if ("".equals(sqlCommand)) {
      return;
    }

    sqlCommand = mayCleanForSpark(sqlCommand);
    if (nonWrappablePattern.matcher(sqlCommand).matches()) {
      nonWrappableBatches.add(sqlCommand);
    } else {
      super.addBatch(sqlCommand);
    }

  }

  @Override
  public int getUpdateCount() throws SQLException {
    return -1;
  }

  @Override
  public void close() throws SQLException {
    if (getConnection().getAutoCommit()) {
      super.close();
    }
  }

  @Override
  public boolean isClosed() throws SQLException {
    return closed;
  }

  public boolean isCommited() {
    return commited;
  }

  /**
   * Execute the collected scripts. First execute irreversible commands (e.g.: CREATE/DROP CLASS, CREATE/DROP PROPERTY) Then execute
   * the transaction capable commands(e.g.: INSERT, UPDATE)
   *
   * @throws SQLException
   */
  public void executeTransaction() throws SQLException {
    if (commited) {
      throw new SQLException("Already commited!");
    }

    documents = new ArrayList<ODocument>();

    StringBuilder scriptBuilder = new StringBuilder();

    if (nonWrappableBatches.size() > 0) {
      // First run the irreversible commands
      for (String sql : nonWrappableBatches) {
        scriptBuilder.append(sql);
        scriptBuilder.append("\n");
      }
      executeScript(buildScript(nonWrappableBatches, false));
    }

    if (batches.size() > 0) {
      // Run the the root batches, because those can be wrapper with 'BEGIN-COMMIT'
      executeScript(buildScript(batches, true));
    }

    resultSet = new OrientJdbcResultSet(this, documents, resultSetType, resultSetConcurrency, resultSetHoldability);

    commited = true;
  }

  private String buildScript(List<String> scripts, boolean wrap) {
    StringBuilder scriptBuilder = new StringBuilder();

    if (wrap) {
      scriptBuilder.append("BEGIN\n");
    }

    for (String sql : scripts) {
      scriptBuilder.append(sql);
      scriptBuilder.append("\n");
    }

    if (wrap) {
      scriptBuilder.append("COMMIT\n");
    }

    return scriptBuilder.toString();
  }

  private void executeScript(String script) throws SQLException {
    try {
      Object rawResult = executeCommand(new OCommandScript("sql", script));
      if (rawResult instanceof List<?>) {
        documents.addAll((List<ODocument>) rawResult);
      } else if (rawResult instanceof ODocument) {
        documents.add((ODocument) rawResult);
      } else if (rawResult instanceof Integer) {
        documents.add(new ODocument().field("VALUE", 1));
      } else if (rawResult instanceof OIdentifiable) {
        documents.add((ODocument) ((OIdentifiable) rawResult).getRecord());
      }
    } catch (OQueryParsingException e) {
      throw new SQLSyntaxErrorException("Error while parsing query", e);
    } catch (OException e) {
      throw new SQLException("Error while executing query", e);
    }

  }
}
