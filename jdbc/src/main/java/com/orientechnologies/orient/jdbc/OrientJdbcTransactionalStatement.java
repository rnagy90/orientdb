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
 *  - Only commands which are supported by the Orient team (e.g.: UPDATE, INSERT etc.)
 *  - When the execute command is called, just adds the command into the super class's batches field.
 *  - The {@link OrientJdbcConnection#commit()} will call the {@link #executeTransaction()} to perform
 *    the script execution
 */
public class OrientJdbcTransactionalStatement extends OrientJdbcStatement {

  private Pattern invalidScriptPattern = Pattern.compile("(DROP|CREATE|ALTER) (CLASS|PROPERTY|INDEX|DATABASE) .*");
  private boolean commited             = false;

  public OrientJdbcTransactionalStatement(OrientJdbcConnection iConnection) {
    super(iConnection);
  }

  @Override
  public boolean execute(String sqlCommand) throws SQLException {
    if (invalidScriptPattern.matcher(sqlCommand).matches()) {
      throw new SQLException("This script is not transactional compatible!" + sqlCommand);
    }
    addBatch(sqlCommand);

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
    super.addBatch(sqlCommand);
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
    return closed || commited;
  }

  public boolean isCommited() {
    return commited;
  }

  /**
   * Execute the collected scripts as a BATCH command;
   *
   * @throws SQLException
   */
  public void executeTransaction() throws SQLException {
    if (commited) {
      throw new SQLException("Already commited!");
    }

    documents = new ArrayList<ODocument>();

    if (batches.size() > 0) {
      executeScript(buildScript(batches, true));
    }

    resultSet = new OrientJdbcResultSet(this, documents, resultSetType, resultSetConcurrency, resultSetHoldability);

    commited = true;
  }

  private String buildScript(List<String> scripts, boolean wrap) {
    StringBuilder scriptBuilder = new StringBuilder();

    scriptBuilder.append("BEGIN\n");

    for (String sql : scripts) {
      scriptBuilder.append(sql.replaceAll("(\n|\n\\w)", " "));
      scriptBuilder.append("\n");
    }

    scriptBuilder.append("COMMIT\n");

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
