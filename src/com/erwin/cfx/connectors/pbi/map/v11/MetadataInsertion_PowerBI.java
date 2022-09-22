/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.connectors.pbi.map.v11;

import com.erwin.cfx.connectors.pbi.map.util.ReadXMLFileToObject;
import com.erwin.cfx.connectors.pbi.map.pojo.PowerBI_Bean;
import com.ads.api.beans.common.AuditHistory;
import com.ads.api.beans.sm.SMEnvironment;
import com.ads.api.beans.sm.SMSystem;
import com.ads.api.beans.sm.SMTable;
import com.ads.api.util.SystemManagerUtil;
import com.ads.mm.db.dao.DBColumn;
import com.ads.mm.db.dao.DBEnvironment;
import com.ads.mm.db.dao.DBSchema;
import com.ads.mm.db.dao.DBSystem;
import com.ads.mm.db.dao.DBTable;
import static com.erwin.cfx.connectors.pbi.map.v11.PowerBIReportParser.exceptionLog;
import com.erwin.cfx.connectors.pbi.map.util.PowerBI_LOGGER;
import com.icc.util.DBProperties;
import com.icc.util.DatabaseUtil;
import com.icc.util.RequestStatus;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.log4j.Logger;

public class MetadataInsertion_PowerBI {

    private static Logger logger = Logger.getLogger(MetadataInsertion_PowerBI.class);
    static StringBuilder message = null;
    static PowerBI_Bean bean = new PowerBI_Bean();

    public static void deletTable(SystemManagerUtil systemManagerUtil, int envid, String tableName) {
        try {
            int tableId = systemManagerUtil.getTableId(envid, tableName);
            if (tableId > 0) {
                systemManagerUtil.deleteTable(tableId);
            }
        } catch (Exception e) {
//            StringWriter exception = new StringWriter();
//            e.printStackTrace(new PrintWriter(exception));
//            PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Error::Exception In MetadataInsertion_PowerBI.deletTable() Method " + exception + "\n");
        }
    }

    public static DBTable prepareDBTab(DBTable dbtab, String tableName, Calendar time) {
        String table_defination = "";
        if (ReadXMLFileToObject.itemPathAndxlPath.containsKey(tableName)) {
            try {
                table_defination = ReadXMLFileToObject.itemPathAndxlPath.get(tableName).replace("\\\\\\\\", "\\").replace("\\\\", "\\").split("#erwin@")[1];
            } catch (ArrayIndexOutOfBoundsException ae) {
                table_defination = ReadXMLFileToObject.itemPathAndxlPath.get(tableName);
            }
        }
        if (dbtab == null) {
            dbtab = new DBTable(tableName);
            dbtab.setLastModifiedDateTime(time);
            dbtab.setCreatedDateTime(time);
            dbtab.setValid(true);
            dbtab.setCreatedBy("Administrator");
            dbtab.setLastModifiedBy("Administrator");
            dbtab.setTableType(DBTable.TableType.TABLE);
        }
        if (table_defination.trim().equals("Manual Table")) {
            dbtab.setTableDefinition("Enter Data (Manual Inserted Data)");
        } else if (table_defination.contains("xlsx") || table_defination.contains("xls")) {
            dbtab.setTableDefinition("Excel Source");
            dbtab.setTableDefinitionComments(table_defination);
            dbtab.setTableComments(table_defination);
        } else if (table_defination.contains("csv")) {
            dbtab.setTableDefinition("CSV Source");
            dbtab.setTableDefinitionComments(table_defination);
            dbtab.setTableComments(table_defination);
        } else if (!table_defination.equals("")) {
            dbtab.setTableDefinition("Flat File");
            dbtab.setTableDefinitionComments(table_defination);
            dbtab.setTableComments(table_defination);
        }
        return dbtab;
    }

    public static DBEnvironment insertDBEnvironment(int systemId, int environmentId, String environmentName, String envreloadType, SystemManagerUtil systemManagerUtil, Calendar time, Map<String, String> dbProperties, Map<String, Set<String>> mapOfTables) {
        DBEnvironment dbEnv = DatabaseUtil.loadEnvironment(systemId, environmentName);
        try {
            dbEnv.setSystem(new DBSystem(systemId));
            dbEnv.setEnvironmentId(environmentId);
            DBTable dbtab = null;
            for (Map.Entry<String, Set<String>> mapOfTable : mapOfTables.entrySet()) {
                String tableName = mapOfTable.getKey().trim();
                if (!tableName.equalsIgnoreCase("")) {
                    if (envreloadType.equals("Fresh Reload")) {
                        deletTable(systemManagerUtil, environmentId, tableName);
                    }
                    dbtab = dbEnv.getTable(tableName);
                    dbtab = prepareDBTab(dbtab, tableName, time);
                    dbtab.setValid(true);
                    if ((tableName.split("\\.").length != 1) && !environmentName.equals("Dashboard")) {
                        dbtab.setSchemaName(tableName.substring(0, tableName.lastIndexOf(".")));
                    }
                    Set<String> setOfColumns = mapOfTable.getValue();
                    List<String> listOfColumns = new ArrayList(setOfColumns);
                    listOfColumns.remove("");
                    for (int col_i = 0; col_i < listOfColumns.size(); col_i++) {
                        String columnName = listOfColumns.get(col_i).trim();
                        String description = "";
                        String comments = "";
                        String dataType = "";
                        if (bean.getREPORT_expressionAgainstMeasureMap().containsKey(tableName.split("\\.")[0])) {
                            if (bean.getREPORT_expressionAgainstMeasureMap().get(tableName.split("\\.")[0]).containsKey(tableName + "@ERWIN@" + columnName)) {
                                description = bean.getREPORT_expressionAgainstMeasureMap().get(tableName.split("\\.")[0]).get(tableName + "@ERWIN@" + columnName);
                            }
                        }
                        if (bean.getREPORT_descriptionAgainstColumnMap().containsKey(tableName.split("\\.")[0])) {
                            if (bean.getREPORT_descriptionAgainstColumnMap().get(tableName.split("\\.")[0]).containsKey(tableName + "@ERWIN@" + columnName)) {
                                comments = bean.getREPORT_descriptionAgainstColumnMap().get(tableName.split("\\.")[0]).get(tableName + "@ERWIN@" + columnName);
                            }
                        }
                        if (bean.getREPORT_datatypeAgainstColumnMap().containsKey(tableName.split("\\.")[0])) {
                            if (bean.getREPORT_datatypeAgainstColumnMap().get(tableName.split("\\.")[0]).containsKey(tableName + "@ERWIN@" + columnName)) {
                                dataType = bean.getREPORT_datatypeAgainstColumnMap().get(tableName.split("\\.")[0]).get(tableName + "@ERWIN@" + columnName);
                            }
                        }
                        String column_ = "";
//                        String dataType = "";
                        String length = "";
                        String nullableflag = "";
                        try {
                            if (columnName.contains("#ERWIN#") && columnName.split("#ERWIN#").length == 4) {
                                column_ = columnName.split("#ERWIN#")[0];
//                                dataType = columnName.split("#ERWIN#")[1];
                                length = columnName.split("#ERWIN#")[2];
                                nullableflag = columnName.split("#ERWIN#")[3];
//                                if (dataType.equalsIgnoreCase("StringType")) {
//                                    dataType = "string";
//                                } else if (dataType.equalsIgnoreCase("IntegerType")) {
//                                    dataType = "integer";
//                                } else if (dataType.equalsIgnoreCase("ByteType")) {
//                                    dataType = "byte";
//                                } else if (dataType.equalsIgnoreCase("ShortType")) {
//                                    dataType = "short";
//                                } else if (dataType.equalsIgnoreCase("DoubleType")) {
//                                    dataType = "double";
//                                } else if (dataType.equalsIgnoreCase("BooleanType")) {
//                                    dataType = "boolean";
//                                } else if (dataType.equalsIgnoreCase("FloatType")) {
//                                    dataType = "float";
//                                } else if (dataType.equalsIgnoreCase("LongType")) {
//                                    dataType = "long";
//                                } else if (dataType.equalsIgnoreCase("TimestampType")) {
//                                    dataType = "timestamp";
//                                } else {
//                                    dataType = "varchar";
//                                }
                            } else {
                                column_ = columnName;
                            }
                            if ("".equalsIgnoreCase(column_)) {
                                continue;
                            }
                            DBColumn dbColumn = insertDBColumns(dbtab, column_, dataType, length, nullableflag, time, description, comments);
                            dbtab.addColumn(dbColumn);
                        } catch (Exception colsExe) {
                            colsExe.printStackTrace();
                            logger.error(colsExe);
                            StringWriter exception = new StringWriter();
                            colsExe.printStackTrace(new PrintWriter(exception));
                        }
                    }
                    if (dbtab != null) {
                        dbEnv.addTable(dbtab);
                        dbEnv.setDatabseType(dbProperties.get("databaseType"));
                    }
                }
            }
        } catch (Exception ex) {

            StringWriter exception = new StringWriter();
            ex.printStackTrace(new PrintWriter(exception));
            PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Error::Exception In MetadataInsertion_PowerBI.insertDBEnvironment() Method " + exception + "\n");

        }
        return dbEnv;
    }

    public static DBColumn insertDBColumns(DBTable dbtab, String column_, String dataType, String length, String nullableflag, Calendar time, String description, String comments) {
        DBColumn dbColumn = null;
        dbColumn = dbtab.getColumn(column_);
        if (dbColumn == null) {
            dbColumn = new DBColumn(column_);
            dbColumn.setLastModifiedDatetime(time);
            dbColumn.setCreatedDatetime(time);
            dbColumn.setValid(true);
            dbColumn.setDatatype(dataType);
            dbColumn.setLength(length);
            if (nullableflag.equalsIgnoreCase("true")) {
                dbColumn.setNullableFlag(true);
            }
            dbColumn.setCreatedBy("Administrator");
            dbColumn.setLastModifiedBy("Administrator");
            dbColumn.setDataDomain("");
            dbColumn.setDefinition(description);
            dbColumn.setColumnComments(comments);
        } else {
            dbColumn.setDataDomain("");
            dbColumn.setValid(true);
        }
        return dbColumn;
    }

    public static String insertMetadata(SystemManagerUtil systemManagerUtil,
            Map<String, Map<String, Map<String, Set<String>>>> mapOfSystems, String envreloadType, String srcSystemName, String tgtSystemName, PowerBI_Bean pbib) {
        String systemName_ = "";
        String environmentName = "";
        String tableName = "";
        int systemId = 0;
        int environmentId = 0;
        RequestStatus reqStat = null;
        bean = pbib;
        Set<String> setOfColumns = null;
        message = new StringBuilder();
        if (mapOfSystems.isEmpty()) {
            return message.append("No data is present  to  create metadata\n").toString();
        }

        Map<String, String> dbProperties = getSystem_env_Properties();
        try {
            for (Map.Entry<String, Map<String, Map<String, Set<String>>>> mapOfSystem : mapOfSystems.entrySet()) {
                systemName_ = mapOfSystem.getKey();
                if (!systemName_.trim().equals(pbib.getPOWERBI_SYSTEM())) {
                    continue;
                }
                systemId = createSystem_If_NotAvailable(systemManagerUtil, systemName_);
                if (systemName_.equalsIgnoreCase(srcSystemName) || systemName_.equalsIgnoreCase(tgtSystemName)) {
                    Map<String, Map<String, Set<String>>> mapOfEnvs = mapOfSystem.getValue();
                    for (Map.Entry<String, Map<String, Set<String>>> mapOfEnv : mapOfEnvs.entrySet()) {
                        environmentName = mapOfEnv.getKey();
                        environmentName = environmentName.replaceAll("[^a-zA-Z0-9_\\s]", "");
                        if ("".equalsIgnoreCase(environmentName)) {
                            continue;
                        }
                        try {
                            if (envreloadType.equals("Versioning")) {
                                deleteTableOnVersioning(systemManagerUtil, systemName_, environmentName, "version created", "version created");
                            } else {
                                deleteTableOnFreshLoad(systemManagerUtil, systemName_, environmentName);
                            }
                            dbProperties.put("environmentname", environmentName);
                            dbProperties.put("environmenttype", environmentName);
                            environmentId = metadataEnvironmentCreation(systemId, environmentName, systemManagerUtil, dbProperties);
                        } catch (Exception ex) {
                            ex.printStackTrace();
                            logger.error(ex);
                        }
                        if (environmentId == -1) {
                            continue;
                        }
                        Map<String, Set<String>> mapOfTables = mapOfEnv.getValue();
                        Calendar time = Calendar.getInstance();
                        DBEnvironment dbEnv = insertDBEnvironment(systemId, environmentId, environmentName, envreloadType, systemManagerUtil, time, dbProperties, mapOfTables);
                        if (envreloadType.equals("Fresh Reload")) {
                            try {
                                PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Info::Inserting Metadata for Environment :" + environmentName + " \n");
                                PowerBI_LOGGER.overviewLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Info::Inserting Metadata for Environment :" + environmentName + " \n");
                                reqStat = systemManagerUtil.updateEnvironmentMetadata(dbEnv, DBProperties.Operation.UPDATE_APPEND, false);
                                String msg = "Subquery returned more than 1 value. This is not permitted when the subquery follows =, !=, <, <= , >, >= or when the subquery is used as an expression.";
                                if (msg.trim().equals(reqStat.getStatusMessage().trim())) {
                                    PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Info::" + environmentName + "Inserted Successfully :" + " \n");
                                    PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Info::" + environmentName + "Saved Successfully :" + " \n");
                                    PowerBI_LOGGER.overviewLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Info::" + environmentName + "Inserted Successfully :" + " \n");
                                    PowerBI_LOGGER.overviewLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Info::" + environmentName + "Saved Successfully :" + " \n");
                                    message.append("\n " + environmentName + " : Environment Saved Successfully\n");
                                } else {
                                    PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Info::" + environmentName + "Inserted Successfully :" + " \n");
                                    PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Info::" + environmentName + "Saved Successfully :" + " \n");
                                    PowerBI_LOGGER.overviewLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Info::" + environmentName + "Inserted Successfully :" + " \n");
                                    PowerBI_LOGGER.overviewLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Info::" + environmentName + "Saved Successfully :" + " \n");
                                    message.append("\n " + environmentName + " : " + reqStat.getStatusMessage() + "\n");
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                                StringWriter exception = new StringWriter();
                                e.printStackTrace(new PrintWriter(exception));
                                PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Error::Exception In MetadataInsertion_PowerBI.insertDataIntoAMM() Method " + exception + "\n");
                                exceptionLog.append("Exception In insertDataIntoAMM() \n" + exception.toString());
                                exceptionLog.append("\n ================================");
                            }
                        } else {
                            try {
                                reqStat = systemManagerUtil.updateEnvironmentMetadata(dbEnv, DBProperties.Operation.UPDATE_APPEND, false);
                                message.append("\n " + environmentName + " : " + reqStat.getStatusMessage() + "\n");
                            } catch (Exception e) {
                                e.printStackTrace();
                                logger.error(e);
                                StringWriter exception = new StringWriter();
                                e.printStackTrace(new PrintWriter(exception));
                                PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Error::Exception In MetadataInsertion_PowerBI.insertDataIntoAMM() Method " + exception + "\n");
                                exceptionLog.append("Exception In insertDataIntoAMM() \n" + exception.toString());
                                exceptionLog.append("\n ================================");
                            }
                        }
                    }
                }
            }
            if (reqStat != null) {
                return reqStat.getStatusMessage();
            } else {
                return message.toString();
            }
        } catch (Exception ex) {
        }
        return message.toString();
    }

    public static int metadataEnvironmentCreation(int systemId, String environentName, SystemManagerUtil systemManagerUtil, Map<String, String> dbProperties) {
        int environmentId = -1;
        try {
            if (systemManagerUtil.getEnvironmentId(systemId, environentName) == -1) {
                environmentId = createEnvironment(systemId, environentName, systemManagerUtil, dbProperties);
            } else {
                environmentId = systemManagerUtil.getEnvironmentId(systemId, environentName);
            }
        } catch (Exception e) {
            environmentId = createEnvironment(systemId, environentName, systemManagerUtil, dbProperties);
        }
        return environmentId;
    }

    public static int createEnvironment(int systemId, String environentName, SystemManagerUtil systemManagerUtil, Map<String, String> dbProperties) {
        int environmentId = -1;
        try {
            SMEnvironment environment = new SMEnvironment();
            environment.setSystemId(systemId);
            //now create  AuditHistory class object
            AuditHistory auditHistory = new AuditHistory();
            auditHistory.setCreatedBy("Administrator");
            environment.setAuditHistory(auditHistory);
            environment.setSystemEnvironmentType(environentName);
            environment.setSystemEnvironmentName(environentName);

            environment.setDatabaseType(dbProperties.get("databaseType"));
            environment.setDatabaseName(dbProperties.get("databasename"));
            environment.setNoOfPartitions(2);
            environment.setMaximumNoOfConnectionsPerPartition(5);
            environment.setMinimumNoOfConnectionsPerPartition(3);
            environment.setDatabaseUserName(dbProperties.get("environmentusername"));
            environment.setDatabasePassword(dbProperties.get("environmentpassword"));
            environment.setDatabaseURL(dbProperties.get("databaseurl"));
            environment.setDatabaseDriver(dbProperties.get("databaserdriver"));
            environment.setDatabaseIPAddress(dbProperties.get("ip"));
            environment.setDatabasePort(dbProperties.get("databaseport"));
//            environment.setFileLocation(dbProperties.get("Filelocation"));
            environment.setFieldDelimiter(",");
            environment.setRowDelimiter("\n");
            RequestStatus requestStatus = systemManagerUtil.createEnvironment(environment);
//            requestStatus.getStatusMessage()
            if (requestStatus.getStatusNumber() == 0) {
            }
            logger.info(requestStatus.getStatusMessage());
            message.append(environentName + " : ");
            message.append(requestStatus.getStatusMessage());
            environmentId = systemManagerUtil.getEnvironmentId(systemId, environentName);
        } catch (Exception e) {
            logger.error(e);
            StringWriter exception = new StringWriter();
            e.printStackTrace(new PrintWriter(exception));
            exceptionLog.append("Exception In createEnvironment() \n" + exception.toString());
            exceptionLog.append("\n ================================");
        }
        return environmentId;
    }

    private static void deleteTableOnFreshLoad(SystemManagerUtil smutil, String systemName, String environmentName) {

        try {
            int envId = smutil.getEnvironmentId(systemName, environmentName);
            if (envId > 0) {
                smutil.deleteEnvironment(envId);
            }
        } catch (Exception e) {
            logger.error(e);
            StringWriter exception = new StringWriter();
            e.printStackTrace(new PrintWriter(exception));
            PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Error::Exception In MetadataInsertion_PowerBI.deleteTableOnFreshLoad() Method " + exception + "\n");
            exceptionLog.append("Exception In deleteTableOnFreshLoad() \n" + exception.toString());
            exceptionLog.append("\n ================================");
        }

    }

    public static void deleteTable(SystemManagerUtil smutil, int environmentId, String tableName) {
        try {
            int tableID = smutil.getTableId(environmentId, tableName);
            if (tableID > 0) {
                smutil.deleteTable(tableID);
            }
            if (tableName.contains(".")) {
                String tab1 = tableName.split("\\.")[tableName.split("\\.").length - 1].trim();
                tableID = smutil.getTableId(environmentId, tableName);
                if (tableID > 0) {
                    smutil.deleteTable(tableID);
                }
            }

        } catch (Exception ex) {
            logger.error(ex);
        }
    }

    private static void deleteTableOnVersioning(SystemManagerUtil systemManagerUtil, String sysName, String envName, String description, String label) {
        try {
            int envid = systemManagerUtil.getEnvironmentId(sysName, envName);
            String msg = systemManagerUtil.versionEnvironment(envid, description, label).getStatusMessage();
            List<SMTable> tableList = systemManagerUtil.getEnvironmentTables(envid);
            for (int i = 0; i < tableList.size(); i++) {
                SMTable table = tableList.get(i);
                systemManagerUtil.deleteTable(table.getTableId());
            }
            message.append("\n envName : " + msg + "\n");
        } catch (Exception e) {
            logger.error(e);
            StringWriter exception = new StringWriter();
            e.printStackTrace(new PrintWriter(exception));
            PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Error::Exception In MetadataInsertion_PowerBI.deleteTableOnVersioning() Method " + exception + "\n");
            exceptionLog.append("Exception In deleteTableOnVersioning() \n" + exception.toString());
            exceptionLog.append("\n ================================");
        }
    }

    public static int createSystem_If_NotAvailable(SystemManagerUtil smutil, String systemName) {
        int systemId = 0;
        try {

            systemId = smutil.getSystemId(systemName);
            if (systemId <= 0) {
                systemId = createSMSystem(smutil, systemName);
            } else {
                systemId = smutil.getSystemId(systemName);
            }
        } catch (Exception ex) {

        }
        return systemId;
    }

    public static int createSMSystem(SystemManagerUtil smutil, String systemName) {
        try {
            SMSystem system = new SMSystem();
            system.setSystemName(systemName);
            smutil.createSystem(system);
            return smutil.getSystemId(systemName);
        } catch (Exception ex) {

        }
        return 0;
    }

    private static Map<String, String> getSystem_env_Properties() {
        Map<String, String> map = new HashMap();
        map.put("databaseType", "SqlServer");
        map.put("environmentusername", "username");
        map.put("environmentpassword", "password");
        map.put("databasename", "PowerBI");
        map.put("databaserdriver", "com.microsoft.sqlserver.jdbc.SQLServerDriver");
        map.put("databaseurl", "jdbc:sqlserver://qe:9999;databaseName=PowerBI");
        map.put("databaseport", "9999");
        map.put("ip", "pbi");
        return map;
    }

}
