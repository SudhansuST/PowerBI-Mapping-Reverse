/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.connectors.pbi.map.v11;

import com.ads.api.beans.common.AuditHistory;
import com.ads.api.beans.common.Node;
import com.ads.api.beans.kv.KeyValue;
import com.ads.api.beans.sm.SMEnvironment;
import com.ads.api.beans.sm.SMSystem;
import com.ads.api.beans.sm.SMTable;
import com.ads.api.util.KeyValueUtil;
import com.ads.api.util.SystemManagerUtil;
import com.ads.mm.db.dao.DBColumn;
import com.ads.mm.db.dao.DBEnvironment;
import com.ads.mm.db.dao.DBSchema;
import com.ads.mm.db.dao.DBSystem;
import com.ads.mm.db.dao.DBTable;
import static com.erwin.cfx.connectors.pbi.map.v11.PowerBIReportParser.exceptionLog;
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

/**
 *
 * @author Bhanu Prakash Earla
 */
public class BulkInsertionOfEmmMetaData {

    private static final Logger LOGGER = Logger.getLogger(BulkInsertionOfEmmMetaData.class);
    static StringBuilder message = null;

    //starting of execution
    public static String insertDataIntoAMM(SystemManagerUtil systemManagerUtil,
            Map<String, Map<String, Map<String, Set<String>>>> mapOfSystems, String envreloadType, String srcSystemName, String tgtSystemName) {
        String environmentName = "";
        String tableName = "";
        int environmentId = -1;
        String systemName_ = "";
        RequestStatus reqStat = null;
        Set<String> setOfColumns = null;
        int systemId = 0;
//        BulkInsertionOfEmmMetaData.message = log;
        message = new StringBuilder();
        if (mapOfSystems.isEmpty()) {
            return message.append("No data is present  to  create metadata").toString();
        }
        //get the system and Env Static properties
        Map<String, String> dbProperties = getSystem_env_Properties();
        try {
            for (Map.Entry<String, Map<String, Map<String, Set<String>>>> mapOfSystem : mapOfSystems.entrySet()) {
                systemName_ = mapOfSystem.getKey();
                if (systemName_.equalsIgnoreCase(srcSystemName) || systemName_.equalsIgnoreCase(tgtSystemName)) {
                    //continue;

                    Map<String, Map<String, Set<String>>> mapOfEnvs = mapOfSystem.getValue();
                    //system creation method

                    for (Map.Entry<String, Map<String, Set<String>>> mapOfEnv : mapOfEnvs.entrySet()) {
                        environmentName = mapOfEnv.getKey();
                        environmentName = environmentName.replaceAll("[^a-zA-Z0-9_\\s]", "");
                        systemId = metadataSystemCreation(systemName_, systemManagerUtil, envreloadType, environmentName);
                        if ("".equalsIgnoreCase(environmentName)) {
                            continue;
                        }
                        //here it contains Tablename,Columlist
                        Map<String, Set<String>> mapOfTables = mapOfEnv.getValue();
                        try {
                            // environment creation
                            //check wheather it is versioning or fresh load
                            //if it is fresh load delete that environment 
                            //if  it is versioning do version
                            if (envreloadType.equals("Versioning")) {
                                deleteTableOnVersioning(systemManagerUtil, systemName_, environmentName, "version created", "version created");
                            }
                            dbProperties.put("environmentname", environmentName);
                            dbProperties.put("environmenttype", environmentName);
                            environmentId = metadataEnvironmentCreation(systemId, environmentName, systemManagerUtil, dbProperties);
                            //get folders list 
                        } catch (Exception ex) {
                            ex.printStackTrace();
                            LOGGER.error(ex);
                        }
                        if (environmentId == -1) {
                            continue;
                        }
                        Calendar time = Calendar.getInstance();
                        DBEnvironment dbEnv = DatabaseUtil.loadEnvironment(systemId, environmentName);
                        dbEnv.setSystem(new DBSystem(systemId));
                        dbEnv.setEnvironmentId(environmentId);
                        DBTable dbtab = null;
                        DBSchema schema = null;
                        DBColumn dbColumn = null;
                        for (Map.Entry<String, Set<String>> mapOfTable : mapOfTables.entrySet()) {
                            tableName = mapOfTable.getKey();
                            //String trypeOfFile = mapOfTable.getKey().split("##")[1];
                            if ("".equalsIgnoreCase(tableName)) {
                                continue;
                            }
                            //check wheather table is present is or not 
                            //if it is present delete that table and add new table
                            if (envreloadType.equals("Fresh Reload")) {
                                deleteTable(systemManagerUtil, environmentId, tableName);
                            }
                            dbtab = dbEnv.getTable(tableName);
                            if (dbtab == null) {
                                dbtab = new DBTable(tableName);
                                if ((tableName.split("\\.").length != 1) && !environmentName.equals("Dashboard")) {
                                    dbtab.setSchemaName(tableName.substring(0, tableName.lastIndexOf(".")));
                                }
                                dbtab.setLastModifiedDateTime(time);
                                dbtab.setCreatedDateTime(time);
                                dbtab.setValid(true);
                                //dbtab.setUserDefined1(trypeOfFile);

                                dbtab.setCreatedBy("Administrator");
                                dbtab.setLastModifiedBy("Administrator");
                                dbtab.setTableType(DBTable.TableType.TABLE);

                            }
                            dbtab.setValid(true);
                            setOfColumns = mapOfTable.getValue();
                            List<String> listOfColumns = new ArrayList(setOfColumns);
                            listOfColumns.remove("");

                            for (int col_i = 0; col_i < listOfColumns.size(); col_i++) {
                                String columnName = listOfColumns.get(col_i);
                                String column_ = "";
                                String dataType = "";
                                String length = "";
                                String nullableflag = "";
                                try {
                                    if (columnName.contains("#ERWIN#") && columnName.split("#ERWIN#").length == 4) {
                                        column_ = columnName.split("#ERWIN#")[0].replace("\"", "").replace("*", "");
                                        dataType = columnName.split("#ERWIN#")[1];
                                        length = columnName.split("#ERWIN#")[2];
                                        nullableflag = columnName.split("#ERWIN#")[3];
                                        if (dataType.equalsIgnoreCase("StringType")) {
                                            dataType = "string";
                                        } else if (dataType.equalsIgnoreCase("IntegerType")) {
                                            dataType = "integer";
                                        } else if (dataType.equalsIgnoreCase("ByteType")) {
                                            dataType = "byte";
                                        } else if (dataType.equalsIgnoreCase("ShortType")) {
                                            dataType = "short";
                                        } else if (dataType.equalsIgnoreCase("DoubleType")) {
                                            dataType = "double";
                                        } else if (dataType.equalsIgnoreCase("BooleanType")) {
                                            dataType = "boolean";
                                        } else if (dataType.equalsIgnoreCase("FloatType")) {
                                            dataType = "float";
                                        } else if (dataType.equalsIgnoreCase("LongType")) {
                                            dataType = "long";
                                        } else if (dataType.equalsIgnoreCase("TimestampType")) {
                                            dataType = "timestamp";
                                        } else {
                                            dataType = "varchar";
                                        }
                                    } else {
                                        column_ = columnName.replace("\"", "").replace("*", "");
                                    }
                                    if ("".equalsIgnoreCase(column_)) {
                                        continue;
                                    }
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

                                    } else {
                                        dbColumn.setDataDomain("");
                                        dbColumn.setValid(true);
                                    }
                                    dbtab.addColumn(dbColumn);
                                } catch (Exception colsExe) {
                                    colsExe.printStackTrace();
                                    LOGGER.error(colsExe);
                                    StringWriter exception = new StringWriter();
                                    colsExe.printStackTrace(new PrintWriter(exception));
                                    exceptionLog.append("Exception In insertDataIntoAMM() \n" + exception.toString());
                                    exceptionLog.append("\n ================================");
                                    continue;
                                }
                            }
                            if (dbtab != null) {
                                dbEnv.addTable(dbtab);
//                            dbEnv.addSchema(schema_, dbtab);
                                dbEnv.setDatabseType(dbProperties.get("databaseType"));
                            }
                        }
//                    if (!environmentSet.contains(environmentName)) {
                        if (envreloadType.equals("Fresh Reload")) {
                            try {
                                reqStat = systemManagerUtil.updateEnvironmentMetadata(dbEnv, DBProperties.Operation.UPDATE_APPEND, false);
                                message.append("\n " + reqStat.getStatusMessage() + "\n");
                            } catch (Exception e) {
                                e.printStackTrace();
                                LOGGER.error(e);
                                StringWriter exception = new StringWriter();
                                e.printStackTrace(new PrintWriter(exception));
                                exceptionLog.append("Exception In insertDataIntoAMM() \n" + exception.toString());
                                exceptionLog.append("\n ================================");
                            }
                        } else {
                            try {
                                reqStat = systemManagerUtil.updateEnvironmentMetadata(dbEnv, DBProperties.Operation.UPDATE_APPEND, false);
                                message.append("\n " + reqStat.getStatusMessage() + "\n");
                            } catch (Exception e) {
                                e.printStackTrace();
                                LOGGER.error(e);
                                StringWriter exception = new StringWriter();
                                e.printStackTrace(new PrintWriter(exception));
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
        } catch (Exception e) {
            LOGGER.error(message);
            e.printStackTrace();
            StringWriter exception = new StringWriter();
            e.printStackTrace(new PrintWriter(exception));
//            exceptionLog.append("Exception In insertDataIntoAMM() \n" + exception.toString());
//            exceptionLog.append("\n ================================");
        }
        return message.toString();
    }

    public static void deleteSystemAndEnv(String systemName, SystemManagerUtil systemManagerUtil, String environmentName) {
        try {
            int systemId = systemManagerUtil.getSystem(systemName).getSystemId();
            int evId = systemManagerUtil.getEnvironmentId(systemId, environmentName);
            systemManagerUtil.deleteEnvironment(evId);
        } catch (Exception e) {
            // e.printStackTrace();
            LOGGER.error(e);
            StringWriter exception = new StringWriter();
            e.printStackTrace(new PrintWriter(exception));
            exceptionLog.append("Exception In deleteSystemAndEnv() \n" + exception.toString());
            exceptionLog.append("\n ================================");
        }
    }

    public static Map<String, DBColumn> getValidColumns(Map<String, DBColumn> columnMap) {
        Map<String, DBColumn> updatedcolumnMap = new HashMap<String, DBColumn>();
        try {

            if (columnMap != null) {
                for (Map.Entry<String, DBColumn> entry : columnMap.entrySet()) {
                    String columnName = entry.getKey();
                    DBColumn dbColumn = entry.getValue();
                    if (dbColumn.isValid()) {
                        updatedcolumnMap.put(columnName, dbColumn);
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error(e);
            StringWriter exception = new StringWriter();
            e.printStackTrace(new PrintWriter(exception));
            exceptionLog.append("Exception In getValidColumns() \n" + exception.toString());
            exceptionLog.append("\n ================================");
        }
        return updatedcolumnMap;
    }

    public static int metadataSystemCreation(String systemName, SystemManagerUtil systemManagerUtil,
            String metaDataDeleteType, String environmentName) {
        int systemId = 0;
        try {
            if (systemManagerUtil.getSystemId(systemName) == 0) {
                systemId = createSystem(systemName, systemManagerUtil);
            } else if ("Versioning".equalsIgnoreCase(metaDataDeleteType)) {
                systemId = systemManagerUtil.getSystemId(systemName);
            } else if ("Fresh Reload".equalsIgnoreCase(metaDataDeleteType)) {
                systemId = systemManagerUtil.getSystemId(systemName);
            }
        } catch (Exception e) {
            LOGGER.error(e);
            StringWriter exception = new StringWriter();
            e.printStackTrace(new PrintWriter(exception));
            exceptionLog.append("Exception In metadataSystemCreation() \n" + exception.toString());
            exceptionLog.append("\n ================================");
        }
        return systemId;
    }

    public static int createSystem(String systemName, SystemManagerUtil systemManagerUtil) {
        int systemId = -1;
        try {
            SMSystem system = new SMSystem();
            system.setSystemName(systemName);
            RequestStatus req = systemManagerUtil.createSystem(system);
            systemId = systemManagerUtil.getSystemId(systemName) == 0 ? systemId : systemManagerUtil.getSystemId(systemName);
        } catch (Exception e) {
            LOGGER.error(e);
            StringWriter exception = new StringWriter();
            e.printStackTrace(new PrintWriter(exception));
            exceptionLog.append("Exception In createSystem() \n" + exception.toString());
            exceptionLog.append("\n ================================");
        }
        return systemId;
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
            LOGGER.info("Message " + requestStatus.getStatusMessage());
            message.append("Message " + requestStatus.getStatusMessage());
            environmentId = systemManagerUtil.getEnvironmentId(systemId, environentName);
        } catch (Exception e) {
            LOGGER.error(e);
            StringWriter exception = new StringWriter();
            e.printStackTrace(new PrintWriter(exception));
            exceptionLog.append("Exception In createEnvironment() \n" + exception.toString());
            exceptionLog.append("\n ================================");
        }
        return environmentId;
    }

    private static Map<String, String> getSystem_env_Properties() {
        Map<String, String> map = new HashMap();
        map.put("databaseType", "SqlServer");
        map.put("environmentusername", "UserName");
        map.put("environmentpassword", "password");
        map.put("databasename", "PowerBI");
        map.put("databaserdriver", "com.microsoft.sqlserver.jdbc.SQLServerDriver");
        map.put("databaseurl", "jdbc:sqlserver://qe:1433;databaseName=PowerBI");
        map.put("databaseport", "9999");
        map.put("ip", "pbi");
//        map.put("Filelocation", bucketName);
        return map;
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
            message.append("\n " + msg + "\n");
        } catch (Exception e) {
            LOGGER.error(e);
            StringWriter exception = new StringWriter();
            e.printStackTrace(new PrintWriter(exception));
            exceptionLog.append("Exception In deleteTableOnVersioning() \n" + exception.toString());
            exceptionLog.append("\n ================================");
        }
    }
    
    public static void deleteTable(SystemManagerUtil smutil, int environmentId, String tableName){
        try{
        int tableID=smutil.getTableId(environmentId, tableName);
        if(tableID>0){
            smutil.deleteTable(tableID);
        }
        }catch(Exception ex){
            LOGGER.error(ex);
        }
    }

    public static void deletTable(SystemManagerUtil systemManagerUtil, int envid, String tableName) {
        try {
            //get the tableid
            int tableId = systemManagerUtil.getTableId(envid, tableName);
            if (tableId > 0) {
                //delete the table
                systemManagerUtil.deleteTable(tableId);
            }
        } catch (Exception e) {
            LOGGER.error(e);
            StringWriter exception = new StringWriter();
            e.printStackTrace(new PrintWriter(exception));
//            exceptionLog.append("Exception In deletTable() " + tableName + "\n" + exception.toString());
//            exceptionLog.append("\n ================================");
        }
    }
}
