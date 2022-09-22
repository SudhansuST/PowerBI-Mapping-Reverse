// 
// Decompiled by Procyon v0.5.36
// 
package com.erwin.cfx.connectors.pbi.map.util;

import java.io.FileWriter;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.List;
import org.json.simple.JSONObject;
import java.io.Reader;
import java.io.FileReader;
import java.io.File;
import org.json.simple.parser.JSONParser;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import com.ads.api.beans.mm.Mapping;
import java.util.HashSet;
import org.codehaus.jackson.map.ObjectMapper;
import com.ads.api.beans.mm.MappingSpecificationRow;
import java.util.ArrayList;
import java.util.HashMap;

public class SyncMetadataJsonFileDesign {

    public static StringBuilder log;

    public static ArrayList<MappingSpecificationRow> setMetaDataSpec(String json, String ssisdatabaseName, String ssisserverName, final String jsonFilePath, final String defSysName, final String defEnvName, final HashMap cacheMap, final HashMap<String, String> allTablesMap, final HashMap<String, String> allDBMap, final String defSchema, final String filePath) {
        ArrayList<MappingSpecificationRow> finalMapSPecsLists = null;
        try {
            ssisdatabaseName = ssisdatabaseName.toUpperCase();
            ssisserverName = ssisserverName.toUpperCase();
            ssisdatabaseName = ssisdatabaseName.trim();
            ssisserverName = ssisserverName.trim();
            final ObjectMapper mapper = new ObjectMapper();
            finalMapSPecsLists = new ArrayList<MappingSpecificationRow>();
            final Set<String> removeDuplicate = new HashSet<String>();
            json = json.replace(",\"childNodes\":[]", "");
            final Mapping mapObj = (Mapping) mapper.readValue(json, (Class) Mapping.class);
            final ArrayList<MappingSpecificationRow> mapSPecsLists = (ArrayList<MappingSpecificationRow>) mapObj.getMappingSpecifications();
            final String mapName = mapObj.getMappingName();
            for (final MappingSpecificationRow mapSPecRow : mapSPecsLists) {
                try {
                    String sourcetableName = mapSPecRow.getSourceTableName();
                    sourcetableName = sourcetableName.replace("[", "").replace("]", "");
                    String targetTableName = mapSPecRow.getTargetTableName();
                    targetTableName = targetTableName.replace("[", "").replace("]", "");
                    String querySourceServerName = "";
                    String querySourceDatabaseName = "";
                    String querySourceSchemaName = "";
                    String querySourceTableName = "";
                    String queryTargetServerName = "";
                    String queryTargetDatabaseName = "";
                    String queryTargetSchemaName = "";
                    String queryTargetTableName = "";
                    final ArrayList<String> sourceSystemList = new ArrayList<String>();
                    final ArrayList<String> sourceEnvironmentList = new ArrayList<String>();
                    final ArrayList<String> sourceTableList = new ArrayList<String>();
                    final ArrayList<String> targetSystemList = new ArrayList<String>();
                    final ArrayList<String> targetEnvironmentList = new ArrayList<String>();
                    final ArrayList<String> targetTableList = new ArrayList<String>();
                    if (!StringUtils.isBlank((CharSequence) sourcetableName)) {
                        final ArrayList<String> sourceDetailedTableNameList = getTableName(sourcetableName, defSchema);
                        for (final String sourceDetailedTableName : sourceDetailedTableNameList) {
                            try {
                                querySourceServerName = sourceDetailedTableName.split("erwinseprator")[0];
                                querySourceDatabaseName = sourceDetailedTableName.split("erwinseprator")[1];
                                querySourceSchemaName = sourceDetailedTableName.split("erwinseprator")[2];
                                querySourceTableName = sourceDetailedTableName.split("erwinseprator")[3];
                                if (querySourceDatabaseName.equals("")) {
                                    String useLineDataBaseName = "";
                                    if (ssisdatabaseName.contains("separatorUseLine")) {
                                        try {
                                            useLineDataBaseName = ssisdatabaseName.split("separatorUseLine")[1];
                                        } catch (Exception e) {
                                            e.printStackTrace();
                                        }
                                        if (!StringUtils.isBlank((CharSequence) useLineDataBaseName)) {
                                            querySourceDatabaseName = useLineDataBaseName;
                                        }
                                    } else {
                                        querySourceDatabaseName = ssisdatabaseName;
                                    }
                                }
                                if (querySourceServerName.equals("")) {
                                    querySourceServerName = ssisserverName;
                                }
                                final String sourcesystemEnvironment = newmetasync(querySourceTableName.trim().toUpperCase(), querySourceDatabaseName, querySourceServerName, jsonFilePath, defSysName, defEnvName, cacheMap, allTablesMap, allDBMap, mapName, querySourceSchemaName, defSchema);
                                sourceSystemList.add(sourcesystemEnvironment.split("##")[0]);
                                sourceEnvironmentList.add(sourcesystemEnvironment.split("##")[1]);
                                sourceTableList.add(sourcesystemEnvironment.split("##")[2]);
                            } catch (Exception ex2) {
                            }
                        }
                    }
                    if (!StringUtils.isBlank((CharSequence) targetTableName)) {
                        final ArrayList<String> targetDetailedTableNameList = getTableName(targetTableName, defSchema);
                        for (final String targetDetailedTableName : targetDetailedTableNameList) {
                            try {
                                queryTargetServerName = targetDetailedTableName.split("erwinseprator")[0];
                                queryTargetDatabaseName = targetDetailedTableName.split("erwinseprator")[1];
                                queryTargetSchemaName = targetDetailedTableName.split("erwinseprator")[2];
                                queryTargetTableName = targetDetailedTableName.split("erwinseprator")[3];
                                if (queryTargetDatabaseName.equals("")) {
                                    String useLineDataBaseName = "";
                                    if (ssisdatabaseName.contains("separatorUseLine")) {
                                        try {
                                            useLineDataBaseName = ssisdatabaseName.split("separatorUseLine")[1];
                                        } catch (Exception e) {
                                            e.printStackTrace();
                                        }
                                        if (!StringUtils.isBlank((CharSequence) useLineDataBaseName)) {
                                            queryTargetDatabaseName = useLineDataBaseName;
                                        }
                                    } else {
                                        queryTargetDatabaseName = ssisdatabaseName;
                                    }
                                }
                                if (queryTargetServerName.equals("")) {
                                    queryTargetServerName = ssisserverName;
                                }
                                final String targetsystemEnvironment = newmetasync(queryTargetTableName.trim().toUpperCase(), queryTargetDatabaseName, queryTargetServerName, jsonFilePath, defSysName, defEnvName, cacheMap, allTablesMap, allDBMap, mapName, queryTargetSchemaName, defSchema);
                                targetSystemList.add(targetsystemEnvironment.split("##")[0]);
                                targetEnvironmentList.add(targetsystemEnvironment.split("##")[1]);
                                targetTableList.add(targetsystemEnvironment.split("##")[2]);
                            } catch (Exception ex3) {
                            }
                        }
                    }
                    mapSPecRow.setSourceTableName(StringUtils.join((Iterable) sourceTableList, "\n"));
                    mapSPecRow.setSourceSystemName(StringUtils.join((Iterable) sourceSystemList, "\n"));
                    mapSPecRow.setSourceSystemEnvironmentName(StringUtils.join((Iterable) sourceEnvironmentList, "\n"));
                    mapSPecRow.setTargetTableName(StringUtils.join((Iterable) targetTableList, "\n"));
                    mapSPecRow.setTargetSystemName(StringUtils.join((Iterable) targetSystemList, "\n"));
                    mapSPecRow.setTargetSystemEnvironmentName(StringUtils.join((Iterable) targetEnvironmentList, "\n"));
                    removeDublicate(mapSPecRow, finalMapSPecsLists, removeDuplicate);
                } catch (Exception ex4) {
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return finalMapSPecsLists;
    }

    public static ArrayList<String> getTableName(String inputTableName, final String defaultSchema) {
        String serverName = "";
        String databaseName = "";
        String schemaName = "";
        String tableName = "";
        String returnValue = "";
        final ArrayList<String> returnValueList = new ArrayList<String>();
        try {
            final String[] inputTableNameArr = inputTableName.split("\n");
            for (int i = 0; i < inputTableNameArr.length; ++i) {
                serverName = "";
                databaseName = "";
                schemaName = "";
                tableName = "";
                returnValue = "";
                inputTableName = inputTableNameArr[i];
                final ArrayList<String> tablePartsList = getTablePartsList(inputTableName);
                if (tablePartsList.size() >= 4) {
                    serverName = tablePartsList.get(0);
                    databaseName = tablePartsList.get(1);
                    schemaName = tablePartsList.get(2);
                    tableName = tablePartsList.get(3);
                } else if (tablePartsList.size() >= 3) {
                    databaseName = tablePartsList.get(0);
                    schemaName = tablePartsList.get(1);
                    tableName = tablePartsList.get(2);
                } else if (tablePartsList.size() >= 2) {
                    schemaName = tablePartsList.get(0);
                    tableName = tablePartsList.get(1);
                } else if (tablePartsList.size() >= 1) {
                    tableName = tablePartsList.get(0);
                }
                if (!"".equals(tableName.trim()) && !StringUtils.isBlank((CharSequence) schemaName)) {
                    returnValue = serverName + "erwinseprator" + databaseName + "erwinseprator" + schemaName + "erwinseprator" + schemaName + "." + tableName;
                } else {
                    returnValue = serverName + "erwinseprator" + databaseName + "erwinseprator" + schemaName + "erwinseprator" + tableName;
                }
                returnValue = returnValue.replace("[", "").replace("]", "");
                returnValueList.add(returnValue);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return returnValueList;
    }

    public static ArrayList<String> getTableNameOld(String inputTableName, final String defaultSchema) {
        String serverName = "";
        String databaseName = "";
        String schemaName = "";
        String tableName = "";
        String returnValue = "";
        final ArrayList<String> returnValueList = new ArrayList<String>();
        try {
            final String[] inputTableNameArr = inputTableName.split("\n");
            for (int i = 0; i < inputTableNameArr.length; ++i) {
                serverName = "";
                databaseName = "";
                schemaName = "";
                tableName = "";
                returnValue = "";
                inputTableName = inputTableNameArr[i];
                if (inputTableName.contains("].[")) {
                    final String[] tableStringArry = inputTableName.split("\\]\\.\\[");
                    if (tableStringArry.length == 4) {
                        serverName = tableStringArry[0];
                        databaseName = tableStringArry[1];
                        schemaName = tableStringArry[2];
                        if (StringUtils.isBlank((CharSequence) schemaName)) {
                            schemaName = defaultSchema;
                        }
                        tableName = tableStringArry[3];
                    } else if (tableStringArry.length == 3) {
                        databaseName = tableStringArry[0];
                        schemaName = tableStringArry[1];
                        tableName = tableStringArry[2];
                        try {
                            if (tableName.contains("].")) {
                                serverName = databaseName;
                                databaseName = schemaName;
                                schemaName = tableName.split("\\]\\.")[0];
                                tableName = tableName.split("\\]\\.")[1];
                            }
                        } catch (Exception ex) {
                        }
                        if (StringUtils.isBlank((CharSequence) schemaName)) {
                            schemaName = defaultSchema;
                        }
                    } else if (tableStringArry.length == 2) {
                        schemaName = tableStringArry[0];
                        tableName = tableStringArry[1];
                        try {
                            if (tableName.contains("].")) {
                                databaseName = schemaName;
                                schemaName = tableName.split("\\]\\.")[0];
                                if (StringUtils.isBlank((CharSequence) schemaName)) {
                                    schemaName = defaultSchema;
                                }
                                tableName = tableName.split("\\]\\.")[1];
                            }
                        } catch (Exception ex2) {
                        }
                    }
                } else if (inputTableName.contains("[") && inputTableName.contains("]") && inputTableName.contains(".")) {
                    tableName = inputTableName;
                    try {
                        if (tableName.contains("].")) {
                            schemaName = tableName.split("\\]\\.")[0];
                            tableName = tableName.split("\\]\\.")[1];
                        } else if (tableName.contains(".[") && tableName.split("\\.\\[").length == 2) {
                            schemaName = tableName.split("\\.\\[")[0];
                            tableName = tableName.split("\\.\\[")[1];
                        }
                    } catch (Exception ex3) {
                    }
                } else {
                    final String[] tableStringArry = inputTableName.split("\\.");
                    if (tableStringArry.length == 4) {
                        serverName = tableStringArry[0];
                        databaseName = tableStringArry[1];
                        schemaName = tableStringArry[2];
                        if (StringUtils.isBlank((CharSequence) schemaName)) {
                            schemaName = defaultSchema;
                        }
                        tableName = tableStringArry[3];
                    } else if (tableStringArry.length == 3) {
                        databaseName = tableStringArry[0];
                        schemaName = tableStringArry[1];
                        if (StringUtils.isBlank((CharSequence) schemaName)) {
                            schemaName = defaultSchema;
                        }
                        tableName = tableStringArry[2];
                    } else if (tableStringArry.length == 2) {
                        schemaName = tableStringArry[0];
                        tableName = tableStringArry[1];
                    } else {
                        tableName = inputTableName;
                    }
                }
                if (!"".equals(tableName.trim()) && !StringUtils.isBlank((CharSequence) schemaName)) {
                    returnValue = serverName + "erwinseprator" + databaseName + "erwinseprator" + schemaName + "erwinseprator" + schemaName + "." + tableName;
                } else {
                    returnValue = serverName + "erwinseprator" + databaseName + "erwinseprator" + schemaName + "erwinseprator" + tableName;
                }
                returnValue = returnValue.replace("[", "").replace("]", "");
                returnValueList.add(returnValue);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return returnValueList;
    }

    public static String newmetasync(String tableName, String ssisdatabaseName, String ssisserverName, final String jsonFileDir, final String defSysName, final String defEnvName, final HashMap cacheMap, final HashMap<String, String> allTablesMap, final HashMap<String, String> allDBMap, final String mapName, final String querySchemaName, final String defaultSchema) {
        ssisdatabaseName = ssisdatabaseName.toUpperCase().replaceAll("[^a-zA-Z0-9]", "_");
        ssisserverName = ssisserverName.toUpperCase().replaceAll("[^a-zA-Z0-9]", "_");
        String systemName = "";
        String environementName = "";
        String schemaName = "";
        tableName = tableName.toUpperCase();
        final JSONParser parser = new JSONParser();
        try {
            final ObjectMapper mapper = new ObjectMapper();
            String jsonFilePath = "";
            if (!StringUtils.isBlank((CharSequence) ssisserverName)) {
                jsonFilePath = jsonFileDir + ssisserverName + "_" + ssisdatabaseName + ".json";
            } else {
                jsonFilePath = jsonFileDir + ssisdatabaseName + ".json";
            }
            File jsonFile = new File(jsonFilePath);
            if ((StringUtils.isBlank((CharSequence) ssisserverName) && !StringUtils.isBlank((CharSequence) ssisdatabaseName)) || !jsonFile.exists()) {
                ssisserverName = allDBMap.get(ssisdatabaseName.toUpperCase());
                jsonFilePath = "";
                if (!StringUtils.isBlank((CharSequence) ssisserverName)) {
                    jsonFilePath = jsonFileDir + ssisserverName + "_" + ssisdatabaseName + ".json";
                } else {
                    jsonFilePath = jsonFileDir + ssisdatabaseName + ".json";
                }
                jsonFile = new File(jsonFilePath);
            }
            String sourceSysEnvInfo = allTablesMap.get(tableName);
            if (sourceSysEnvInfo != null && !StringUtils.isBlank((CharSequence) jsonFileDir.trim()) && new File(jsonFileDir).exists()) {
                if (!sourceSysEnvInfo.contains("@ERWIN@")) {
                    final String[] sourceSysEnvInfoArr = sourceSysEnvInfo.split("#");
                    if (sourceSysEnvInfoArr.length == 3) {
                        systemName = sourceSysEnvInfoArr[1];
                        environementName = sourceSysEnvInfoArr[0];
                        schemaName = sourceSysEnvInfoArr[2];
                        tableName = schemaName + "." + tableName;
                    } else if (sourceSysEnvInfoArr.length == 2) {
                        systemName = sourceSysEnvInfoArr[1];
                        environementName = sourceSysEnvInfoArr[0];
                    }
                } else if (!jsonFile.exists()) {
                    if (sourceSysEnvInfo.contains("@ERWIN@")) {
                        sourceSysEnvInfo = sourceSysEnvInfo.split("@ERWIN@")[0];
                    }
                    final String[] sourceSysEnvInfoArr = sourceSysEnvInfo.split("#");
                    if (sourceSysEnvInfoArr.length == 3) {
                        systemName = sourceSysEnvInfoArr[1];
                        environementName = sourceSysEnvInfoArr[0];
                        schemaName = sourceSysEnvInfoArr[2];
                        tableName = schemaName + "." + tableName;
                    } else if (sourceSysEnvInfoArr.length == 2) {
                        systemName = sourceSysEnvInfoArr[1];
                        environementName = sourceSysEnvInfoArr[0];
                    }
                } else {
                    HashMap serverDatabaseMap = (HashMap) cacheMap.get(ssisserverName + "_" + ssisdatabaseName);
                    if (serverDatabaseMap != null) {
                        final List tables = (List) serverDatabaseMap.get("Tables");
                        final List schemaList = (List) serverDatabaseMap.get("Schemas");
                        if (tables.toString().toLowerCase().contains(tableName.toLowerCase())) {
                            systemName = serverDatabaseMap.get("SystemName").toString();
                            environementName = serverDatabaseMap.get("EnvironmentName").toString();
                            if (StringUtils.isBlank((CharSequence) querySchemaName) && schemaList.size() > 0) {
                                tableName = schemaList.get(0) + "." + tableName;
                            }
                        } else {
                            systemName = defSysName;
                            environementName = defEnvName;
                            SyncMetadataJsonFileDesign.log.append("MapName = " + mapName + "\nTableName = " + tableName + "\n");
                        }
                    } else {
                        FileReader fileReader = null;
                        try {
                            fileReader = new FileReader(jsonFile);
                            final Object obj = parser.parse((Reader) fileReader);
                            final JSONObject jsonObject = (JSONObject) obj;
                            serverDatabaseMap = (HashMap) mapper.convertValue((Object) jsonObject, (Class) HashMap.class);
                            cacheMap.put(ssisserverName + "_" + ssisdatabaseName, serverDatabaseMap);
                            final List tables2 = (List) serverDatabaseMap.get("Tables");
                            final List schemaList2 = (List) serverDatabaseMap.get("Schemas");
                            if (tables2.toString().toLowerCase().contains(tableName.toLowerCase())) {
                                systemName = serverDatabaseMap.get("SystemName").toString();
                                environementName = serverDatabaseMap.get("EnvironmentName").toString();
                                if (StringUtils.isBlank((CharSequence) querySchemaName) && schemaList2.size() > 0) {
                                    tableName = schemaList2.get(0) + "." + tableName;
                                }
                            } else {
                                systemName = defSysName;
                                environementName = defEnvName;
                                SyncMetadataJsonFileDesign.log.append("MapName = " + mapName + "\nTableName = " + tableName + "\n");
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            systemName = defSysName;
                            environementName = defEnvName;
                            SyncMetadataJsonFileDesign.log.append("MapName = " + mapName + "\nTableName = " + tableName + "\n");
                        } finally {
                            try {
                                if (fileReader != null) {
                                    fileReader.close();
                                }
                            } catch (Exception ex) {
                            }
                        }
                    }
                }
            } else {
                systemName = defSysName;
                environementName = defEnvName;
                SyncMetadataJsonFileDesign.log.append("MapName = " + mapName + "\nTableName = " + tableName + "\n");
            }
        } catch (Exception e2) {
            e2.printStackTrace();
        }
        tableName = getTableNameDefaults(tableName, systemName, environementName, defSysName, defEnvName, querySchemaName, defaultSchema);
        return systemName + "##" + environementName + "##" + tableName;
    }

    public static String getTableNameDefaults(String inputTableName, final String systemName, final String environementName, final String defSysName, final String defEnvName, final String querySchemaName, final String defaultSchemaName) {
        try {
            if (inputTableName.contains("RESULT_OF_") || inputTableName.contains("INSERT-SELECT") || inputTableName.contains("UPDATE-") || inputTableName.contains("RS") || inputTableName.contains("MERGE") || inputTableName.contains("UNION")) {
                return inputTableName;
            }
            if (systemName.equals(defSysName) && environementName.equals(defEnvName) && StringUtils.isBlank((CharSequence) querySchemaName) && !StringUtils.isBlank((CharSequence) defaultSchemaName)) {
                inputTableName = defaultSchemaName + "." + inputTableName;
            }
        } catch (Exception ex) {
        }
        return inputTableName;
    }

    public static void writeUnsyncTableDataToFile(final String filePath) {
        System.out.println("filePath---" + filePath);
        FileWriter writer = null;
        String fileDate = "";
        try {
            fileDate = new SimpleDateFormat("yyyyMMdd").format(new Date());
            writer = new FileWriter(filePath + "/Failed_Syncuplogs_" + fileDate + ".txt", true);
            writer.write(SyncMetadataJsonFileDesign.log.toString());
        } catch (Exception e) {
//            e.printStackTrace();
            try {
                writer.close();
            } catch (Exception ex) {
//                e.printStackTrace();
            }
        } finally {
            try {
                writer.close();
            } catch (Exception e2) {
//                e2.printStackTrace();
            }
        }
    }

    public static HashMap<String, String> allTablesMap(final String jsonFilePath) {
        HashMap<String, String> allTablesMap = new HashMap<String, String>();
        FileReader fileReader = null;
        try {
            if (!new File(jsonFilePath).exists()) {
                return allTablesMap;
            }
            final JSONParser parser = new JSONParser();
            fileReader = new FileReader(jsonFilePath + "AllTables.json");
            final Object obj = parser.parse((Reader) fileReader);
            final JSONObject jsonObject = (JSONObject) obj;
            final ObjectMapper mapper = new ObjectMapper();
            allTablesMap = (HashMap<String, String>) mapper.convertValue(jsonObject.get((Object) "Tables"), (Class) HashMap.class);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (fileReader != null) {
                    fileReader.close();
                }
            } catch (Exception ex) {
            }
        }
        return allTablesMap;
    }

    public static HashMap<String, String> allDBMap(final String jsonFilePath) {
        HashMap<String, String> allDBMap = new HashMap<String, String>();
        FileReader fileReader = null;
        try {
            if (!new File(jsonFilePath).exists()) {
                return allDBMap;
            }
            final JSONParser parser = new JSONParser();
            fileReader = new FileReader(jsonFilePath + "AllTables.json");
            final Object obj = parser.parse((Reader) fileReader);
            final JSONObject jsonObject = (JSONObject) obj;
            final ObjectMapper mapper = new ObjectMapper();
            allDBMap = (HashMap<String, String>) mapper.convertValue(jsonObject.get((Object) "Databases"), (Class) HashMap.class);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (fileReader != null) {
                    fileReader.close();
                }
            } catch (Exception ex) {
            }
        }
        return allDBMap;
    }

    public static void removeDublicate(final MappingSpecificationRow mapSPecRow, final ArrayList<MappingSpecificationRow> finalMapSPecsLists, final Set<String> removeDuplicate) {
        String sourceTableName = mapSPecRow.getSourceTableName().trim();
        final String sourceColumnName = mapSPecRow.getSourceColumnName().trim();
        String targetTableName = mapSPecRow.getTargetTableName().trim();
        final String targetColumnName = mapSPecRow.getTargetColumnName().trim();
        final String businessRule = mapSPecRow.getBusinessRule();
        if ("".equals(sourceTableName) && !"".equals(businessRule)) {
            sourceTableName = targetTableName;
        } else if (sourceTableName.contains(targetTableName)) {
            targetTableName = "";
        }
        final String stringSpecRow = sourceTableName + "#" + sourceColumnName + "#" + targetTableName + "#" + targetColumnName + "#" + businessRule;
        if (!removeDuplicate.contains(stringSpecRow)) {
            finalMapSPecsLists.add(mapSPecRow);
            removeDuplicate.add(stringSpecRow);
        }
    }

    public static ArrayList<String> getTablePartsList(String inputTableName) {
        final ArrayList<String> list = new ArrayList<String>();
        try {
            while (!inputTableName.equals("")) {
                if (inputTableName.trim().startsWith("[")) {
                    list.add(inputTableName.substring(inputTableName.indexOf("[") + 1, inputTableName.indexOf("]")).trim());
                    if (inputTableName.contains("].")) {
                        inputTableName = inputTableName.substring(inputTableName.indexOf("].") + 2);
                    } else {
                        inputTableName = "";
                    }
                } else if (inputTableName.contains(".")) {
                    list.add(inputTableName.substring(0, inputTableName.indexOf(".")).trim());
                    inputTableName = inputTableName.substring(inputTableName.indexOf(".") + 1);
                } else {
                    list.add(inputTableName.trim());
                    inputTableName = "";
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return list;
    }

    static {
        SyncMetadataJsonFileDesign.log = new StringBuilder();
    }
}
