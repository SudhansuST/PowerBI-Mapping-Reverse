/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.connectors.pbi.map.sql.util;

import com.ads.api.beans.common.Node;
import com.ads.api.beans.mm.Mapping;
import com.ads.api.beans.mm.MappingSpecificationRow;
import com.ads.api.beans.mm.Subject;
import com.ads.api.util.SystemManagerUtil;
import com.erwin.cfx.connectors.json.syncup.SyncupWithServerDBSchamaSysEnvCPT;
import com.erwin.cfx.connectors.pbi.map.pojo.PowerBI_Bean;
import com.erwin.cfx.connectors.pbi.map.util.PowerBI_MappingArrangement;
import com.erwin.cfx.connectors.pbi.map.v11.PBITDataModelFileReader;
import static com.erwin.cfx.connectors.pbi.map.v11.PBITDataModelFileReader.columnsAgainstLogicalTable;
import com.erwin.cfx.connectors.pbi.map.v11.PowerBIReportParser;
import com.erwin.cfx.connectors.pbi.map.util.ReadXMLFileToObject;
import com.erwin.cfx.connectors.pbi.map.util.SyncMetadataJsonFileDesign;
import com.erwin.cfx.connectors.sqlparser.v3.MappingCreator;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.json.JSONObject;

/**
 *
 * @author Sudhansu Tarai Date 24th Nov 2021
 */
public class PowerBI_SQLUtil {

    PowerBI_Bean bean;
    public Set<String> extreamResultSet = new HashSet<>();
    HashMap<String, String> cacheMap = new HashMap<String, String>();
    ArrayList<MappingSpecificationRow> overallSpecs = new ArrayList<>();
    LinkedHashMap<String, HashSet<String>> keyValuesDeailsMap = null;
    String extractedSQL_Queries = "";
    Map<String, LinkedHashMap<String, HashSet<String>>> keyValuesAgainstLogicalTable = new HashMap<>();
    Map<String, ArrayList<MappingSpecificationRow>> mappingSpecificationAgainstLogicalViews = new HashMap<>();
    Set<String> extractedQueries = new HashSet<>();
    Map<String, String> sqlTables = new HashMap<>();
    Map<String, String> keyValuePairForEon = new HashMap<>();
    Map<String, ArrayList<MappingSpecificationRow>> specificationsAgainstTable = new HashMap<>();
    ArrayList<MappingSpecificationRow> dataflowMapSpecs = new ArrayList<>();
    public Set<String> traverse = new HashSet<>();

//    public static void main(String[] args) {
//        String query = "SELECT EMP_NAME, EMP_ID FROM EMPLOYEE";
//        MappingCreator mappingCreator = new MappingCreator();
////        String json = mappingCreator.getMappingObjectToJsonForSSIS(query, "SYS", "ENV", 0, "mssql", "EMPLOYEE", 0);
//        String json = mappingCreator.getMappingObjectToJsonForReport(query, "SYS", "ENV", 0, "mssql", "MAP-1", 0);
//        System.out.println(json);
//
//    }
    public int getMainPhysicalSubject(PowerBI_Bean bean) {
        int subjectID = -1;
        try {
            subjectID = bean.getMappingManagerUtil().getSubjectId(bean.getPARENT_SUBJECT_ID(), Node.NodeType.MM_SUBJECT, bean.getPHYSICAL_SUBJECT());
            if (subjectID <= 0) {
                Subject subject = new Subject();
                subject.setProjectId(Integer.parseInt(bean.getPROJECT_ID()));
                subject.setParentSubjectId(bean.getPARENT_SUBJECT_ID());
                subject.setSubjectName(bean.getPHYSICAL_SUBJECT());
                bean.getMappingManagerUtil().createSubject(subject);
                subjectID = bean.getMappingManagerUtil().getSubjectId(bean.getPARENT_SUBJECT_ID(), Node.NodeType.MM_SUBJECT, bean.getPHYSICAL_SUBJECT());
            }
        } catch (Exception ex) {

        }
        return subjectID;

    }

    public void getSpecificationForSnowflakeQuery(String queries, PowerBI_Bean bean) {

        for (String query_temp : queries.split("@erwin@")) {
            String logicalTableName = query_temp.split("#erwin#")[1];
            String query = query_temp.split("#erwin#")[0];
            String dbType = getDBType(bean, logicalTableName);
            String serverName = getServerName(bean, logicalTableName);
            String databaseName = getDatabaseName(bean, logicalTableName);

            getMappingSpecifications(bean, query, logicalTableName, dbType, serverName, databaseName, 0);
            extractedSQL_Queries = extractedSQL_Queries + logicalTableName + "<br>" + query + "<br>";
            getMergedQuerySpecification(logicalTableName);
            if (!overallSpecs.isEmpty()) {
//                overallSpecs = removeSpecialCharactersFromSpecs(overallSpecs);
                int parentSubjectId = getMainPhysicalSubject(bean);
                createMapping(bean, logicalTableName, parentSubjectId);
                overallSpecs.clear();
            }
        }

    }

    public void summeriseSQLTable(ArrayList<MappingSpecificationRow> specs) {
        for (MappingSpecificationRow spec : specs) {
            try {
                String sourceTab = spec.getSourceTableName().split("\\.")[1];
                sqlTables.put(sourceTab.trim().toUpperCase(), spec.getSourceTableName().trim().toUpperCase());
            } catch (Exception ex) {

            }
        }
    }

    public ArrayList<MappingSpecificationRow> checkSQLSpecs(ArrayList<MappingSpecificationRow> specs) {
        ArrayList<MappingSpecificationRow> updatedSpecs = new ArrayList<>();
        for (MappingSpecificationRow spec : specs) {
            try {
                if (sqlTables.containsKey(spec.getSourceTableName().split("\\.")[1].toUpperCase())) {
                    String sourceTab = sqlTables.get(spec.getSourceTableName().split("\\.")[1].toUpperCase());
                    spec.setSourceTableName(sourceTab);
                }
            } catch (Exception ex) {

            }
            updatedSpecs.add(spec);
        }
        return updatedSpecs;
    }

    public ArrayList<MappingSpecificationRow> removeSpecialCharactersFromSpecs(ArrayList<MappingSpecificationRow> specs) {
        ArrayList<MappingSpecificationRow> updatedSpecs = new ArrayList<>();
        for (MappingSpecificationRow spec : specs) {
            if (spec.getSourceSystemName().toUpperCase().trim().equals("PBI_NEW")) {
                spec.setSourceSystemName(bean.getSQL_SYSTEM_NAME());
                spec.setSourceSystemEnvironmentName(bean.getSQL_ENVIRONMENT_NAME());
            }

            if (spec.getTargetSystemName().toUpperCase().trim().equals("PBI_NEW")) {
                spec.setTargetSystemName(bean.getSQL_SYSTEM_NAME());
                spec.setTargetSystemEnvironmentName(bean.getSQL_ENVIRONMENT_NAME());
            }
            if (spec.getBusinessRule().equals("") && spec.getTargetColumnName().equals("")) {
                continue;
            }
            updatedSpecs.add(spec);

        }

        return updatedSpecs;

    }

    public ArrayList<MappingSpecificationRow> processFilesSpecs(ArrayList<MappingSpecificationRow> specs, String physicalTableName) {
        ArrayList<MappingSpecificationRow> updatedSpecs = new ArrayList<>();
        for (MappingSpecificationRow spec : specs) {
            try {
                if (spec.getSourceTableName().split("\\.")[1].toUpperCase().equals(physicalTableName.split("\\.")[1].toUpperCase())) {
                    spec.setSourceTableName(physicalTableName);
                }
            } catch (Exception ex) {

            }
            if (spec.getSourceTableName().toUpperCase().contains("REMOVED DUPLICATES")) {
                continue;
            }
            if (spec.getSourceTableName().toUpperCase().contains("CHANGED TYPE4")) {
                continue;
            }
            updatedSpecs.add(spec);
        }
        return updatedSpecs;
    }

    public void createMappingSpecificationsForDataflow(String sourceTableName, String sourceType, String targetTableName, String targetType, Set<String> columns) {
        if (bean.getDataflowUtil().queryAgainstEntityName.containsKey(sourceTableName.trim())) {
            String temp = bean.getDataflowUtil().queryAgainstEntityName.get(sourceTableName.trim());
            for (String tab : temp.split("@ERWIN@")) {
                String tableName = tab.split("#ERWIN#")[0];
                String type = tab.split("#ERWIN#")[1];
                if (traverse.contains(tableName)) {
                    continue;
                } else {
                    traverse.add(tableName);
                }
                createMappingSpecificationsForDataflow(tableName, type, sourceTableName, sourceType, columns);
            }
        }

        for (String columnName : columns) {
            MappingSpecificationRow row = new MappingSpecificationRow();
            String sourceSystemName = bean.getSQL_SYSTEM_NAME();
            String sourceEnvironmentName = bean.getSQL_ENVIRONMENT_NAME();
            String targetSystemName = bean.getSQL_SYSTEM_NAME();
            String targetEnvironmentName = bean.getSQL_ENVIRONMENT_NAME();

            String metadataInfo = SyncMetadataJsonFileDesign.newmetasync(sourceTableName, "", "", bean.getMetadataJsonPath(), bean.getSQL_SYSTEM_NAME(), bean.getSQL_ENVIRONMENT_NAME(), cacheMap, bean.getAllTablesMap(), bean.getAllDBMap(), "", "", "");
            sourceSystemName = metadataInfo.split("##")[0];
            sourceEnvironmentName = metadataInfo.split("##")[1];
            sourceTableName = metadataInfo.split("##")[2];
            metadataInfo = SyncMetadataJsonFileDesign.newmetasync(targetTableName, "", "", bean.getMetadataJsonPath(), bean.getSQL_SYSTEM_NAME(), bean.getSQL_ENVIRONMENT_NAME(), cacheMap, bean.getAllTablesMap(), bean.getAllDBMap(), "", "", "");
            targetSystemName = metadataInfo.split("##")[0];
            targetEnvironmentName = metadataInfo.split("##")[1];
            targetTableName = metadataInfo.split("##")[2];

            row.setSourceSystemName(sourceSystemName);
            row.setSourceSystemEnvironmentName(sourceEnvironmentName);
            row.setSourceTableName(sourceTableName);
            row.setTargetSystemName(targetSystemName);
            row.setTargetSystemEnvironmentName(targetEnvironmentName);
            row.setTargetTableName(targetTableName);
            row.setSourceColumnName(columnName);
            row.setTargetColumnName(columnName);

            dataflowMapSpecs.add(row);
        }

    }

    public void createMappingsForSSAS(Map<String, ArrayList<MappingSpecificationRow>> measureSpecsAgainstLogicalTable) {
        int parentSubjectId = getMainPhysicalSubject(bean);
        for (Map.Entry<String, ArrayList<MappingSpecificationRow>> entry : measureSpecsAgainstLogicalTable.entrySet()) {
            String logicalTableName = entry.getKey().split("\\.")[1];
            ArrayList<MappingSpecificationRow> mappingSpecifications = entry.getValue();
            try {
                int mapId = bean.getMappingManagerUtil().getMappingId(parentSubjectId, logicalTableName.split("\\.")[logicalTableName.split("\\.").length - 1], Node.NodeType.MM_SUBJECT);
                Mapping mapping = new Mapping();
                if (mapId > 0) {
                    if (bean.getMAP_RELOAD_TYPE().equalsIgnoreCase("Versioning")) {
                        mapping = PowerBIReportParser.creatingMapVersion(Integer.parseInt(bean.getPROJECT_ID()), logicalTableName.split("\\.")[logicalTableName.split("\\.").length - 1], parentSubjectId, bean.getMappingManagerUtil(), new StringBuffer());
                        bean.getMappingManagerUtil().addMappingSpecifications(mapping.getMappingId(), mappingSpecifications).getStatusMessage();
                        bean.getMappingManagerUtil().updateMapping(mapping);
                    } else {
                        if (PowerBIReportParser.getMappingVersions(parentSubjectId, logicalTableName.split("\\.")[logicalTableName.split("\\.").length - 1], bean.getMappingManagerUtil()).size() != 1) {
                            mapping = PowerBIReportParser.deleteMappingsOnFreshLoad(Integer.parseInt(bean.getPROJECT_ID()), parentSubjectId, logicalTableName.split("\\.")[logicalTableName.split("\\.").length - 1], bean.getMappingManagerUtil());
                            bean.getMappingManagerUtil().addMappingSpecifications(mapping.getMappingId(), mappingSpecifications).getStatusMessage();
                            bean.getMappingManagerUtil().updateMapping(mapping);
                        } else {
                            bean.getMappingManagerUtil().deleteMapping(mapId);
                            mapping = new Mapping();
                            mapping.setMappingName(logicalTableName.split("\\.")[logicalTableName.split("\\.").length - 1]);
                            mapping.setProjectId(Integer.parseInt(bean.getPROJECT_ID()));
                            mapping.setSubjectId(parentSubjectId);
                            mapping.setMappingSpecifications(mappingSpecifications);
                            bean.getMappingManagerUtil().createMapping(mapping);
                        }
                    }
                } else {
                    mapping.setMappingName(logicalTableName.split("\\.")[logicalTableName.split("\\.").length - 1]);
                    mapping.setProjectId(Integer.parseInt(bean.getPROJECT_ID()));
                    mapping.setSubjectId(parentSubjectId);
                    mapping.setMappingSpecifications(mappingSpecifications);
                    bean.getMappingManagerUtil().createMapping(mapping);
                }

            } catch (Exception ex) {

            }
        }

    }

    public void createExtraMappings(Map<String, ArrayList<MappingSpecificationRow>> measureSpecsAgainstLogicalTable, Map<String, String> temp_sqlMap) {
        int parentSubjectId = getMainPhysicalSubject(bean);
        for (Map.Entry<String, ArrayList<MappingSpecificationRow>> entry : measureSpecsAgainstLogicalTable.entrySet()) {
            String logicalTableName = entry.getKey().split("\\.")[1];
            ArrayList<MappingSpecificationRow> mappingSpecifications = entry.getValue();
            if (!temp_sqlMap.containsKey(logicalTableName)) {
                try {
                    int mapId = bean.getMappingManagerUtil().getMappingId(parentSubjectId, logicalTableName.split("\\.")[logicalTableName.split("\\.").length - 1], Node.NodeType.MM_SUBJECT);
                    Mapping mapping = new Mapping();
                    if (mapId > 0) {
                        if (bean.getMAP_RELOAD_TYPE().equalsIgnoreCase("Versioning")) {
                            mapping = PowerBIReportParser.creatingMapVersion(Integer.parseInt(bean.getPROJECT_ID()), logicalTableName.split("\\.")[logicalTableName.split("\\.").length - 1], parentSubjectId, bean.getMappingManagerUtil(), new StringBuffer());
                            bean.getMappingManagerUtil().addMappingSpecifications(mapping.getMappingId(), mappingSpecifications).getStatusMessage();
                            bean.getMappingManagerUtil().updateMapping(mapping);
                        } else {
                            if (PowerBIReportParser.getMappingVersions(parentSubjectId, logicalTableName.split("\\.")[logicalTableName.split("\\.").length - 1], bean.getMappingManagerUtil()).size() != 1) {
                                mapping = PowerBIReportParser.deleteMappingsOnFreshLoad(Integer.parseInt(bean.getPROJECT_ID()), parentSubjectId, logicalTableName.split("\\.")[logicalTableName.split("\\.").length - 1], bean.getMappingManagerUtil());
                                bean.getMappingManagerUtil().addMappingSpecifications(mapping.getMappingId(), mappingSpecifications).getStatusMessage();
                                bean.getMappingManagerUtil().updateMapping(mapping);
                            } else {
                                bean.getMappingManagerUtil().deleteMapping(mapId);
                                mapping = new Mapping();
                                mapping.setMappingName(logicalTableName.split("\\.")[logicalTableName.split("\\.").length - 1]);
                                mapping.setProjectId(Integer.parseInt(bean.getPROJECT_ID()));
                                mapping.setSubjectId(parentSubjectId);
                                mapping.setMappingSpecifications(mappingSpecifications);
                                bean.getMappingManagerUtil().createMapping(mapping);
                            }
                        }
                    } else {
                        mapping.setMappingName(logicalTableName.split("\\.")[logicalTableName.split("\\.").length - 1]);
                        mapping.setProjectId(Integer.parseInt(bean.getPROJECT_ID()));
                        mapping.setSubjectId(parentSubjectId);
                        mapping.setMappingSpecifications(mappingSpecifications);
                        bean.getMappingManagerUtil().createMapping(mapping);
                    }

                } catch (Exception ex) {

                }
            }

        }
    }

    //Calling method
    public void processQuery(PowerBI_Bean bean) {
        this.bean = bean;
        Map<String, String> sqlQueries = this.bean.getSqlQuery();
        Map<String, String> dbTypes = this.bean.getDbTypes();
        Map<String, String> temp_sqlMap = new HashMap<>();
        temp_sqlMap.putAll(sqlQueries);
        boolean check = false;
        Map<String, ArrayList<MappingSpecificationRow>> measureSpecsAgainstLogicalTable = getMeasureSpecifications(bean);
        if (bean.isSSAS()) {
            createMappingsForSSAS(measureSpecsAgainstLogicalTable);
            return;
        }
        createExtraMappings(measureSpecsAgainstLogicalTable, temp_sqlMap);
        for (Map.Entry<String, String> entry : temp_sqlMap.entrySet()) {
            check = false;
            boolean isExecuted = false;
            String logicalTableName = entry.getKey();
            if (logicalTableName.trim().isEmpty()) {
                continue;
            }
            String queries = entry.getValue();
            int increment = 0;
            extractedSQL_Queries = "";
            if (measureSpecsAgainstLogicalTable.containsKey(logicalTableName)) {
                overallSpecs.addAll(measureSpecsAgainstLogicalTable.get(logicalTableName));
            }
            if (logicalTableName.equals("DataModelSchema_Snowflake_Query")) {
                getSpecificationForSnowflakeQuery(PBITDataModelFileReader.snowflakeQuery, bean);
                continue;
            }
            for (String query : queries.split("#ERWIN#")) {
                query = repairQuery(query);
                if (query.toUpperCase().contains("#@ERWIN@#DATAFLOW")) {
                    String entityName = query.split("#@ERWIN@#")[0];
                    Set<String> columns = PBITDataModelFileReader.columnsAgainstLogicalTable.get(logicalTableName);
                    createMappingSpecificationsForDataflow(entityName, "ENTITY", logicalTableName, "LOGICAL", columns);
                    specificationsAgainstTable.put(logicalTableName, dataflowMapSpecs);
                    overallSpecs.addAll(dataflowMapSpecs);
                    getMergedQuerySpecification(entityName);
                    dataflowMapSpecs = new ArrayList<>();
                    check = false;
                    continue;
                }
                if ((query.contains("EXEC ") || query.contains("EXE ")) && !query.contains("SELECT ")) {
                    query = query.replace("EXEC ", "");
                    query = query.replace("EXE ", "");
                    this.bean.getSqlQuery().put(logicalTableName, query + "#@ERWIN@#PROC");
                    continue;
                }

                if (query.toLowerCase().contains(".xlsx") || query.toLowerCase().contains("snowflake_tab") || query.toLowerCase().contains("#@erwin@#view") || query.toLowerCase().contains("#@erwin@#odata_tab") || query.toLowerCase().contains("#@erwin@#temp_tab") || query.toLowerCase().contains("#@erwin@#proc") || query.toLowerCase().contains(".csv") || query.toLowerCase().contains(".json") || query.toLowerCase().contains(".xls") || query.toLowerCase().contains(".xml") || query.toLowerCase().contains(".txt") || query.toLowerCase().contains(".pdf")) {

                    if (columnsAgainstLogicalTable.containsKey(logicalTableName) || PowerBIReportParser.columnsAgainstLogicalTable.containsKey(logicalTableName.toUpperCase())) {
                        String sourceExtractedQuery = prepareMappingForTempTables(bean, logicalTableName, query);
                        getMergedQuerySpecification(logicalTableName);
                        if (!overallSpecs.isEmpty()) {
//                        overallSpecs = removeSpecialCharactersFromSpecs(overallSpecs);
                            int parentSubjectId = getMainPhysicalSubject(bean);
                            createMappingForFiles(bean, logicalTableName, parentSubjectId, query, sourceExtractedQuery);
                            check = true;
                            overallSpecs.clear();
                        }
                        continue;
                    }
                }

                String dbType = getDBType(bean, logicalTableName);
                String serverName = getServerName(bean, logicalTableName);
                String databaseName = getDatabaseName(bean, logicalTableName);
                if (databaseName.equals("EMPTY_DATABASE") || serverName.equals("EMPTY_SERVER")) {
                    serverName = "";
                    databaseName = "";
                }
                isExecuted = true;
                getMappingSpecifications(bean, query, logicalTableName, dbType, serverName, databaseName, increment);
                extractedSQL_Queries = extractedSQL_Queries + logicalTableName + "<br>" + query + "<br>";

            }
            sqlTables.clear();
            summeriseSQLTable(overallSpecs);
            if (!check) {
                getMergedQuerySpecification(logicalTableName);
            }
            if (!overallSpecs.isEmpty()) {
//                overallSpecs = removeSpecialCharactersFromSpecs(overallSpecs);
                int parentSubjectId = getMainPhysicalSubject(bean);
                createMapping(bean, logicalTableName, parentSubjectId);
                overallSpecs.clear();
                keyValuePairForEon.clear();
            }
        }
    }

    public void createMappingForEON(PowerBI_Bean bean, String logicalTableName, int parentSubjectId) {
        try {
            int mapId = bean.getMappingManagerUtil().getMappingId(parentSubjectId, logicalTableName.split("\\.")[logicalTableName.split("\\.").length - 1], Node.NodeType.MM_SUBJECT);
            Mapping mapping = new Mapping();
            if (mapId > 0) {
                if (bean.getMAP_RELOAD_TYPE().equalsIgnoreCase("Versioning")) {
                    mapping = PowerBIReportParser.creatingMapVersion(Integer.parseInt(bean.getPROJECT_ID()), logicalTableName.split("\\.")[logicalTableName.split("\\.").length - 1], parentSubjectId, bean.getMappingManagerUtil(), new StringBuffer());
                } else {
                    if (PowerBIReportParser.getMappingVersions(parentSubjectId, logicalTableName.split("\\.")[logicalTableName.split("\\.").length - 1], bean.getMappingManagerUtil()).size() != 1) {
                        mapping = PowerBIReportParser.deleteMappingsOnFreshLoad(Integer.parseInt(bean.getPROJECT_ID()), parentSubjectId, logicalTableName.split("\\.")[logicalTableName.split("\\.").length - 1], bean.getMappingManagerUtil());
                    } else {
                        bean.getMappingManagerUtil().deleteMapping(mapId);
                        mapping = new Mapping();
                        mapping.setMappingName(logicalTableName.split("\\.")[logicalTableName.split("\\.").length - 1]);
                        mapping.setProjectId(Integer.parseInt(bean.getPROJECT_ID()));
                        mapping.setSubjectId(parentSubjectId);

                    }
                }
            } else {
                mapping.setMappingName(logicalTableName.split("\\.")[logicalTableName.split("\\.").length - 1]);
                mapping.setProjectId(Integer.parseInt(bean.getPROJECT_ID()));
                mapping.setSubjectId(parentSubjectId);
            }
            if (!overallSpecs.isEmpty()) {
                ArrayList<MappingSpecificationRow> updatedSpecs = PowerBIReportParser.removeDuplicateMappingSpecification(overallSpecs, bean.getPOWERBI_REPORT_NAME().toUpperCase());
                ArrayList<MappingSpecificationRow> updatedSpecs1 = removeSpecialCharactersFromSpecs(updatedSpecs);
                ArrayList<MappingSpecificationRow> updatedSpecs2 = checkSQLSpecs(updatedSpecs1);
                mapping.setSourceExtractQuery(extractedSQL_Queries);
                if (mapping.getMappingId() > 0) {
                    bean.getMappingManagerUtil().addMappingSpecifications(mapping.getMappingId(), updatedSpecs2).getStatusMessage();
                    bean.getMappingManagerUtil().updateMapping(mapping);
                } else {
                    mapping.setMappingSpecifications(updatedSpecs2);
                    bean.getMappingManagerUtil().createMapping(mapping);
                }
            }
            mapId = bean.getMappingManagerUtil().getMappingId(parentSubjectId, logicalTableName.split("\\.")[logicalTableName.split("\\.").length - 1], Node.NodeType.MM_SUBJECT);
//            LinkedHashMap<String, HashSet<String>> keyValueMap = keyValuesAgainstLogicalTable.get(logicalTableName);
//            Map<String, String> keyValuePair = new HashMap<>();
//            for (Map.Entry<String, HashSet<String>> entry : keyValueMap.entrySet()) {
//                String key = entry.getKey();
//                HashSet<String> values = entry.getValue();
//                int increment = 0;
//                for (String value : values) {
//                    String newkey = key + "_" + increment;
//                    keyValuePair.put(newkey, value);
//                }
//            }
            bean.getKeyValueUtil().addKeyValueMap(keyValuePairForEon, "8", "" + mapId).getStatusMessage();
        } catch (Exception e) {

        }
    }

    public String prepareMappingForTempTables(PowerBI_Bean bean, String logicalTableName, String query) {

        String sourceExtractSql = "";
        String actualtableName = "";
        String sourceSystemName = "";
        String sourceEnvironmentName = "";
        String sourceSystemName_temp = bean.getSQL_SYSTEM_NAME();
        String sourceEnvironment_temp = bean.getSQL_ENVIRONMENT_NAME();
        Set<String> columns = columnsAgainstLogicalTable.get(logicalTableName);

        if (query.contains("#@ERWIN@#PROC")) {
            actualtableName = query.split("#@ERWIN@#")[0];
            sourceExtractSql = "<div>" + logicalTableName + "</div><div>--------------------------------</div><div>" + actualtableName + " (STORED PROCEDURE)</div>";
            String dbType = "";
            if (bean.getDbTypes().get(logicalTableName).split("\\*ERWIN\\*")[0] == null) {
                dbType = "mssql";
            } else {
                dbType = bean.getDbTypes().get(logicalTableName).split("\\*ERWIN\\*")[0].toLowerCase();
                if (dbType.equalsIgnoreCase("sql")) {
                    dbType = "mssql";
                }
            }
            String databaseName = bean.getDbTypes().get(logicalTableName).split("\\*ERWIN\\*")[2].replaceAll("[^a-zA-Z0-9_\\s]", "_");
            if (databaseName.equals("EMPTY_DATABASE")) {
                databaseName = "";
            }
            String sourceSysEnvInfo1 = SyncMetadataJsonFileDesign.newmetasync(actualtableName, "", "", bean.getMetadataJsonPath(), bean.getSTORED_PROC_SYSTEM_NAME(), databaseName + bean.getSTORED_PROC_ENVIRONMENT_EXTENSION(), cacheMap, bean.getAllTablesMap(), bean.getAllDBMap(), "", "", "");
            sourceSystemName = sourceSysEnvInfo1.split("##")[0];
            bean.setSQL_SYSTEM_NAME(sourceSystemName);
            sourceEnvironmentName = sourceSysEnvInfo1.split("##")[1];
            bean.setSQL_ENVIRONMENT_NAME(sourceEnvironmentName);
        } else if (query.contains("#@ERWIN@#TEMP_TAB")) {
            actualtableName = query.split("#@ERWIN@#")[0];
            String databaseName = "";
            if (bean.getDbTypes() != null && bean.getDbTypes().containsKey(logicalTableName)) {
                databaseName = bean.getDbTypes().get(logicalTableName).split("\\*ERWIN\\*")[2].replaceAll("[^a-zA-Z0-9_\\s]", "_");
            }
            String dbType = getDBType(bean, logicalTableName);
            String serverName = getServerName(bean, logicalTableName);
            if (databaseName.equals("EMPTY_DATABASE")) {
                databaseName = "";
            }
            sourceExtractSql = "<div>" + logicalTableName + "</div><div>--------------------------------</div><div>" + actualtableName + " (VIEW)</div><div>Database Type : " + dbType.toUpperCase() + "</div><div>Server : " + serverName + "</div><div>Database : " + databaseName + "</div>  ";
            String sourceSysEnvInfo1 = SyncMetadataJsonFileDesign.newmetasync(actualtableName, "", "", bean.getMetadataJsonPath(), bean.getSQL_SYSTEM_NAME(), bean.getSQL_ENVIRONMENT_NAME(), cacheMap, bean.getAllTablesMap(), bean.getAllDBMap(), "", "", "");
            sourceSystemName = sourceSysEnvInfo1.split("##")[0];
            bean.setSQL_SYSTEM_NAME(sourceSystemName);
            sourceEnvironmentName = sourceSysEnvInfo1.split("##")[1];
            bean.setSQL_ENVIRONMENT_NAME(sourceEnvironmentName);
        } else if (query.contains("#@ERWIN@#ODATA_TAB")) {
            actualtableName = query.split("#@ERWIN@#")[0];
            String sourceSysEnvInfo1 = SyncMetadataJsonFileDesign.newmetasync(actualtableName, "", "", bean.getMetadataJsonPath(), bean.getODATA_SYSTEM_NAME(), bean.getODATA_ENVIRONEMNT_NAME(), cacheMap, bean.getAllTablesMap(), bean.getAllDBMap(), "", "", "");
            sourceSystemName = sourceSysEnvInfo1.split("##")[0];
            bean.setSQL_SYSTEM_NAME(sourceSystemName);
            sourceEnvironmentName = sourceSysEnvInfo1.split("##")[1];
            bean.setSQL_ENVIRONMENT_NAME(sourceEnvironmentName);
        } else if (query.contains("#@ERWIN@#SNOWFLAKE_TAB")) {
            actualtableName = query.split("#@ERWIN@#")[0];
            String sourceSysEnvInfo1 = SyncMetadataJsonFileDesign.newmetasync(actualtableName, "", "", bean.getMetadataJsonPath(), bean.getSQL_SYSTEM_NAME(), bean.getSQL_ENVIRONMENT_NAME(), cacheMap, bean.getAllTablesMap(), bean.getAllDBMap(), "", "", "");
            sourceSystemName = sourceSysEnvInfo1.split("##")[0];
            bean.setSQL_SYSTEM_NAME(sourceSystemName);
            sourceEnvironmentName = sourceSysEnvInfo1.split("##")[1];
            bean.setSQL_ENVIRONMENT_NAME(sourceEnvironmentName);
        } else if (query.contains("#@ERWIN@#VIEW")) {
            actualtableName = query.split("#@ERWIN@#")[0];
            String sourceSysEnvInfo1 = SyncMetadataJsonFileDesign.newmetasync(actualtableName, "", "", bean.getMetadataJsonPath(), bean.getSQL_SYSTEM_NAME(), bean.getSQL_ENVIRONMENT_NAME(), cacheMap, bean.getAllTablesMap(), bean.getAllDBMap(), "", "", "");
            sourceSystemName = sourceSysEnvInfo1.split("##")[0];
            bean.setSQL_SYSTEM_NAME(sourceSystemName);
            sourceEnvironmentName = sourceSysEnvInfo1.split("##")[1];
            bean.setSQL_ENVIRONMENT_NAME(sourceEnvironmentName);
        } else {
            actualtableName = query.split("\\.")[0];
            String sourceSysEnvInfo1 = SyncMetadataJsonFileDesign.newmetasync(actualtableName.toUpperCase(), "", "", bean.getMetadataJsonPath(), bean.getSQL_SYSTEM_NAME(), bean.getSQL_ENVIRONMENT_NAME(), cacheMap, bean.getAllTablesMap(), bean.getAllDBMap(), "", "", "");
            sourceSystemName = sourceSysEnvInfo1.split("##")[0];
            bean.setSQL_SYSTEM_NAME(sourceSystemName);
            sourceEnvironmentName = sourceSysEnvInfo1.split("##")[1];
            bean.setSQL_ENVIRONMENT_NAME(sourceEnvironmentName);
        }

        for (String columnName : columns) {

            MappingSpecificationRow row = getMappingSpecificationRow(bean.getSQL_SYSTEM_NAME(), bean.getSQL_ENVIRONMENT_NAME(), actualtableName, columnName, sourceSystemName_temp, sourceEnvironment_temp, logicalTableName, columnName);
            overallSpecs.add(row);
        }
        bean.setSQL_SYSTEM_NAME(sourceSystemName_temp);
        bean.setSQL_ENVIRONMENT_NAME(sourceEnvironment_temp);
        return sourceExtractSql;
    }

    public MappingSpecificationRow getMappingSpecificationRow(String sourceSystemName, String sourceEnvironmentName, String sourceTableName, String sourceColumnName, String targetSystemName, String targetEnvironmentName, String targetTableName, String targetColumnName) {

        MappingSpecificationRow row = new MappingSpecificationRow();
        row.setSourceSystemName(sourceSystemName);
        row.setSourceSystemEnvironmentName(sourceEnvironmentName);
        row.setSourceTableName(sourceTableName);
        row.setSourceColumnName(sourceColumnName);
        row.setTargetSystemName(targetSystemName);
        row.setTargetSystemEnvironmentName(targetEnvironmentName);
        row.setTargetTableName(targetTableName);
        row.setTargetColumnName(targetColumnName);
        return row;

    }

    public String getSourceExtractedSQL(String query, String logicalTableName) {
        String sourceExtractSQLQuery = "";
        String databaseName = "";
        String serverName = "";
        String tableName = "";
        String dbType = "";
        sourceExtractSQLQuery = sourceExtractSQLQuery + logicalTableName.split("\\.")[logicalTableName.split("\\.").length - 1] + "<br>------------------<br>";

        try {
            dbType = getDBType(bean, logicalTableName);
            serverName = getServerName_SES(bean, logicalTableName);
            databaseName = getDatabaseName_SES(bean, logicalTableName);
            sourceExtractSQLQuery = sourceExtractSQLQuery + "<div>Database Type : " + dbType.toUpperCase() + "</div><div>Server : " + serverName + "</div><div>Database : " + databaseName + "</div>  ";
        } catch (Exception ex) {

        }
        if (query.contains("#@ERWIN@#PROC")) {
            tableName = query.split("#@ERWIN@#")[0];
            sourceExtractSQLQuery = sourceExtractSQLQuery + "PROCEDURE NAME : " + tableName;
        } else if (query.contains("#@ERWIN@#VIEW")) {
            tableName = query.split("#@ERWIN@#")[0];
            sourceExtractSQLQuery = sourceExtractSQLQuery + "VIEW NAME : " + tableName;
        } else if (query.contains("#@ERWIN@#ODATA_TAB")) {
            tableName = query.split("#@ERWIN@#")[0];
            sourceExtractSQLQuery = sourceExtractSQLQuery + "ODATA TABLE NAME : " + tableName;
        } else if (query.contains("#@ERWIN@#TEMP_TAB")) {
            tableName = query.split("#@ERWIN@#")[0];
            sourceExtractSQLQuery = sourceExtractSQLQuery + "VIEW TABLE NAME : " + tableName;
        } else {
            tableName = query.split("#@ERWIN@#")[0];
            sourceExtractSQLQuery = sourceExtractSQLQuery + "TABLE NAME : " + tableName.toUpperCase();
        }
        return sourceExtractSQLQuery;
    }

    public void createMappingForFiles(PowerBI_Bean bean, String logicalTableName, int parentSubjectId, String extractedQuery, String sourceExtractedQuery) {
        try {
            sourceExtractedQuery = getSourceExtractedSQL(extractedQuery, logicalTableName);
            int mapId = bean.getMappingManagerUtil().getMappingId(parentSubjectId, logicalTableName.split("\\.")[logicalTableName.split("\\.").length - 1], Node.NodeType.MM_SUBJECT);
            Mapping mapping = new Mapping();
            if (mapId > 0) {
                if (bean.getMAP_RELOAD_TYPE().equalsIgnoreCase("Versioning")) {
                    mapping = PowerBIReportParser.creatingMapVersion(Integer.parseInt(bean.getPROJECT_ID()), logicalTableName.split("\\.")[logicalTableName.split("\\.").length - 1], parentSubjectId, bean.getMappingManagerUtil(), new StringBuffer());
                } else {
                    if (PowerBIReportParser.getMappingVersions(parentSubjectId, logicalTableName.split("\\.")[logicalTableName.split("\\.").length - 1], bean.getMappingManagerUtil()).size() != 1) {
                        mapping = PowerBIReportParser.deleteMappingsOnFreshLoad(Integer.parseInt(bean.getPROJECT_ID()), parentSubjectId, logicalTableName.split("\\.")[logicalTableName.split("\\.").length - 1], bean.getMappingManagerUtil());
                    } else {
                        bean.getMappingManagerUtil().deleteMapping(mapId);
                        mapping = new Mapping();
                        mapping.setMappingName(logicalTableName.split("\\.")[logicalTableName.split("\\.").length - 1]);
                        mapping.setProjectId(Integer.parseInt(bean.getPROJECT_ID()));
                        mapping.setSubjectId(parentSubjectId);

                    }
                }
            } else {
                mapping.setMappingName(logicalTableName.split("\\.")[logicalTableName.split("\\.").length - 1]);
                mapping.setProjectId(Integer.parseInt(bean.getPROJECT_ID()));
                mapping.setSubjectId(parentSubjectId);
            }
            mapping.setSourceExtractQuery(sourceExtractedQuery);
            if (!overallSpecs.isEmpty()) {
                ArrayList<MappingSpecificationRow> updatedSpecs = PowerBIReportParser.removeDuplicateMappingSpecification(overallSpecs, bean.getPOWERBI_REPORT_NAME().toUpperCase());
                ArrayList<MappingSpecificationRow> updatedSpecs1 = removeSpecialCharactersFromSpecs(updatedSpecs);
                ArrayList<MappingSpecificationRow> updatedSpecs2 = processFilesSpecs(updatedSpecs1, extractedQuery.split("#@ERWIN@#")[0]);
//                ArrayList<MappingSpecificationRow> newspecs = PowerBI_MappingArrangement.removeReportNameAsSchema(updatedSpecs2, bean.getPOWERBI_REPORT_NAME().toUpperCase());
//                mapping.setSourceExtractQuery(extractedSQL_Queries);
                if (mapping.getMappingId() > 0) {
                    bean.getMappingManagerUtil().addMappingSpecifications(mapping.getMappingId(), updatedSpecs2).getStatusMessage();
                    bean.getMappingManagerUtil().updateMapping(mapping);
                } else {
                    mapping.setMappingSpecifications(updatedSpecs2);
                    bean.getMappingManagerUtil().createMapping(mapping);
                }
            }
            mapId = bean.getMappingManagerUtil().getMappingId(parentSubjectId, logicalTableName.split("\\.")[logicalTableName.split("\\.").length - 1], Node.NodeType.MM_SUBJECT);
            HashMap<String, String> keyValuePair = new HashMap<>();
            if (extractedQuery.contains("PROC")) {
                keyValuePair.put("STORED_PROCEDURE", extractedQuery.split("#@ERWIN@#")[0]);
            } else {
                String path = ReadXMLFileToObject.itemPathAndxlPath.get(logicalTableName);
                String key = path.split("#erwin@")[0];
                String value = path.split("#erwin@")[1];
                keyValuePair.put(key, value);
//                String value
            }
            bean.getKeyValueUtil().addKeyValueMap(keyValuePair, "8", "" + mapId).getStatusMessage();
        } catch (Exception e) {

        }
    }

    public void createMapping(PowerBI_Bean bean, String logicalTableName, int parentSubjectId) {
        try {
            int mapId = bean.getMappingManagerUtil().getMappingId(parentSubjectId, logicalTableName.split("\\.")[logicalTableName.split("\\.").length - 1], Node.NodeType.MM_SUBJECT);
            Mapping mapping = new Mapping();
            if (mapId > 0) {
                if (bean.getMAP_RELOAD_TYPE().equalsIgnoreCase("Versioning")) {
                    mapping = PowerBIReportParser.creatingMapVersion(Integer.parseInt(bean.getPROJECT_ID()), logicalTableName.split("\\.")[logicalTableName.split("\\.").length - 1], parentSubjectId, bean.getMappingManagerUtil(), new StringBuffer());
                } else {
                    if (PowerBIReportParser.getMappingVersions(parentSubjectId, logicalTableName.split("\\.")[logicalTableName.split("\\.").length - 1], bean.getMappingManagerUtil()).size() != 1) {
                        mapping = PowerBIReportParser.deleteMappingsOnFreshLoad(Integer.parseInt(bean.getPROJECT_ID()), parentSubjectId, logicalTableName.split("\\.")[logicalTableName.split("\\.").length - 1], bean.getMappingManagerUtil());
                    } else {
                        bean.getMappingManagerUtil().deleteMapping(mapId);
                        mapping = new Mapping();
                        mapping.setMappingName(logicalTableName.split("\\.")[logicalTableName.split("\\.").length - 1]);
                        mapping.setProjectId(Integer.parseInt(bean.getPROJECT_ID()));
                        mapping.setSubjectId(parentSubjectId);

                    }
                }
            } else {
                mapping.setMappingName(logicalTableName.split("\\.")[logicalTableName.split("\\.").length - 1]);
                mapping.setProjectId(Integer.parseInt(bean.getPROJECT_ID()));
                mapping.setSubjectId(parentSubjectId);
            }
            if (!overallSpecs.isEmpty()) {
                ArrayList<MappingSpecificationRow> updatedSpecs = PowerBIReportParser.removeDuplicateMappingSpecification(overallSpecs, bean.getPOWERBI_REPORT_NAME().toUpperCase());
                ArrayList<MappingSpecificationRow> updatedSpecs1 = removeSpecialCharactersFromSpecs(updatedSpecs);
                ArrayList<MappingSpecificationRow> updatedSpecs2 = checkSQLSpecs(updatedSpecs1);
                String serverName = getServerName(bean, logicalTableName);
                String databaseName = getDatabaseName(bean, logicalTableName);
                ArrayList<MappingSpecificationRow> updatedSpecs3 = SyncupWithServerDBSchamaSysEnvCPT.setMetaDataSpec(setMetadataMap1(updatedSpecs2, databaseName, serverName, bean.getMetadataJsonPath(), bean.getSQL_SYSTEM_NAME(), bean.getSQL_ENVIRONMENT_NAME(), bean.getAllDBMap(), bean.getAllTablesMap(), bean.getMetadataMap(), bean.getDEFAULT_SCHEMA(), bean.getSystemManagerUtil()));
                ArrayList<MappingSpecificationRow> newspecs = updateLogicalSystemEnvironment(updatedSpecs, bean.getPOWERBI_REPORT_NAME());
                mapping.setSourceExtractQuery(extractedSQL_Queries);
                if (mapping.getMappingId() > 0) {
                    bean.getMappingManagerUtil().addMappingSpecifications(mapping.getMappingId(), newspecs).getStatusMessage();
                    bean.getMappingManagerUtil().updateMapping(mapping);
                } else {
                    mapping.setMappingSpecifications(newspecs);
                    bean.getMappingManagerUtil().createMapping(mapping);
                }
            }
            mapId = bean.getMappingManagerUtil().getMappingId(parentSubjectId, logicalTableName.split("\\.")[logicalTableName.split("\\.").length - 1], Node.NodeType.MM_SUBJECT);
            LinkedHashMap<String, HashSet<String>> keyValueMap = keyValuesAgainstLogicalTable.get(logicalTableName);
            Map<String, String> keyValuePair = new HashMap<>();
            for (Map.Entry<String, HashSet<String>> entry : keyValueMap.entrySet()) {
                String key = entry.getKey();
                HashSet<String> values = entry.getValue();
                int increment = 0;
                for (String value : values) {
                    String newkey = key + "_" + increment++;
                    if (key.equalsIgnoreCase("JOIN_CONDITION")) {
                        if (value.split("#@ERWIN@#")[1].equalsIgnoreCase("JOIN")) {
                            newkey = newkey + "_INNER_JOIN";
                            value = value.split("#@ERWIN@#")[0];
                        } else {
                            newkey = newkey + "_" + value.split("#@ERWIN@#")[1].toUpperCase();
                            value = value.split("#@ERWIN@#")[0];
                        }
                    }
                    keyValuePair.put(newkey, value);
                }
            }
            bean.getKeyValueUtil().addKeyValueMap(keyValuePair, "8", "" + mapId).getStatusMessage();
        } catch (Exception e) {

        }
    }

    public ArrayList<MappingSpecificationRow> updateLogicalSystemEnvironment(ArrayList<MappingSpecificationRow> specifications, String reportName) {

        ArrayList<MappingSpecificationRow> updatedSpecification = new ArrayList<>();
        for (MappingSpecificationRow specification : specifications) {
            if (specification.getSourceTableName().toUpperCase().contains(reportName.toUpperCase() + ".")) {
                specification.setSourceSystemName(bean.getPOWERBI_SYSTEM());
                specification.setSourceSystemEnvironmentName(bean.getPOWERBI_ENV_LOGICAL());
            }
            if (specification.getTargetTableName().toUpperCase().contains(reportName.toUpperCase() + ".")) {
                specification.setTargetSystemName(bean.getPOWERBI_SYSTEM());
                specification.setTargetSystemEnvironmentName(bean.getPOWERBI_ENV_LOGICAL());
            }
            updatedSpecification.add(specification);
        }
        return updatedSpecification;
    }

    public void getMergedQuerySpecification(String logicalTable) {
        if (ReadXMLFileToObject.MergeQuerySpecs.get(logicalTable) != null || ReadXMLFileToObject.MergeQuerySpecs.get(logicalTable.toUpperCase()) != null) {
            if (ReadXMLFileToObject.MergeQuerySpecs.get(logicalTable) == null) {
                overallSpecs.addAll(ReadXMLFileToObject.MergeQuerySpecs.get(logicalTable.split("\\.")[logicalTable.split("\\.").length - 1].toUpperCase()));
            } else {
                overallSpecs.addAll(ReadXMLFileToObject.MergeQuerySpecs.get(logicalTable));
            }
        }
    }

    public ArrayList<MappingSpecificationRow> removeDuplicateSpecifications(ArrayList<MappingSpecificationRow> mappingSpecs) {
        ArrayList<MappingSpecificationRow> updatedSpecs = new ArrayList<>();
        Set<String> container = new HashSet<>();
        for (MappingSpecificationRow mappingSpec : mappingSpecs) {
            if (!mappingSpec.getSourceTableName().replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim().toUpperCase().contains("TABLE.SELECTCOLUMNS") && !mappingSpec.getSourceTableName().replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim().toUpperCase().contains("ODATA.FEED") && !mappingSpec.getSourceTableName().replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim().toUpperCase().contains("TABLE.SELECTROWS") && !mappingSpec.getSourceTableName().replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim().toUpperCase().contains("FILTERED ROWS") && !mappingSpec.getSourceTableName().replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim().toUpperCase().contains("SPLIT COLUMN BY DELIMITER") && !mappingSpec.getSourceTableName().replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim().toUpperCase().contains("TABLE.TRANSFORMCOLUMNS") && !mappingSpec.getSourceTableName().replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim().toUpperCase().contains(".EXPANDED ") && !mappingSpec.getSourceTableName().replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim().toUpperCase().contains("RENAMED COLUMNS") && !mappingSpec.getSourceTableName().replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim().toUpperCase().contains("ADDED CUSTOM") && !mappingSpec.getSourceTableName().replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim().toUpperCase().contains("DUPLICATED COLUMN") && !mappingSpec.getSourceTableName().replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim().toUpperCase().contains("MERGED ") && !mappingSpec.getSourceTableName().replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim().toUpperCase().contains("REMOVED COLUMNS") && !mappingSpec.getSourceTableName().replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim().toUpperCase().contains("REORDERED COLUMNS") && !mappingSpec.getSourceTableName().replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim().toUpperCase().contains("NEWCOLUMN") && !mappingSpec.getSourceTableName().replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim().toUpperCase().contains("REPLACED")) {
                if (!mappingSpec.getSourceSystemName().isEmpty() && !mappingSpec.getSourceSystemEnvironmentName().isEmpty() && !mappingSpec.getSourceTableName().isEmpty() && !mappingSpec.getSourceColumnName().isEmpty() && !mappingSpec.getTargetSystemName().isEmpty() && !mappingSpec.getTargetSystemEnvironmentName().isEmpty() && !mappingSpec.getTargetTableName().isEmpty() && !mappingSpec.getTargetColumnName().isEmpty()) {
                    updatedSpecs.add(mappingSpec);
                }
            }
        }

        return updatedSpecs;
    }

    public void updateMappingSpecification_EON(PowerBI_Bean bean, HashMap<String, Set<String>> extreamTarget, ArrayList<MappingSpecificationRow> rows, String logicalTableName) {
        Set<String> tarversedTable = new HashSet<String>();
        for (MappingSpecificationRow row : rows) {
            if (extreamTarget.containsKey(row.getTargetTableName().trim())) {
                Set<String> columns = extreamTarget.get(row.getTargetTableName().trim());
                if (!tarversedTable.contains(row.getTargetTableName().trim())) {
                    tarversedTable.add(row.getTargetTableName().trim());
                    for (String column : columns) {
                        try {
                            MappingSpecificationRow msr = new MappingSpecificationRow();
                            msr.setSourceSystemName(row.getTargetSystemName());
                            msr.setSourceSystemEnvironmentName(row.getTargetSystemEnvironmentName());
                            msr.setSourceTableName(row.getTargetTableName());
                            msr.setSourceColumnName(column);
                            msr.setTargetSystemName(bean.getSQL_SYSTEM_NAME());
                            msr.setTargetSystemEnvironmentName(bean.getSQL_ENVIRONMENT_NAME());
                            msr.setTargetTableName(logicalTableName);
                            msr.setTargetColumnName(column);
                            overallSpecs.add(msr);
                        } catch (Exception ex) {
                            ex.printStackTrace();
                        }
                    }
                }
            }
        }

    }

    public ArrayList<MappingSpecificationRow> syncMappingSpecification(ArrayList<MappingSpecificationRow> mappingSpecification, String mappingJSON, String reportName, String serverName, String databaseName) {
        HashMap<Object, Object> inputsMap = new HashMap<>();
//        inputsMap.put("json", mappingJSON);
        inputsMap.put("inputSpecsLists", mappingSpecification);
        inputsMap.put("databasename", databaseName);
        inputsMap.put("servername", serverName);
        inputsMap.put("jsonFilePath", bean.getMetadataJsonPath());
        inputsMap.put("defSysName", bean.getSQL_SYSTEM_NAME());
        inputsMap.put("defEnvName", bean.getSQL_ENVIRONMENT_NAME());
        inputsMap.put("cacheMap", cacheMap);
        inputsMap.put("allDBMap", bean.getAllDBMap());
        inputsMap.put("allTablesMap", bean.getAllTablesMap());
        inputsMap.put("delimiter", "@erwin@");
        inputsMap.put("storeProcDelimeter", "");
        inputsMap.put("systemManagerUtil", bean.getSystemManagerUtil());
        inputsMap.put("storeProcedureTableFlag", "");
        ArrayList<MappingSpecificationRow> updatedSpecification = SyncupWithServerDBSchamaSysEnvCPT.setMetaDataSpec(inputsMap);

        return updatedSpecification;
    }

    public void getMappingSpecifications(PowerBI_Bean bean, String query, String logicalTableName, String dbType, String serverName, String databaseName, int increment) {
        String json = "";
        MappingCreator mappingCreator = new MappingCreator();//Object creation for Map Object
        if (databaseName.contains("SSAS") || serverName.contains("SSAS")) {
            json = mappingCreator.getMappingObjectToJsonForSSIS(query, serverName, databaseName, 0, dbType, logicalTableName.split("\\.")[logicalTableName.split("\\.").length - 1], 0);
        } else {
            json = mappingCreator.getMappingObjectToJsonForSSIS(query, bean.getSQL_SYSTEM_NAME(), bean.getSQL_ENVIRONMENT_NAME(), 0, dbType, logicalTableName.split("\\.")[logicalTableName.split("\\.").length - 1], 0);
        }
        if (!json.equals("") && json != null) {

            ArrayList<MappingSpecificationRow> mapspecrows = null;
            if (databaseName.contains("SSAS") || serverName.contains("SSAS")) {
                mapspecrows = SyncupWithServerDBSchamaSysEnvCPT.setMetaDataSpec(setMetadataMap(json, databaseName, serverName, "", serverName, databaseName, bean.getAllDBMap(), bean.getAllTablesMap(), bean.getMetadataMap(), bean.getDEFAULT_SCHEMA(), bean.getSystemManagerUtil()));
            } else {
                mapspecrows = SyncupWithServerDBSchamaSysEnvCPT.setMetaDataSpec(setMetadataMap(json, databaseName, serverName, bean.getMetadataJsonPath(), bean.getSQL_SYSTEM_NAME(), bean.getSQL_ENVIRONMENT_NAME(), bean.getAllDBMap(), bean.getAllTablesMap(), bean.getMetadataMap(), bean.getDEFAULT_SCHEMA(), bean.getSystemManagerUtil()));
            }
            if (!mapspecrows.isEmpty()) {
                keyValuesDeailsMap = MappingCreator.keyValuesDeailsMap;
                if (keyValuesAgainstLogicalTable.containsKey(logicalTableName)) {
                    LinkedHashMap<String, HashSet<String>> tempKeyValue = keyValuesAgainstLogicalTable.get(logicalTableName);
                    for (Map.Entry<String, HashSet<String>> entry : keyValuesDeailsMap.entrySet()) {
                        String key = entry.getKey();
                        HashSet<String> val = entry.getValue();
                        if (tempKeyValue.containsKey(key)) {
                            HashSet<String> valuees = tempKeyValue.get(key);
                            valuees.addAll(val);
                            tempKeyValue.put(key, valuees);
                        } else {
                            tempKeyValue.put(key, val);
                        }
                    }
                    keyValuesAgainstLogicalTable.put(logicalTableName, tempKeyValue);
                } else {
                    keyValuesAgainstLogicalTable.put(logicalTableName, keyValuesDeailsMap);
                }
                extreamResultSet.clear();
                getExtreamResultSet(mapspecrows);
                updateQueryMapSpec(mapspecrows, logicalTableName, bean);
                overallSpecs.addAll(mapspecrows);
            }

        }
    }

    public void updateQueryMapSpec(ArrayList<MappingSpecificationRow> mapSpecs, String ObjectName, PowerBI_Bean bean) {
        ArrayList<MappingSpecificationRow> extraSpecs = new ArrayList<>();
        try {
            for (int i = 0; i < mapSpecs.size(); i++) {
                MappingSpecificationRow eachSpec = mapSpecs.get(i);
                if (extreamResultSet.contains(eachSpec.getTargetTableName())) {
                    MappingSpecificationRow spec = new MappingSpecificationRow();
                    spec.setSourceSystemName(eachSpec.getTargetSystemName());
                    spec.setSourceSystemEnvironmentName(eachSpec.getTargetSystemEnvironmentName());
                    spec.setSourceTableName(eachSpec.getTargetTableName());
                    spec.setSourceColumnName(eachSpec.getTargetColumnName());
                    spec.setTargetSystemName(bean.getPOWERBI_SYSTEM());
                    spec.setTargetSystemEnvironmentName(bean.getPOWERBI_ENV_LOGICAL());
                    spec.setTargetTableName(ObjectName);
                    spec.setTargetColumnName(eachSpec.getTargetColumnName());
                    extraSpecs.add(spec);
                } else if (eachSpec.getTargetTableName().contains("#@NIWRE@#")) {
//                    String sourceSysEnvInfo1 = SyncMetadataJsonFileDesign.newmetasync(ObjectName, "", "", bean.getMetadataJsonPath(), bean.getSQL_SYSTEM_NAME(), bean.getSQL_ENVIRONMENT_NAME(), cacheMap, bean.getAllTablesMap(), bean.getAllDBMap(), "", "", "");
//                    String sourceSystemName_temp = sourceSysEnvInfo1.split("##")[0];
//                    String sourceEnvironmentName_temp = sourceSysEnvInfo1.split("##")[1];
                    eachSpec.setTargetTableName(ObjectName);
                    eachSpec.setTargetSystemName(bean.getPOWERBI_SYSTEM());
                    eachSpec.setTargetSystemEnvironmentName(bean.getPOWERBI_ENV_LOGICAL());

                }
            }
        } catch (Exception e) {
//            logger.error(e.getMessage());
//            StringWriter exception = new StringWriter();
//            e.printStackTrace(new PrintWriter(exception));
//            exceptionLog.append("Exception In updateQueryMapSpec() \n" + exception.toString());
//            exceptionLog.append("\n ================================");
        }

        mapSpecs.addAll(extraSpecs);

    }

    public void getExtreamResultSet(ArrayList<MappingSpecificationRow> mapspecs) {
        extreamResultSet.clear();
        for (MappingSpecificationRow mapspec : mapspecs) {

            extreamResultSet.add(mapspec.getTargetTableName().trim());

        }

        for (MappingSpecificationRow mapspec : mapspecs) {
            if (extreamResultSet.contains(mapspec.getSourceTableName().trim())) {
                extreamResultSet.remove(mapspec.getSourceTableName().trim());
            }
        }

    }

    public HashMap setMetadataMap(String mappingJSON, String databaseName, String serverName, String metadataJSONPath, String defSystemName, String defEnvironmentName, Map allDBMap, Map allTablesMap, Map metadatChacheHM, String defSchema, SystemManagerUtil systemManagerUtil) {
        HashMap inputsMap = new HashMap();
        inputsMap.put("json", mappingJSON);
        inputsMap.put("databasename", databaseName);
        inputsMap.put("servername", serverName);
        inputsMap.put("jsonFilePath", metadataJSONPath);
        inputsMap.put("defSysName", defSystemName);
        inputsMap.put("defEnvName", defEnvironmentName);
        inputsMap.put("cacheMap", metadatChacheHM);
        inputsMap.put("allDBMap", allDBMap);
        inputsMap.put("allTablesMap", allTablesMap);
        inputsMap.put("defSchema", defSchema);
        inputsMap.put("delimiter", "@erwin@");
        inputsMap.put("intermediateCompSet", MappingCreator.intermediateComponents);
        inputsMap.put("storeProcDelimeter", "");
        inputsMap.put("systemManagerUtil", systemManagerUtil);
        inputsMap.put("storeProcedureTableFlag", "");

        return inputsMap;
    }

    public HashMap setMetadataMap1(ArrayList<MappingSpecificationRow> inputSpecsLists, String databaseName, String serverName, String metadataJSONPath, String defSystemName, String defEnvironmentName, Map allDBMap, Map allTablesMap, Map metadatChacheHM, String defSchema, SystemManagerUtil systemManagerUtil) {
        HashMap inputsMap = new HashMap();
        inputsMap.put("inputSpecsLists", inputSpecsLists);
        inputsMap.put("databasename", databaseName);
        inputsMap.put("servername", serverName);
        inputsMap.put("jsonFilePath", metadataJSONPath);
        inputsMap.put("defSysName", defSystemName);
        inputsMap.put("defEnvName", defEnvironmentName);
        inputsMap.put("cacheMap", metadatChacheHM);
        inputsMap.put("allDBMap", allDBMap);
        inputsMap.put("allTablesMap", allTablesMap);
        inputsMap.put("defSchema", defSchema);
        inputsMap.put("delimiter", "@erwin@");
        inputsMap.put("intermediateCompSet", MappingCreator.intermediateComponents);
        inputsMap.put("storeProcDelimeter", "");
        inputsMap.put("systemManagerUtil", systemManagerUtil);
        inputsMap.put("storeProcedureTableFlag", "");

        return inputsMap;
    }

    public String getServerName(PowerBI_Bean bean, String logicalTableName) {
        if (bean.getDbTypes() != null && bean.getDbTypes().containsKey(logicalTableName) && bean.getDbTypes().get(logicalTableName).split("\\*ERWIN\\*")[1] != null) {
            return bean.getDbTypes().get(logicalTableName).split("\\*ERWIN\\*")[1].replaceAll("[^a-zA-Z0-9_\\s]", "_");
        } else {
            return "";
        }
    }

    public String getServerName_SES(PowerBI_Bean bean, String logicalTableName) {
        if (bean.getDbTypes() != null && bean.getDbTypes().containsKey(logicalTableName) && bean.getDbTypes().get(logicalTableName).split("\\*ERWIN\\*")[1] != null) {
            return bean.getDbTypes().get(logicalTableName).split("\\*ERWIN\\*")[1];
        } else {
            return "";
        }
    }

    public String getDatabaseName(PowerBI_Bean bean, String logicalTableName) {
        if (bean.getDbTypes() != null && bean.getDbTypes().containsKey(logicalTableName) && bean.getDbTypes().get(logicalTableName).split("\\*ERWIN\\*")[2] != null) {
            return bean.getDbTypes().get(logicalTableName).split("\\*ERWIN\\*")[2].replaceAll("[^a-zA-Z0-9_\\s]", "_");
        } else {
            return "";
        }
    }

    public String getDatabaseName_SES(PowerBI_Bean bean, String logicalTableName) {
        if (bean.getDbTypes() != null && bean.getDbTypes().containsKey(logicalTableName) && bean.getDbTypes().get(logicalTableName).split("\\*ERWIN\\*")[2] != null) {
            return bean.getDbTypes().get(logicalTableName).split("\\*ERWIN\\*")[2];
        } else {
            return "";
        }
    }

    public String getDBType(PowerBI_Bean bean, String logicalTableName) {
        String dbType = "";
        if (bean.getDbTypes() != null && bean.getDbTypes().containsKey(logicalTableName) && bean.getDbTypes().get(logicalTableName).split("\\*ERWIN\\*")[0] == null) {
            dbType = "mssql";
        } else if (bean.getDbTypes() != null && bean.getDbTypes().containsKey(logicalTableName)) {
            dbType = bean.getDbTypes().get(logicalTableName).split("\\*ERWIN\\*")[0].toLowerCase();
            if (dbType.equalsIgnoreCase("sql")) {
                dbType = "mssql";
            }
        }

        return dbType;
    }

    public String repairQuery(String query) {
        if (query.contains(", CommandTimeout=#duration")) {
            query = query.replace(query.substring(query.indexOf(", CommandTimeout=#duration"), query.length()), "");
        }
        query = query.replace("\\u0027", "'");
        query = query.replace("\\u003e", "> ");
        query = query.replace("\\u003c", "< ");
        query = query.replace("$", " ");
        query = query.replace(", CommandTimeout=#duration(0, 0, 45, 0)", "");
        query = query.replace(", HierarchicalNavigation=true", "");
        query = query.replace(", CreateNavigationProperties=false", "");
        query = query.replace(", Implementation=2.0", "");
        query = query.replace(", CommandTimeout=#duration(0, 0, 30, 0)", "");
        query = query.replace("INSERT INTO @responseText (ResponseText)", "");
        query = query.replace(", CommandTimeout=#duration(0, 2, 0, 0)", "");
        query = query.replace(", CommandTimeout=#duration(0, 0, 4, 0)", "");
        query = query.replace(", CommandTimeout=#duration(0, 0, 15, 0)", "");
        query = query.replace(", CommandTimeout=#duration(0, 0, 10, 0)", "");
        query = query.replace(", CommandTimeout=#duration(0, 0, 20, 0)", "");
        query = query.replace("WITH (HEAP, DISTRIBUTION = ROUND_ROBIN)", "");
        query = query.replace("with (heap, distribution = hash(ShawID))", "");
        query = query.replace("with (heap, distribution = hash(User_Id))", "");
        query = query.replace("with (heap, distribution = hash(CollectorID))", "");
        query = query.replace(", MultiSubnetFailover=true", "");
        query = query.replace("With (Distribution = HASH(fundeddateyearmonth)", "");
        query = query.replace("With (Distribution = HASH(reportdate)", "");
        query = query.replace("With (Distribution = HASH(FacilityName)", "");
        query = query.replace("With (Distribution = HASH(vintage)", "");
        query = query.replace(",Clustered Columnstore Index)", "");
        query = query.replace(",Unpvt2.FactForecast", "");
        query = query.replace("AND f.ClosingDate = CASE WHEN f.FacilityName = 'EART 2017-1' THEN DATEADD(dd,-1,filedate) else adv.FileDate END", "");
        query = query.replace("-SUM(RecoveryTotalAmount) AS RecoveryTotal", "");
        return query;
    }

    public Map<String, ArrayList<MappingSpecificationRow>> getMeasureSpecifications(PowerBI_Bean bean) {
        Map<String, ArrayList<MappingSpecificationRow>> measureSpecsAgainstLogicalTable = new HashMap<>();
        Pattern pattern = Pattern.compile("\\'[a-zA-Z0-9\\s\\_\\-\\&\\'\\(\\)]*\\[.*?\\]");
        Pattern pattern1 = Pattern.compile("[\\'a-zA-Z0-9\\s\\_\\-\\&\\']+\\[.+?\\]");
        Map<String, String> expressionAgainstMeasureMap = bean.getExpressionAgainstMeasureMap();
        for (Map.Entry<String, String> entry : expressionAgainstMeasureMap.entrySet()) {
            String key = entry.getKey();
            if (key.contains("DateTable") || key.contains("LocalDateTable")) {
                continue;
            }
            String value = entry.getValue();
            String targetTable = key.split("@ERWIN@")[0];
            String targetColumnName = key.split("@ERWIN@")[1];
            ArrayList<MappingSpecificationRow> specifications = new ArrayList<>();
            Matcher matcher = pattern.matcher(value);
            if (matcher.matches()) {
                while (matcher.find()) {
                    MappingSpecificationRow row = new MappingSpecificationRow();
                    String matchString = matcher.group();
                    matchString = matchString.replace("'", "");
                    String sourceTableName = matchString.split("\\[")[0].trim();
                    String sourceColumnName = matchString.substring(matchString.indexOf("[") + 1, matchString.indexOf("]"));
                    if (bean.isSSAS()) {
                        targetTable = targetTable.replace(bean.getPOWERBI_REPORT_NAME() + ".", "");
                        row.setSourceSystemName(bean.getSSAS_SYSTEM_NAME());
                        row.setSourceSystemEnvironmentName(bean.getSSAS_ENVIRONMENT_NAME());
                        row.setSourceTableName(sourceTableName);
                        row.setSourceColumnName(sourceColumnName);
                        row.setTargetSystemName(bean.getSSAS_SYSTEM_NAME());
                        row.setSourceSystemEnvironmentName(bean.getSSAS_ENVIRONMENT_NAME());
                        row.setTargetTableName(targetTable.split("\\.")[1]);
                        row.setTargetColumnName(targetColumnName);
                        row.setBusinessRule(value);
                    } else {
                        row.setSourceSystemName(bean.getPOWERBI_SYSTEM());
                        row.setSourceSystemEnvironmentName(bean.getPOWERBI_ENV_LOGICAL());
                        row.setSourceTableName(bean.getPOWERBI_REPORT_NAME() + "." + sourceTableName.toUpperCase());
                        row.setSourceColumnName(sourceColumnName);
                        row.setTargetSystemName(bean.getPOWERBI_SYSTEM());
                        row.setTargetSystemEnvironmentName(bean.getPOWERBI_ENV_LOGICAL());
                        row.setTargetTableName(targetTable);
                        row.setTargetColumnName(targetColumnName);
                        row.setBusinessRule(value);
                        PowerBIReportParser.metadataMapCreation(bean.getPOWERBI_SYSTEM(), bean.getPOWERBI_ENV_LOGICAL(), bean.getPOWERBI_REPORT_NAME() + "." + sourceTableName.toUpperCase(), sourceColumnName);
                        PowerBIReportParser.metadataMapCreation(bean.getPOWERBI_SYSTEM(), bean.getPOWERBI_ENV_LOGICAL(), targetTable, targetColumnName);
                    }
                    specifications.add(row);
                }
            } else {
                Matcher matcher1 = pattern1.matcher(value);
                while (matcher1.find()) {
                    MappingSpecificationRow row = new MappingSpecificationRow();
                    String matchString = matcher1.group();
                    matchString = matchString.replace("'", "");
                    String sourceTableName = matchString.split("\\[")[0].trim();
                    if (sourceTableName.replaceAll("[^\\w\\*]", "").equals("")) {
                        continue;
                    }
                    String sourceColumnName = matchString.substring(matchString.indexOf("[") + 1, matchString.indexOf("]"));
                    if (bean.isSSAS()) {
                        targetTable = targetTable.replace(bean.getPOWERBI_REPORT_NAME() + ".", "");
                        row.setSourceSystemName(bean.getSSAS_SYSTEM_NAME());
                        row.setSourceSystemEnvironmentName(bean.getSSAS_ENVIRONMENT_NAME());
                        row.setSourceTableName(sourceTableName);
                        row.setSourceColumnName(sourceColumnName);
                        row.setTargetSystemName(bean.getSSAS_SYSTEM_NAME());
                        row.setSourceSystemEnvironmentName(bean.getSSAS_ENVIRONMENT_NAME());
                        row.setTargetTableName(targetTable.split("\\.")[1]);
                        row.setTargetColumnName(targetColumnName);
                        row.setBusinessRule(value);
                    } else {
                        row.setSourceSystemName(bean.getPOWERBI_SYSTEM());
                        row.setSourceSystemEnvironmentName(bean.getPOWERBI_ENV_LOGICAL());
                        row.setSourceTableName(bean.getPOWERBI_REPORT_NAME() + "." + sourceTableName.toUpperCase());
                        row.setSourceColumnName(sourceColumnName);
                        row.setTargetSystemName(bean.getPOWERBI_SYSTEM());
                        row.setTargetSystemEnvironmentName(bean.getPOWERBI_ENV_LOGICAL());
                        row.setTargetTableName(targetTable);
                        row.setTargetColumnName(targetColumnName);
                        row.setBusinessRule(value);
                        PowerBIReportParser.metadataMapCreation(bean.getPOWERBI_SYSTEM(), bean.getPOWERBI_ENV_LOGICAL(), bean.getPOWERBI_REPORT_NAME() + "." + sourceTableName.toUpperCase(), sourceColumnName);
                        PowerBIReportParser.metadataMapCreation(bean.getPOWERBI_SYSTEM(), bean.getPOWERBI_ENV_LOGICAL(), targetTable, targetColumnName);
                    }
                    specifications.add(row);
                }
            }
            if (measureSpecsAgainstLogicalTable.containsKey(targetTable.toUpperCase())) {
                ArrayList<MappingSpecificationRow> mapSpecs = measureSpecsAgainstLogicalTable.get(targetTable.toUpperCase());
                mapSpecs.addAll(specifications);
                measureSpecsAgainstLogicalTable.put(targetTable.toUpperCase(), mapSpecs);
            } else {
                measureSpecsAgainstLogicalTable.put(targetTable.toUpperCase(), specifications);
            }
            measureSpecsAgainstLogicalTable = PowerBI_MappingArrangement.removeDuplicates1(measureSpecsAgainstLogicalTable, bean.getPOWERBI_REPORT_NAME(), bean.isSSAS());
        }
        measureSpecsAgainstLogicalTable = PowerBI_MappingArrangement.removeDuplicates(measureSpecsAgainstLogicalTable, bean.getPOWERBI_REPORT_NAME(), bean.isSSAS());
        return measureSpecsAgainstLogicalTable;
    }

}
