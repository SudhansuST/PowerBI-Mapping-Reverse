/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.connectors.pbi.map.sql.util;

import com.ads.api.beans.mm.MappingSpecificationRow;
import com.erwin.cfx.connectors.pbi.map.v11.PowerBI_Constanst;
import com.erwin.cfx.connectors.pbi.map.pojo.PowerBI_Bean;
import com.erwin.cfx.connectors.pbi.map.util.PowerBI_LOGGER;
import com.erwin.cfx.connectors.pbi.map.util.ReadXMLFileToObject;
import static com.erwin.cfx.connectors.pbi.map.v11.PBITDataModelFileReader.itemPathAndQueryMap;
import static com.erwin.cfx.connectors.pbi.map.v11.PBITDataModelFileReader.parseMeasuresFromSource;
import static com.erwin.cfx.connectors.pbi.map.v11.PBITDataModelFileReader.parseODBC;
import static com.erwin.cfx.connectors.pbi.map.v11.PBITDataModelFileReader.parseODataFeed;
import static com.erwin.cfx.connectors.pbi.map.v11.PBITDataModelFileReader.parsePowerBI_DAX;
import static com.erwin.cfx.connectors.pbi.map.v11.PBITDataModelFileReader.parseSnowflake;
import static com.erwin.cfx.connectors.pbi.map.v11.PBITDataModelFileReader.parserParameterisedCSVDocument;
import static com.erwin.cfx.connectors.pbi.map.v11.PBITDataModelFileReader.prepareSQLMetadataInfos;
import static com.erwin.cfx.connectors.pbi.map.v11.PBITDataModelFileReader.searchMannualTable;
import static com.erwin.cfx.connectors.pbi.map.v11.PBITDataModelFileReader.searchMannualTable1;
import static com.erwin.cfx.connectors.pbi.map.v11.PBITDataModelFileReader.searchPowerBI_Views;
import static com.erwin.cfx.connectors.pbi.map.v11.PBITDataModelFileReader.search_FlatFiles;
import static com.erwin.cfx.connectors.pbi.map.v11.PBITDataModelFileReader.search_SQLQueries;
import static com.erwin.cfx.connectors.pbi.map.v11.PBITDataModelFileReader.search_StoredProcedures;
import static com.erwin.cfx.connectors.pbi.map.v11.PowerBIReportParser.pbib;
import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 *
 * @author Sudhansu Tarai
 */
public class PowerBI_DataflowUtil {

    public Set<String> tables = new HashSet<>();
    String prefix = "";
    String databaseName = "";
    String warehouse = "";
    String schema = "";
    String serverName = "";
    public Map<String, String> queryAgainstEntityName = new HashMap<>();
    public String dataflowName = "";
    public Map<String, ArrayList<MappingSpecificationRow>> specAgainstEntity = new HashMap<>();
    public Map<String, String> dataflowsAgainstID = new HashMap<>();
    public Map<String, Map<String, String>> tablesMapAgainstID = new HashMap<>();
    public ArrayList<MappingSpecificationRow> specs = new ArrayList<>();
    public String[] sysEnvDetail = {pbib.getSQL_SYSTEM_NAME(), pbib.getSQL_ENVIRONMENT_NAME(), pbib.getSQL_SYSTEM_NAME(), pbib.getSQL_ENVIRONMENT_NAME()};

    public void cleanVariables() {
        tables = new HashSet<>();
        prefix = "";
        databaseName = "";
        warehouse = "";
        schema = "";
        serverName = "";
        dataflowName = "";
        queryAgainstEntityName = new HashMap<>();
    }

    public void parseDataflowJSON(List<String> dataflowDirectory, String fileSeparator) {
        for (String file : dataflowDirectory) {
            File powerBIFile = new File(file);
            String powerBIFilePath = powerBIFile.getPath();
            String powerBIFileName = powerBIFilePath.substring(powerBIFilePath.lastIndexOf(fileSeparator) + 1);
            if (powerBIFileName.contains(".json")) {
                try {
                    String dataflowJSON = FileUtils.readFileToString(powerBIFile);
                    readDataflowJSON(dataflowJSON);
                } catch (Exception ex) {

                }
            }
        }
    }

    public void readDataflowJSON(String dataflowJSON) {
//        cleanVariables();
        try {
            JSONObject dataflow = new JSONObject(dataflowJSON);
            if (dataflow.has(PowerBI_Constanst.DATAFLOW_NODE_NAME)) {
                dataflowName = dataflow.getString(PowerBI_Constanst.DATAFLOW_NODE_NAME);
            } else {
                return;
            }
            if (dataflow.has(PowerBI_Constanst.DATAFLOW_NODE_PBI_MASHUP)) {
                JSONObject mashups = dataflow.getJSONObject(PowerBI_Constanst.DATAFLOW_NODE_PBI_MASHUP);
                if (mashups.has(PowerBI_Constanst.DATAFLOW_MASHUP_QUERY_METADATA)) {
                    JSONObject queryMetadata = mashups.getJSONObject(PowerBI_Constanst.DATAFLOW_MASHUP_QUERY_METADATA);
                    Iterator<String> keyIterator = queryMetadata.keys();
                    while (keyIterator.hasNext()) {
                        String key = keyIterator.next();
                        if (!key.equals(PowerBI_Constanst.DATAFLOW_SHARED_DATABASE) && !key.equals(PowerBI_Constanst.DATAFLOW_SHARED_SCHEMA) && !key.equals(PowerBI_Constanst.DATAFLOW_SHARED_SERVER) && !key.equals(PowerBI_Constanst.DATAFLOW_SHARED_PREFIX) && !key.equals(PowerBI_Constanst.DATAFLOW_SHARED_WAREHOUSE)) {
                            tables.add(key);
                        }
                    }
                }

                if (mashups.has(PowerBI_Constanst.DATAFLOW_MASHUP_DOCUMENTS)) {
                    String mashupDocuments = mashups.getString(PowerBI_Constanst.DATAFLOW_MASHUP_DOCUMENTS);
                    getDataflowMetadataInfo(mashupDocuments);
                    getDataflowEntityInfo(mashupDocuments);
                }
            }

        } catch (Exception ex) {

        }
    }

    public void getDataflowMetadataInfo(String mashupDoc) {
        prefix = "";
        databaseName = "";
        warehouse = "";
        schema = "";
        String tableName = "";
        String entityName = "";
        serverName = "";
        mashupDoc = mashupDoc.replace("section Section1;", "").replace("\r", "");
        for (String data : mashupDoc.split("in\\r\\n")) {
            for (int i = 0; i < data.split("\n").length; i++) {
                String eachline = data.split("\n")[i];
                if (eachline.contains(PowerBI_Constanst.DATAFLOW_DOCUMENT_SHARED)) {

                    if (eachline.contains(PowerBI_Constanst.DATAFLOW_SHARED_DATABASE)) {
                        databaseName = data.split("\n")[i + 1].substring(data.split("\n")[i + 1].indexOf("\"") + 1, data.split("\n")[i + 1].lastIndexOf("\""));
                    }
                    if (eachline.contains(PowerBI_Constanst.DATAFLOW_SHARED_PREFIX)) {
                        prefix = data.split("\n")[i + 1].substring(data.split("\n")[i + 1].indexOf("\"") + 1, data.split("\n")[i + 1].lastIndexOf("\""));
                    }
                    if (eachline.contains(PowerBI_Constanst.DATAFLOW_SHARED_SCHEMA)) {
                        schema = data.split("\n")[i + 1].substring(data.split("\n")[i + 1].indexOf("\"") + 1, data.split("\n")[i + 1].lastIndexOf("\""));
                    }
                    if (eachline.contains(PowerBI_Constanst.DATAFLOW_SHARED_SERVER)) {
                        serverName = data.split("\n")[i + 1].substring(data.split("\n")[i + 1].indexOf("\"") + 1, data.split("\n")[i + 1].lastIndexOf("\""));
                    }
                    if (eachline.contains(PowerBI_Constanst.DATAFLOW_SHARED_WAREHOUSE)) {
                        warehouse = data.split("\n")[i + 1].substring(data.split("\n")[i + 1].indexOf("\"") + 1, data.split("\n")[i + 1].lastIndexOf("\""));
                    } else {
                        tables.contains(eachline.replace(PowerBI_Constanst.DATAFLOW_DOCUMENT_SHARED, "").replace(" = let", "").trim());
                    }

                }
            }
        }

    }

    public void getDataflowEntityInfo(String mashupDoc) {
        String tableName = "";
        String entityName = "";
        mashupDoc = mashupDoc.replace("section Section1;", "");
        for (String data : mashupDoc.split(";\r\n")) {
            for (int i = 0; i < data.split("\n").length; i++) {
                String eachline = data.split("\n")[i];
                if (eachline.contains(PowerBI_Constanst.DATAFLOW_DOCUMENT_SHARED)) {
                    if (tables.contains(eachline.replace(PowerBI_Constanst.DATAFLOW_DOCUMENT_SHARED, "").replace(" = let", "").trim())) {
                        tableName = eachline.replace(PowerBI_Constanst.DATAFLOW_DOCUMENT_SHARED, "").replace(" = let", "").trim();
                        break;
                    }
                }
            }
//                        boolean check = true;
//                        while (check) {

//                            if (!data.contains("Kind = \"Table\"") || !data.contains("Kind = \"View\"") || !data.contains("Table.Combine")) {
//                                if (data.split("\n")[i].contains("Kind = \"Table\"")) {
//                                    tableName = data.split("\n")[i].substring(data.split("\n")[i].indexOf("["), data.split("\n")[i].indexOf(",")).replace("varPrefix", prefix).split("=")[1].replace("\"", "").trim();
//                                    check = false;
//                                    tableName = tableName + "#ERWIN#TEMP_TABLE";
//                                } else if (data.split("\n")[i].contains("Kind = \"View\"")) {
//                                    tableName = data.split("\n")[i].substring(data.split("\n")[i].indexOf("["), data.split("\n")[i].indexOf(",")).replace("varPrefix", prefix).split("=")[1].replace("\"", "").trim();
//                                    check = false;
//                                    tableName = tableName + "#ERWIN#VIEW";
//                                } else if (data.split("\n")[i].contains("Table.Combine")) {
////                                Table.Combine
//                                    String tempData = data.split("\n")[i].substring(data.split("\n")[i].indexOf("{") + 1, data.split("\n")[i].indexOf("}"));
//                                    String temptab = "";
//                                    for (String eachtable : tempData.split(",")) {
//                                        temptab = temptab + (temptab.equals("") ? eachtable + "#ERWIN#" + "ENTITY" : "@ERWIN@" + eachtable + "#ERWIN#" + "ENTITY");
//                                    }
//                                    tableName = temptab;
//                                    check = false;
//                                } else if (data.split("\n")[i].contains("Kind = \"Table\"")) {
//
//                                }
//                            } else 
            if (data.contains("Snowflake.Databases") || data.contains("Bron =") || data.contains("OData.Feed") || data.contains("Source") || data.contains("source") || data.contains("Origine") || data.contains("AnalysisServices")) {

                String expression = data;
                if (expression.contains("Snowflake.Databases") || expression.contains("Bron =") || expression.contains("OData.Feed") || expression.contains("Source") || expression.contains("source") || expression.contains("Origine") || expression.contains("AnalysisServices")) {
                    String replaceItem = "";
                    if (expression.contains("Table.FromRows(Json.Document(Binary.Decompress(Binary.FromText(") || expression.contains("Source = DateTime.LocalNow()")) {
                        ReadXMLFileToObject.itemPathAndxlPath.put(tableName.toUpperCase(), "Manual Table");
                    }
                    if (expression.contains("Origine")) {
                        replaceItem = "Origine";
                    } else if (expression.contains("Bron =")) {
                        replaceItem = "Bron =";
                    } else {
                        if (expression.contains("Source =")) {
                            replaceItem = "Source =";
                        } else {
                            replaceItem = "Source";
                        }
                        try {
                            searchPowerBI_Views(expression, tableName);
                        } catch (Exception inex) {
                            StringWriter exception = new StringWriter();
                            inex.printStackTrace(new PrintWriter(exception));
                            PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Error ::Exception In PBITDataModelFileReader.readDataModelFile() Method " + exception + "\n");
                        }
                    }
                    String schemaName = "";
                    if (expression.contains("Snowflake.Databases")) {
                        parseSnowflake(expression, tableName);
                    }
                    if (expression.contains("Odbc.DataSource")) {
                        parseODBC(expression, tableName);
                    }
                    if (expression.contains("OData.Feed")) {
                        parseODataFeed(expression, tableName);
                    }
                    if (expression.contains("Merged Queries") || expression.contains("[Query") || expression.contains("Expanded ") || expression.contains("#\"Modificato ") || expression.contains("#\"Filtrate ") || expression.contains("#\"Merge ") || expression.toLowerCase().contains("select#(lf)") || expression.toLowerCase().contains("select ") || expression.toLowerCase().contains("with") || expression.toLowerCase().contains("exec ") || expression.toLowerCase().contains("exe ") || expression.toLowerCase().contains(".xlsx") || expression.toLowerCase().contains(".xml") || expression.toLowerCase().contains(".json") || expression.toLowerCase().contains("folder.files") || expression.toLowerCase().contains(".pdf") || expression.toLowerCase().contains(".csv") || expression.toLowerCase().contains(".txt") || expression.toLowerCase().contains(".xls")) {
                        for (String query : expression.split(replaceItem)) {
                            prepareSQLMetadataInfos(query, tableName, schemaName);
                            parsePowerBI_DAX(query, expression, tableName, schemaName, dataflowName, sysEnvDetail);
                            if ((query.toLowerCase().contains("select ") || query.toLowerCase().contains("with ") || query.toLowerCase().contains("select#(tab)") || query.toLowerCase().contains("select#(lf)"))) {
                                search_SQLQueries(query, tableName);
                            } else {
                                String[] splitString = query.split(",");
                                if (query.toLowerCase().contains(".xlsx") || query.toLowerCase().contains(".xml") || query.toLowerCase().contains("folder.files") || query.toLowerCase().contains(".json") || query.toLowerCase().contains(".pdf") || query.toLowerCase().contains(".csv") || query.toLowerCase().contains(".txt") || query.toLowerCase().contains(".xls")) {
                                    for (int index = 0; index < splitString.length; index++) {
                                        if (splitString[index].toLowerCase().contains(".xlsx") || splitString[index].toLowerCase().contains(".xml") || splitString[index].toLowerCase().contains("folder.files") || splitString[index].toLowerCase().contains(".json") || splitString[index].toLowerCase().contains(".pdf") || splitString[index].toLowerCase().contains(".csv") || splitString[index].toLowerCase().contains(".txt") || splitString[index].toLowerCase().contains(".xls")) {
                                            search_FlatFiles(splitString, index, tableName);
                                        }
                                    }
                                } else {
                                    search_StoredProcedures(splitString, schemaName, tableName);
                                }
                            }
                        }
                    } else if (expression.contains("Csv.Document")) {
                        parserParameterisedCSVDocument(expression, tableName);
                    } else if (expression.contains("Schema") && expression.contains("Item")) {
                        searchMannualTable(expression, tableName);
                    } else if (expression.contains("Schema") && expression.contains("Name")) {
                        searchMannualTable1(expression, tableName);
                    }
                } else if (expression.contains("ADDCOLUMNS")) {
                    PowerBI_DAX_Parser bI_DAX_Parser = new PowerBI_DAX_Parser();
//                                    bI_DAX_Parser.dax_ParseDAXQuery(expression, tableName, bean);
                    itemPathAndQueryMap.put(tableName.toUpperCase(), expression);

                } else {
                    parseMeasuresFromSource(tableName, expression);
                }
            }
        }
        if (queryAgainstEntityName.containsKey(entityName)) {
            String oldValue = queryAgainstEntityName.get(entityName);
            oldValue = oldValue + "@ERWIN@" + tableName;
            queryAgainstEntityName.put(entityName, oldValue);
        } else {
            queryAgainstEntityName.put(entityName, tableName);
        }
    }

    public ArrayList<MappingSpecificationRow> getDataflowInfo(PowerBI_Bean bean, String expression, JSONArray columnArray) {
        String dataflowId = "";
        String workspaceId = "";
        String entityName = "";

        try {

//            if (expression.contains("dataflow")) {
//                if (!expression.contains("varDataflowID")) {
//                    dataflowId = expression.split("dataflowId=")[1].split("\"")[1];
//                    workspaceId = expression.split("workspaceId=")[1].split("\"")[1];
//                    if (expression.contains("EntityName")) {
//                        entityName = expression.split("EntityName = \"")[1].split("\"")[0];
//                    } else {
//                        entityName = expression.split("entity=\"")[1].split("\"")[0];
//                    }
//                    String key = dataflowId + "#ERWIN#" + workspaceId;
//                    if (!dataflowsAgainstID.containsKey(key)) {
//                        PowerBI_RestUtil pbiru = new PowerBI_RestUtil();
//                        String dataflowJson = pbiru.getPowerBIDataflow(bean, workspaceId, dataflowId);
//                        parseDataflowJSON(dataflowJson);
//                    }
//            File dataflowFile = new File("D:\\Erwin-Project Area\\Power BI\\EON\\Input Files\\Dataflow Inputs\\GV Sicherung (Dataflow Bestand).json");
//            String dataflowJson = FileUtils.file(dataflowFile);
//            readDataflowJSON(dataflowJson);
//                    String dataflowJson = pbiru.getPowerBIDataflow(bean, workspaceId, dataflowId);
//                    tablesMapAgainstID.put(key, queryAgainstEntityName);
//            prepareMappingSpecificationForDataflowEntity(dataflowName, "FACT_BESTAND_ALL", queryAgainstEntityName, columnArray);
            return specs;
//                }
//            }
        } catch (Exception ex) {

        }
        return specs;
    }

    public void prepareMappingSpecificationForDataflowEntity(String dataflowName, String entityName, Map<String, String> queryAgainstEntityName, JSONArray columnArray) {
        specs = new ArrayList<>();
        try {
            String tables = queryAgainstEntityName.get(entityName);
            for (String table : tables.split("@ERWIN@")) {
                String tableName = table.split("#ERWIN#")[0];
                for (int i = 1; i < columnArray.length(); i++) {
                    JSONObject columnObject = columnArray.getJSONObject(i);
                    String columnName = columnObject.getString("name");
                    if (table.split("#ERWIN#")[1].equals("ENTITY")) {
                        getSpecsForEntity(dataflowName, tableName, queryAgainstEntityName, columnName);
                        MappingSpecificationRow spec = new MappingSpecificationRow();
                        spec.setSourceSystemName("PBI");
                        spec.setSourceSystemEnvironmentName("DF_ENTITY");
                        spec.setSourceTableName(tableName);
                        spec.setSourceColumnName(columnName);
                        spec.setTargetSystemName("PBI");
                        spec.setTargetSystemEnvironmentName("DF_ENTITY");
                        spec.setTargetTableName(entityName);
                        spec.setTargetColumnName(columnName);
                        specs.add(spec);
                    } else {
                        MappingSpecificationRow spec = new MappingSpecificationRow();
                        spec.setSourceSystemName("PBI");
                        spec.setSourceSystemEnvironmentName("DF_TABLE");
                        spec.setSourceTableName(tableName);
                        spec.setSourceColumnName(columnName);
                        spec.setTargetSystemName("PBI");
                        spec.setTargetSystemEnvironmentName("DF_ENTITY");
                        spec.setTargetTableName(entityName);
                        spec.setTargetColumnName(columnName);
                        specs.add(spec);
                    }
                    MappingSpecificationRow spec = new MappingSpecificationRow();
                    spec.setSourceSystemName("PBI");
                    spec.setSourceSystemEnvironmentName("DF_TABLE");
                    spec.setSourceTableName(entityName);
                    spec.setSourceColumnName(columnName);
                    spec.setTargetSystemName("PBI");
                    spec.setTargetSystemEnvironmentName("DF_ENTITY");
                    spec.setTargetTableName(dataflowName);
                    spec.setTargetColumnName(columnName);
                    specs.add(spec);
                }
            }
        } catch (Exception ex) {

        }

    }

    public void getSpecsForEntity(String dataflowName, String entityName, Map<String, String> queryAgainstEntityName, String columnName) {
        String tables = queryAgainstEntityName.get(entityName);
        for (String table : tables.split("@ERWIN@")) {
            String tableName = table.split("#ERWIN#")[0];
            if (table.split("#ERWIN#")[1].equals("ENTITY")) {
                getSpecsForEntity(dataflowName, tableName, queryAgainstEntityName, columnName);
                MappingSpecificationRow spec = new MappingSpecificationRow();
                spec.setSourceSystemName("PBI");
                spec.setSourceSystemEnvironmentName("DF_ENTITY");
                spec.setSourceTableName(tableName);
                spec.setSourceColumnName(columnName);
                spec.setTargetSystemName("PBI");
                spec.setTargetSystemEnvironmentName("DF_ENTITY");
                spec.setTargetTableName(entityName);
                spec.setTargetColumnName(columnName);
                specs.add(spec);
            } else {
                MappingSpecificationRow spec = new MappingSpecificationRow();
                spec.setSourceSystemName("PBI");
                spec.setSourceSystemEnvironmentName("DF_TABLE");
                spec.setSourceTableName(tableName);
                spec.setSourceColumnName(columnName);
                spec.setTargetSystemName("PBI");
                spec.setTargetSystemEnvironmentName("DF_ENTITY");
                spec.setTargetTableName(entityName);
                spec.setTargetColumnName(columnName);
                specs.add(spec);
            }
        }
    }
//    public void getSpecificationsForEntities(){
//        
//        for (Map.Entry<String, String> entry : queryAgainstEntityName.entrySet()) {
//            String entityName = entry.getKey();
//            String tables = entry.getValue();
//            for(String table:){
//                
//            }
//            
//        }
//        
//    }

    public static void main(String[] args) {
        try {
            PowerBI_DataflowUtil dataflowUtil = new PowerBI_DataflowUtil();
            PowerBI_Bean bean = new PowerBI_Bean();
            JSONArray array = new JSONArray("[\n"
                    + "	{\n"
                    + "		\"type\": \"rowNumber\",\n"
                    + "		\"name\": \"RowNumber-2662979B-1795-4F74-8F37-6A1BA8059B61\",\n"
                    + "		\"dataType\": \"int64\",\n"
                    + "		\"isHidden\": true,\n"
                    + "		\"isUnique\": true,\n"
                    + "		\"isKey\": true,\n"
                    + "		\"isNullable\": false,\n"
                    + "		\"attributeHierarchy\": {}\n"
                    + "	},\n"
                    + "	{\n"
                    + "		\"name\": \"FULL_DT\",\n"
                    + "		\"dataType\": \"dateTime\",\n"
                    + "		\"sourceColumn\": \"FULL_DT\",\n"
                    + "		\"formatString\": \"Short Date\",\n"
                    + "		\"lineageTag\": \"b9cf728e-7830-4f2a-a688-cede232c70d8\",\n"
                    + "		\"summarizeBy\": \"none\",\n"
                    + "		\"attributeHierarchy\": {},\n"
                    + "		\"annotations\": [\n"
                    + "			{\n"
                    + "				\"name\": \"SummarizationSetBy\",\n"
                    + "				\"value\": \"Automatic\"\n"
                    + "			},\n"
                    + "			{\n"
                    + "				\"name\": \"UnderlyingDateTimeDataType\",\n"
                    + "				\"value\": \"Date\"\n"
                    + "			},\n"
                    + "			{\n"
                    + "				\"name\": \"Format\",\n"
                    + "				\"value\": \"<Format Format=\\\"DateTimeGeneralPattern\\\"><DateTimes><DateTime LCID=\\\"1031\\\" Group=\\\"ShortDate\\\" FormatString=\\\"d\\\" /></DateTimes></Format>\"\n"
                    + "			},\n"
                    + "			{\n"
                    + "				\"name\": \"PBI_ChangedProperties\",\n"
                    + "				\"value\": \"[\\\"FormatString\\\"]\"\n"
                    + "			}\n"
                    + "		]\n"
                    + "	},\n"
                    + "	{\n"
                    + "		\"name\": \"GKZ\",\n"
                    + "		\"dataType\": \"string\",\n"
                    + "		\"sourceColumn\": \"GKZ\",\n"
                    + "		\"lineageTag\": \"63cc1735-45da-4c67-8a00-eceb2eb45c5a\",\n"
                    + "		\"summarizeBy\": \"none\",\n"
                    + "		\"attributeHierarchy\": {},\n"
                    + "		\"annotations\": [\n"
                    + "			{\n"
                    + "				\"name\": \"SummarizationSetBy\",\n"
                    + "				\"value\": \"Automatic\"\n"
                    + "			},\n"
                    + "			{\n"
                    + "				\"name\": \"Format\",\n"
                    + "				\"value\": \"<Format Format=\\\"Text\\\" />\"\n"
                    + "			}\n"
                    + "		]\n"
                    + "	},\n"
                    + "	{\n"
                    + "		\"name\": \"EVU\",\n"
                    + "		\"dataType\": \"string\",\n"
                    + "		\"sourceColumn\": \"EVU\",\n"
                    + "		\"lineageTag\": \"d89ec701-8d4c-44ef-bfbf-b60545cd9538\",\n"
                    + "		\"summarizeBy\": \"none\",\n"
                    + "		\"attributeHierarchy\": {},\n"
                    + "		\"annotations\": [\n"
                    + "			{\n"
                    + "				\"name\": \"SummarizationSetBy\",\n"
                    + "				\"value\": \"Automatic\"\n"
                    + "			},\n"
                    + "			{\n"
                    + "				\"name\": \"Format\",\n"
                    + "				\"value\": \"<Format Format=\\\"Text\\\" />\"\n"
                    + "			}\n"
                    + "		]\n"
                    + "	},\n"
                    + "	{\n"
                    + "		\"name\": \"GV_ACX_REGION_KEY\",\n"
                    + "		\"dataType\": \"int64\",\n"
                    + "		\"sourceColumn\": \"GV_ACX_REGION_KEY\",\n"
                    + "		\"formatString\": \"0\",\n"
                    + "		\"lineageTag\": \"65b7c5b3-5adc-4176-b613-13c8611e0021\",\n"
                    + "		\"summarizeBy\": \"none\",\n"
                    + "		\"attributeHierarchy\": {},\n"
                    + "		\"annotations\": [\n"
                    + "			{\n"
                    + "				\"name\": \"SummarizationSetBy\",\n"
                    + "				\"value\": \"User\"\n"
                    + "			},\n"
                    + "			{\n"
                    + "				\"name\": \"Format\",\n"
                    + "				\"value\": \"<Format Format=\\\"NumberWhole\\\" Accuracy=\\\"0\\\" />\"\n"
                    + "			}\n"
                    + "		]\n"
                    + "	},\n"
                    + "	{\n"
                    + "		\"name\": \"GV_CON_KEY\",\n"
                    + "		\"dataType\": \"int64\",\n"
                    + "		\"sourceColumn\": \"GV_CON_KEY\",\n"
                    + "		\"formatString\": \"0\",\n"
                    + "		\"lineageTag\": \"169bfd85-8037-4277-adf4-7c23857468fc\",\n"
                    + "		\"summarizeBy\": \"none\",\n"
                    + "		\"attributeHierarchy\": {},\n"
                    + "		\"annotations\": [\n"
                    + "			{\n"
                    + "				\"name\": \"SummarizationSetBy\",\n"
                    + "				\"value\": \"Automatic\"\n"
                    + "			},\n"
                    + "			{\n"
                    + "				\"name\": \"Format\",\n"
                    + "				\"value\": \"<Format Format=\\\"NumberWhole\\\" Accuracy=\\\"0\\\" />\"\n"
                    + "			}\n"
                    + "		]\n"
                    + "	},\n"
                    + "	{\n"
                    + "		\"name\": \"GV_COB_PROFILE_KEY\",\n"
                    + "		\"dataType\": \"int64\",\n"
                    + "		\"sourceColumn\": \"GV_COB_PROFILE_KEY\",\n"
                    + "		\"formatString\": \"0\",\n"
                    + "		\"lineageTag\": \"c341a7f3-dcde-49e7-bff0-64fd3425daec\",\n"
                    + "		\"summarizeBy\": \"none\",\n"
                    + "		\"attributeHierarchy\": {},\n"
                    + "		\"annotations\": [\n"
                    + "			{\n"
                    + "				\"name\": \"SummarizationSetBy\",\n"
                    + "				\"value\": \"Automatic\"\n"
                    + "			},\n"
                    + "			{\n"
                    + "				\"name\": \"Format\",\n"
                    + "				\"value\": \"<Format Format=\\\"NumberWhole\\\" Accuracy=\\\"0\\\" />\"\n"
                    + "			}\n"
                    + "		]\n"
                    + "	},\n"
                    + "	{\n"
                    + "		\"name\": \"PRD_KEY\",\n"
                    + "		\"dataType\": \"int64\",\n"
                    + "		\"sourceColumn\": \"PRD_KEY\",\n"
                    + "		\"formatString\": \"0\",\n"
                    + "		\"lineageTag\": \"e18dc883-eccc-4d83-b1c7-99f92e3b1564\",\n"
                    + "		\"summarizeBy\": \"none\",\n"
                    + "		\"attributeHierarchy\": {},\n"
                    + "		\"annotations\": [\n"
                    + "			{\n"
                    + "				\"name\": \"SummarizationSetBy\",\n"
                    + "				\"value\": \"Automatic\"\n"
                    + "			},\n"
                    + "			{\n"
                    + "				\"name\": \"Format\",\n"
                    + "				\"value\": \"<Format Format=\\\"NumberWhole\\\" Accuracy=\\\"0\\\" />\"\n"
                    + "			}\n"
                    + "		]\n"
                    + "	},\n"
                    + "	{\n"
                    + "		\"name\": \"DIV_GKZ_KEY\",\n"
                    + "		\"dataType\": \"int64\",\n"
                    + "		\"sourceColumn\": \"DIV_GKZ_KEY\",\n"
                    + "		\"formatString\": \"0\",\n"
                    + "		\"lineageTag\": \"c97f0697-2788-46ac-9c19-0ad78cd91af9\",\n"
                    + "		\"summarizeBy\": \"none\",\n"
                    + "		\"attributeHierarchy\": {},\n"
                    + "		\"annotations\": [\n"
                    + "			{\n"
                    + "				\"name\": \"SummarizationSetBy\",\n"
                    + "				\"value\": \"Automatic\"\n"
                    + "			},\n"
                    + "			{\n"
                    + "				\"name\": \"Format\",\n"
                    + "				\"value\": \"<Format Format=\\\"NumberWhole\\\" Accuracy=\\\"0\\\" />\"\n"
                    + "			}\n"
                    + "		]\n"
                    + "	},\n"
                    + "	{\n"
                    + "		\"name\": \"CONTRACT_CNT_DIST\",\n"
                    + "		\"dataType\": \"int64\",\n"
                    + "		\"sourceColumn\": \"CONTRACT_CNT_DIST\",\n"
                    + "		\"formatString\": \"#,0\",\n"
                    + "		\"lineageTag\": \"c5852a5f-07f0-480c-ac8d-720a45da97c1\",\n"
                    + "		\"summarizeBy\": \"sum\",\n"
                    + "		\"attributeHierarchy\": {},\n"
                    + "		\"annotations\": [\n"
                    + "			{\n"
                    + "				\"name\": \"SummarizationSetBy\",\n"
                    + "				\"value\": \"Automatic\"\n"
                    + "			},\n"
                    + "			{\n"
                    + "				\"name\": \"Format\",\n"
                    + "				\"value\": \"<Format Format=\\\"NumberWhole\\\" Accuracy=\\\"0\\\" ThousandSeparator=\\\"True\\\" />\"\n"
                    + "			},\n"
                    + "			{\n"
                    + "				\"name\": \"PBI_ChangedProperties\",\n"
                    + "				\"value\": \"[\\\"FormatString\\\"]\"\n"
                    + "			}\n"
                    + "		]\n"
                    + "	},\n"
                    + "	{\n"
                    + "		\"type\": \"calculated\",\n"
                    + "		\"name\": \"Marktanteil Cluster\",\n"
                    + "		\"dataType\": \"string\",\n"
                    + "		\"isDataTypeInferred\": true,\n"
                    + "		\"isHidden\": true,\n"
                    + "		\"expression\": \"\\nvar __DIV_GKZ = 'Fakten (Bestand)'[DIV_GKZ_KEY]\\nvar CURRENT_MAT = \\n    CALCULATE(\\n        [Aktueller Marktanteil],\\n        TREATAS(VALUES('Fakten (Bestand)'[DIV_GKZ_KEY]), 'GKZ GV'[DIV_GKZ_KEY]),\\n        FILTER(\\n            ALL('Fakten (Bestand)'),\\n            'Fakten (Bestand)'[DIV_GKZ_KEY] = __DIV_GKZ\\n        ),\\n        FILTER(\\n            ALL('GKZ GV'),\\n            'GKZ GV'[DIV_GKZ_KEY] = __DIV_GKZ\\n        )\\n    )\\nreturn\\n    IF(\\n        NOT(ISBLANK(CURRENT_MAT)),\\n        SWITCH(\\n            TRUE(),\\n            CURRENT_MAT < 0.25, \\\"unter 25%\\\",\\n            CURRENT_MAT < 0.3, \\\"25-30%\\\",\\n            CURRENT_MAT < 0.4, \\\"30-40%\\\",\\n            \\\"über 55%\\\"\\n        )\\n    )\",\n"
                    + "		\"lineageTag\": \"8f1ccfe4-5a3b-4770-b395-f65811078b41\",\n"
                    + "		\"summarizeBy\": \"none\",\n"
                    + "		\"attributeHierarchy\": {},\n"
                    + "		\"annotations\": [\n"
                    + "			{\n"
                    + "				\"name\": \"SummarizationSetBy\",\n"
                    + "				\"value\": \"Automatic\"\n"
                    + "			},\n"
                    + "			{\n"
                    + "				\"name\": \"Format\",\n"
                    + "				\"value\": \"<Format Format=\\\"Text\\\" />\"\n"
                    + "			},\n"
                    + "			{\n"
                    + "				\"name\": \"PBI_ChangedProperties\",\n"
                    + "				\"value\": \"[\\\"IsHidden\\\"]\"\n"
                    + "			}\n"
                    + "		]\n"
                    + "	},\n"
                    + "	{\n"
                    + "		\"name\": \"SRC\",\n"
                    + "		\"dataType\": \"string\",\n"
                    + "		\"sourceColumn\": \"SRC\",\n"
                    + "		\"lineageTag\": \"2b7a8368-89b2-4e88-a3df-da339c3da609\",\n"
                    + "		\"summarizeBy\": \"none\",\n"
                    + "		\"attributeHierarchy\": {},\n"
                    + "		\"annotations\": [\n"
                    + "			{\n"
                    + "				\"name\": \"SummarizationSetBy\",\n"
                    + "				\"value\": \"Automatic\"\n"
                    + "			},\n"
                    + "			{\n"
                    + "				\"name\": \"Format\",\n"
                    + "				\"value\": \"<Format Format=\\\"Text\\\" />\"\n"
                    + "			}\n"
                    + "		]\n"
                    + "	}\n"
                    + "]");
            File dataflowFile = new File("D:\\Erwin-Project Area\\Power BI\\EON\\Input Files\\Dataflow Inputs\\GV Sicherung (Dataflow Akquise, Churn).json");
//            String dataflowJSON = FileUtils.fileRead(dataflowFile);
//            dataflowUtil.getDataflowInfo(bean, "", array);
        } catch (Exception ex) {

        }
    }
}
