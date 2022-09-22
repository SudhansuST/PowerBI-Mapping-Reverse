/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.connectors.pbi.map.v11;

import com.erwin.cfx.connectors.pbi.map.pojo.Entry;
import com.erwin.cfx.connectors.pbi.map.pojo.Item;
import com.erwin.cfx.connectors.pbi.map.pojo.StableEntries;
import com.erwin.cfx.connectors.pbi.map.pojo.ItemLocation;
import com.erwin.cfx.connectors.pbi.map.pojo.Items;
import com.erwin.cfx.connectors.pbi.map.pojo.LocalPackageMetadataFile;
import com.ads.api.beans.mm.MappingSpecificationRow;
import com.ads.api.util.SystemManagerUtil;
import com.erwin.cfx.connectors.json.syncup.SyncupWithServerDBSchamaSysEnvCPT;
import static com.erwin.cfx.connectors.pbi.map.v11.PowerBIReportParser.allDBMap;
import static com.erwin.cfx.connectors.pbi.map.v11.PowerBIReportParser.allTablesMap;
import static com.erwin.cfx.connectors.pbi.map.v11.PowerBIReportParser.metadataJsonPath;
import static com.erwin.cfx.connectors.pbi.map.v11.PowerBIReportParser.metadataMap;
import com.erwin.cfx.connectors.sqlparser.v3.MappingCreator;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author SudhansuTarai
 */
public class DataMashupExtractor {

    public static Map<String, String> referenceTableAgnaistLogicalTableName = new HashMap<>();
    public static Map<String, String> queriesAgainstLogicalTable = new HashMap<>();
    public static Map<String, String> allQuery = new HashMap<>();
    public static Map<String, Set<String>> tablecoulmnInfo = new HashMap<>();
    public static Map<String, String> powerBIQueryAgainstTableName = new HashMap<>();
    public static Map<String, String> tableNameAgainstSource = new HashMap<>();
    public static Map<String, String> joinInfoAgainstTableName = new HashMap<>();
    public static Map<String, String> xmlDBMap = new HashMap<>();
    private static Logger logger = Logger.getLogger(DataMashupExtractor.class);
    private static JAXBContext jaxbContext = null;
    private static ItemLocation itemLocation;
    private static String obj = "";
    public static Map<String, Map<String, Set<String>>> extendedPropertiesAgainstTable = new HashMap<>();
    public static Map<String, ArrayList<MappingSpecificationRow>> mapSpecListAgainstLogicalTable = new HashMap<>();

    public static File getXMLFromDataMashUP(File dataMashuFile) {
        FileWriter writer = null;
        File xmlFile = null;
        try {
            String name = dataMashuFile.getAbsolutePath();
            String readFileToString = FileUtils.readFileToString(dataMashuFile);
            String startTag = "<LocalPackageMetadataFile ";
            String endTag = "</LocalPackageMetadataFile>";
            int indexStartTag = 0;
            int indexendTag = 0;
            indexStartTag = readFileToString.indexOf(startTag);
            indexendTag = readFileToString.indexOf(endTag);
            String properXmlFile = readFileToString.substring(indexStartTag, indexendTag);
            properXmlFile = properXmlFile.concat("</LocalPackageMetadataFile>");
            File file = new File(name + "copy.xml");
            xmlFile = file;
            writer = new FileWriter(file);
            writer.append(properXmlFile);
            try {
                if (writer != null) {
                    writer.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        } catch (Exception ex) {
            logger.error(ex.getMessage());
        }
        return xmlFile;
    }

    public static void parseDataMashupXML(File dataMashuFile, String[] sysenvDetails, SystemManagerUtil smutil, String reportName) {
        try {
            File xmlFile = getXMLFromDataMashUP(dataMashuFile);
            jaxbContext = JAXBContext.newInstance(LocalPackageMetadataFile.class);
            Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
            LocalPackageMetadataFile localPackageMetadataFile = (LocalPackageMetadataFile) jaxbUnmarshaller.unmarshal(xmlFile);
            Items items = localPackageMetadataFile.getItems();
            List<Item> item = items.getItem();
            for (Item item2 : item) {
                itemLocation = item2.getItemLocation();
                StableEntries stableEntries = item2.getStableEntries();
                if (!stableEntries.equals(obj)) {
                    List<Entry> entry = stableEntries.getEntry();
                    for (Entry entry2 : entry) {
                        if (entry2.getType().equals("LastAnalysisServicesFormulaText")) {
                            String LASFT_Value = entry2.getValue();
                            LASFT_Value = LASFT_Value.replace("&quot;", "\"");
                            LASFT_Value = LASFT_Value.substring(LASFT_Value.indexOf("{"), LASFT_Value.lastIndexOf("}") + 1);
                            extractReferenceInformation(LASFT_Value, itemLocation.getItemPath().split("/")[1].replace("%20", " ").replace("%25", "%").replace("%26", "&"), reportName);
//                            getMappingSpecifications(sysenvDetails, smutil);
                        }
                    }
                }
            }
        } catch (Exception ex) {
            logger.error(ex.getMessage());
        }
    }

    public static void extractReferenceInformation(String LASFT_JSON, String tableName, String reportName) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            JSONObject LASFT_JSONObject = new JSONObject(LASFT_JSON);
            String RootFormulaText_Info = LASFT_JSONObject.getString("RootFormulaText");
            JSONObject ReferencedQueriesFormulaText_JSONObject = LASFT_JSONObject.getJSONObject("ReferencedQueriesFormulaText");
            if (ReferencedQueriesFormulaText_JSONObject.length() > 0) {
                String referenceTableNames = "";
                Map<String, String> referencedMap = mapper.readValue(ReferencedQueriesFormulaText_JSONObject.toString(), Map.class);
                Set<String> referenceTables = referencedMap.keySet();
                for (Iterator<String> iterator = referenceTables.iterator(); iterator.hasNext();) {
                    String next = iterator.next();
                    if (referenceTableNames.isEmpty()) {
                        referenceTableNames = next.trim();
                    } else {
                        referenceTableNames = referenceTableNames + "#!ERWIN!#" + next.trim();
                    }
                }
                referenceTableAgnaistLogicalTableName.put(reportName+"."+tableName.toUpperCase(), referenceTableNames);
            }
            queriesAgainstLogicalTable.put(reportName+"."+tableName.toUpperCase(), RootFormulaText_Info);

        } catch (Exception ex) {

        }
    }

    public static void getMappingSpecifications(String[] sysenvDetails, SystemManagerUtil smutil) {

        for (Map.Entry<String, String> entry : referenceTableAgnaistLogicalTableName.entrySet()) {
            String logicalTableName = entry.getKey();
            String actualtables = entry.getValue();
            for (String eachtable : actualtables.split("#!ERWIN!#")) {
                String query = queriesAgainstLogicalTable.get(eachtable);
                if (query.toLowerCase().contains("select") || query.toLowerCase().contains("with")) {
                    parse_SQL_Query(query, sysenvDetails, smutil);
                } else {
                    parse_M_Query(query);
                }
            }
            String mainQuery = queriesAgainstLogicalTable.get(logicalTableName);
            if (mainQuery.toLowerCase().contains("select") || mainQuery.toLowerCase().contains("with")) {

            } else {
                parse_M_Query(mainQuery);
            }
        }

    }

    public static void parse_M_Query(String query) {

        for (String eachline : query.split("\n")) {

            if (eachline.contains("Source")) {
                String source = eachline.split("=")[0].replace("#", "").replace("\"", "").replace(",", "").trim();

            } else if (eachline.contains("Source")) {

            }

            eachline.concat("let");

        }

        if (query.contains("Merged Columns")) {

        }
    }

    public static ArrayList<MappingSpecificationRow> parse_SQL_Query(String query, String[] sysenvDetails, SystemManagerUtil smutil) {
        String ServerName = "";
        String databaseName = "";
        String dbType = "";
        if (query.toLowerCase().contains("database")) {
            dbType = query.substring(query.indexOf("=") + 1, query.indexOf(".Database")).trim();
            try {
                ServerName = query.split("\",")[0].substring(query.split(",")[0].indexOf("(") + 1);
                ServerName = ServerName.substring(ServerName.indexOf("\\") + 1, ServerName.length() - 1).replace("\\\\", "\\").replace("\"", "");
                try {
                    databaseName = query.split("\",")[1];
                } catch (StringIndexOutOfBoundsException ex) {
                    databaseName = query.split("\",")[1].substring(query.split("\",")[1].indexOf("\\") + 1, query.split("\",")[1].lastIndexOf("\\")).replace("\\", "").replace(")", "").replace("\"", "").trim();
                }
            } catch (ArrayIndexOutOfBoundsException ex) {
                ServerName = "";
                databaseName = "";
            }
        }

        String rePlace = "";
        if (query.contains(";")) {
            int firstIndex = query.indexOf("[Query=");
            rePlace = "[Query=";
            int lastIndex = query.lastIndexOf(";");
            if (firstIndex == -1 || lastIndex == -1) {
                firstIndex = query.indexOf("Query=");
                lastIndex = query.lastIndexOf(";");
                if (firstIndex == -1 || lastIndex == -1) {
                    return null;
                }
                rePlace = "Query=";
            }
            query = query.substring(firstIndex, lastIndex);
            query = query.replace(rePlace, "");
            query = query.replace("&quot;", "\"");
            query = query.replace("\\u0027", "'").replace("\\u003e", "> ").replace("\"", "").replace("\\u003c", "< ");
            query = query.replace("#(lf)", "\n");
            query = query.replace("#(tab)", "\t");
            query = query.replace("=\\", "");
            query = query.replace("\\\\", "\"").replace("\\", " ").trim();

        } else {
            int firstIndex = query.indexOf("[Query=");
            int lastIndex = query.lastIndexOf("]");
            rePlace = "[Query=";
            if (firstIndex == -1 || lastIndex == -1) {
                firstIndex = query.indexOf("Query=");
                lastIndex = query.lastIndexOf("]");
                if (firstIndex == -1 || lastIndex == -1) {
                    return null;
                }
                rePlace = "Query=";
            }
            query = query.substring(firstIndex, lastIndex);
            query = query.replace(rePlace, "");
            query = query.replace("&quot;", "\"");
            query = query.replace("\\u0027", "'").replace("\\u003e", "> ").replace("\"", "").replace("\\u003c", "< ");
            query = query.replace("#(lf)", "\n");
            query = query.replace("#(tab)", "\t");
            query = query.replace("=\\", "");
            query = query.replace("\\\\", "\"").replace("\\", " ").trim();

        }

        MappingCreator mappingCreator = new MappingCreator();
        String json = mappingCreator.getMappingObjectToJsonForSSIS(query, sysenvDetails[0], sysenvDetails[1], 0, dbType, "", 0);
        if (!json.equals("") && json != null) {
            ArrayList<MappingSpecificationRow> mapspecrows = SyncupWithServerDBSchamaSysEnvCPT.setMetaDataSpec(PowerBIReportParser.setMetadataMap(json, databaseName, ServerName, metadataJsonPath, "SYS", "ENV", allDBMap, allTablesMap, metadataMap, "", smutil));
        }
        return null;

    }

    public static void extractInformationFromCustomQueryJSON(String LASFT_JSON, String tableName) {
        ObjectMapper mapper = new ObjectMapper();
        Map<String, String> referencedMap = null;
        try {
            JSONObject LASFT_JSONObject = new JSONObject(LASFT_JSON);
            String RootFormulaText_Info = LASFT_JSONObject.getString("RootFormulaText");
            JSONObject ReferencedQueriesFormulaText_JSONObject = LASFT_JSONObject.getJSONObject("ReferencedQueriesFormulaText");
            if (ReferencedQueriesFormulaText_JSONObject.length() > 0) {
                String TableName = "";
                referencedMap = mapper.readValue(ReferencedQueriesFormulaText_JSONObject.toString(), Map.class);
                ArrayList<String> tableList = new ArrayList<>(referencedMap.keySet());
                for (int i = 0; i < tableList.size(); i++) {
                    if (i > 0) {
                        TableName = TableName + "#ERWIN#" + tableList.get(i);
                    } else {
                        TableName = tableList.get(i);
                    }
                }
                allQuery.put(tableName, RootFormulaText_Info);
                extractInfoIfReferencedQueryAvailable(RootFormulaText_Info, tableName);
                tableNameAgainstSource.put(tableName, TableName);
                if (powerBIQueryAgainstTableName.get(tableName) == null) {
                    powerBIQueryAgainstTableName.put(tableName, "PowerBI_CutomQuery");
                }
            } else {
                extractModelTableColumnInformation(RootFormulaText_Info, tableName);
            }

        } catch (JSONException ex) {
            logger.error(ex.getMessage());
        } catch (IOException ex) {
            logger.error(ex.getMessage());
        }

    }

    public static void extractModelTableColumnInformation(String powerBi_CustomQuery, String tablaName) {
        Set<String> columns = new HashSet<>();
        String[] customQueryArray = powerBi_CustomQuery.split("\r\n    #");
        try {
            if (customQueryArray[0].toLowerCase().contains("select") || customQueryArray[0].toLowerCase().contains("with")) {
                if (customQueryArray[0].contains(".Database")) {
                    final String databaseName = customQueryArray[0].substring(customQueryArray[0].indexOf("Source =") + 8, customQueryArray[0].indexOf(".Database")).trim();
                    final String objectName = DataMashupExtractor.itemLocation.getItemPath().replace("Section1/", "").replaceAll("%20", " ").replaceAll("%20", " ").replaceAll("%25", "%").replaceAll("%26", "&");
                    DataMashupExtractor.xmlDBMap.put(objectName, databaseName);
                }
                String query = customQueryArray[0];
                String rePlace = "";
                if (query.contains(";")) {
                    int firstIndex = query.indexOf("[Query=");
                    rePlace = "[Query=\"";
                    int lastIndex = query.lastIndexOf(";");
                    if (firstIndex == -1 || lastIndex == -1) {
                        firstIndex = query.indexOf("Query=");
                        lastIndex = query.lastIndexOf(";");
                        if (firstIndex == -1 || lastIndex == -1) {
                            return;
                        }
                        rePlace = "Query=\"";
                    }
                    query = query.substring(firstIndex, lastIndex);
                    query = query.replace(rePlace, "");
                    query = query.replace("\\u0027", "'").replace("\\u003e", "> ");
                    query = query.replace("#(lf)", "\n");
                    query = query.replace("#(tab)", "\t");
                    query = query.replace("=\\", "");
                    query = query.replace("\\\\", "\"").replace("\\", " ").trim();
                    powerBIQueryAgainstTableName.put(tablaName, query);
                    allQuery.put(tablaName, query);
                } else {
                    int firstIndex = query.indexOf("[Query=");
                    int lastIndex = query.lastIndexOf("]");
                    rePlace = "[Query=";
                    if (firstIndex == -1 || lastIndex == -1) {
                        firstIndex = query.indexOf("Query=");
                        lastIndex = query.lastIndexOf("]");
                        if (firstIndex == -1 || lastIndex == -1) {
                            return;
                        }
                        rePlace = "Query=";
                    }
                    query = query.substring(firstIndex, lastIndex);
                    query = query.replace(rePlace, "");
                    query = query.replace("\\u0027", "'").replace("\\u003e", "> ");
                    query = query.replace("#(lf)", "\n");
                    query = query.replace("#(tab)", "\t");
                    query = query.replace("=\\", "");
                    query = query.replace("\\\\", "\"").replace("\\", " ");
                    query = query.substring(query.indexOf("\"") + 1, query.lastIndexOf("\"")).replace("\"\"", "\"").trim();
                    powerBIQueryAgainstTableName.put(tablaName, query);
                    allQuery.put(tablaName, query);
                }
            } else if (powerBi_CustomQuery.toLowerCase().contains("xlsx")) {
                allQuery.put(tablaName, powerBi_CustomQuery);
                for (int i = 1; i < customQueryArray.length; i++) {
                    String eachcontent = customQueryArray[i];
                    if (eachcontent.split("=")[0].contains("xlsx")) {
                        String content = eachcontent.split("=")[2].split(",")[0].replace("\"", "");
                        powerBIQueryAgainstTableName.put(tablaName.replace("%20", " ").replace("%25", "%").replace("%26", "&"), content);
                    }
                }
            } else if (customQueryArray[0].toLowerCase().contains("sharepoint")) {
                powerBIQueryAgainstTableName.put(tablaName, "share-point");

            } else {
                allQuery.put(tablaName, powerBi_CustomQuery);
                for (int i = 1; i < customQueryArray.length; i++) {
                    String eachcontent = customQueryArray[i];
                    try {
                        if (eachcontent.split("=")[0].contains("Merged Columns")) {
                            String columnName = eachcontent.split("=")[1].split(",")[eachcontent.split("=")[1].split(",").length - 1];
                            columnName = columnName.replace(")", "").replace("\"", "").trim();
                            columns.add(columnName);
                        } else if (eachcontent.split("=")[0].contains("Added Custom")) {
                            columns.add(eachcontent.split("=")[1].split(",")[1].replace("\"", "").trim());
                        } else if (eachcontent.split("=")[0].contains("Changed Type")) {
                            columns.add(eachcontent.split("=")[1].split(",")[1].replace("\"", "").replace("{", "").trim());
                        } else if (eachcontent.split("=")[0].contains("Duplicated Column")) {
                            columns.add(eachcontent.split("=")[1].split(",")[1].replace("\"", "").replace("{", "").trim());
                            columns.add(eachcontent.split("=")[1].split(",")[eachcontent.split("=")[1].split(",").length - 1].replace(")", "").replace("\"", "").trim());
                        } else if (eachcontent.split("=")[0].contains("Added Items")) {
                            String content = eachcontent.split("=")[1].substring(eachcontent.split("=")[1].indexOf("{") + 1, eachcontent.split("=")[1].lastIndexOf("}"));
                            content = content.substring(content.indexOf("{") + 1, content.lastIndexOf("}"));
                            content = content.substring(content.indexOf("{") + 1, content.lastIndexOf("}"));
                            content = content.substring(content.lastIndexOf("{") + 1, content.lastIndexOf("}"));
                            for (String columnName : content.split(",")) {
                                columns.add(columnName.replace("\"", ""));
                            }
                        } else if (eachcontent.split("=")[0].contains("xlsx")) {
                            String content = eachcontent.split("=")[2].split(",")[0].replace("\"", "");
                            powerBIQueryAgainstTableName.put(tablaName.replace("%20", " ").replace("%25", "%").replace("%26", "&"), content);
                            return;
                        }
                    } catch (ArrayIndexOutOfBoundsException ex) {
                        logger.error(ex.getMessage());
                    }
                }
                if (tablecoulmnInfo.get(tablaName.replace("%20", " ").replace("%25", "%").replace("%26", "&")) != null) {
                    Set<String> temp_columns = tablecoulmnInfo.get(tablaName.replace("%20", " "));
                    temp_columns.addAll(columns);
                    tablecoulmnInfo.put(tablaName.replace("%20", " ").replace("%25", "%").replace("%26", "&"), temp_columns);
                } else {
                    tablecoulmnInfo.put(tablaName.replace("%20", " ").replace("%25", "%").replace("%26", "&"), columns);
                }
            }
        } catch (Exception ex) {
            logger.error(ex.getMessage());
        }
    }

    public static void extractInfoIfReferencedQueryAvailable(String powerBi_CustomQuery, String tableName) {
        Set<String> columns = new HashSet<>();
        String[] customQueryArray = powerBi_CustomQuery.split("\r\n    #");
        if (powerBi_CustomQuery.toLowerCase().contains("join")) {
            String joinInfo = customQueryArray[0];
            if (joinInfo.toLowerCase().contains("join")) {
                joinInfo = joinInfo.substring(joinInfo.indexOf("(") + 1, joinInfo.indexOf(")"));
                String joinTable1 = joinInfo.split(",")[0];
                String joinColumnTable1 = joinInfo.split(",")[1].replace("{", "").replace("}", "").replace("\"", "");
                if (tablecoulmnInfo.get(joinTable1) != null) {
                    columns.addAll(tablecoulmnInfo.get(joinTable1));
                }
                columns.add(joinColumnTable1.trim());
                String joinTable2 = joinInfo.split(",")[2];
                String joinColumnTable2 = joinInfo.split(",")[3].replace("{", "").replace("}", "").replace("\"", "");
                columns.add(joinColumnTable2.trim());
                String joinName = joinInfo.split(",")[joinInfo.split(",").length - 1].split("\\.")[1];
                String joinCondition = "";
                if (joinName.equals("LeftOuter")) {
                    joinCondition = joinTable1 + " LEFT OUTER JOIN " + joinTable2 + " ON " + joinTable1 + "." + joinColumnTable1 + " = " + joinTable2 + "." + joinColumnTable2;
                } else if (joinName.equals("RightOuter")) {
                    joinCondition = joinTable1 + " RIGHT OUTER JOIN " + joinTable2 + " ON " + joinTable1 + "." + joinColumnTable1 + " = " + joinTable2 + "." + joinColumnTable2;
                } else if (joinName.equals("Inner")) {

                }
                joinInfoAgainstTableName.put(tableName, joinCondition);
                String selectedColumnsInfo = customQueryArray[1];
                selectedColumnsInfo = selectedColumnsInfo.split("=")[1];
                selectedColumnsInfo = selectedColumnsInfo.substring(selectedColumnsInfo.indexOf("(") + 1, selectedColumnsInfo.lastIndexOf(")"));
                String selectedColumnsInfo_1 = selectedColumnsInfo.substring(selectedColumnsInfo.indexOf("{") + 1, selectedColumnsInfo.indexOf("}"));
                for (String column : selectedColumnsInfo_1.split(",")) {
                    columns.add(column.replace("\"", "").trim());
                }
                String selectedColumnsInfo_2 = selectedColumnsInfo.substring(selectedColumnsInfo.lastIndexOf("{") + 1, selectedColumnsInfo.lastIndexOf("}"));
                for (String column : selectedColumnsInfo_2.split(",")) {
                    columns.add(column.replace("\"", "").trim());
                }
                if (tablecoulmnInfo.get(tableName.replace("%20", " ")) != null) {
                    Set<String> temp_columns = tablecoulmnInfo.get(tableName.replace("%20", " "));
                    temp_columns.addAll(columns);
                    tablecoulmnInfo.put(tableName.replace("%20", " "), temp_columns);
                } else {
                    tablecoulmnInfo.put(tableName.replace("%20", " "), columns);
                }
            } else {
                for (String eachSpecs : customQueryArray) {
                    if (eachSpecs.contains("Merged Queries")) {
                        String join = eachSpecs.split("=")[1].trim();
                        join = join.substring(join.indexOf("(") + 1, join.indexOf(")"));
                        String targetColumn = join.split(",")[1].replace("{", "").replace("}", "").replace("\"", "").trim();
                        String sourceTable1 = join.split(",")[2];
                        String sourceColumn2 = join.split(",")[3].replace("{", "").replace("}", "").replace("\"", "").trim();
                        String joinType = join.split(",")[5].split("\\.")[1].trim();
                    } else if (eachSpecs.contains("Appended Query")) {

                    }
//                    else if () {
//
//                    }
                }
            }
        } else if (powerBi_CustomQuery.toLowerCase().contains("combine")) {
            String combineInfo = customQueryArray[0];
            combineInfo = combineInfo.substring(combineInfo.indexOf("(") + 1, combineInfo.indexOf(")"));
            combineInfo = combineInfo.substring(combineInfo.indexOf("{") + 1, combineInfo.lastIndexOf("}"));
            String table1 = combineInfo.split(",")[0].replace("#", "").replace("\"", "").trim();
            String table2 = combineInfo.split(",")[1].replace("#", "").replace("\"", "").trim();
            joinInfoAgainstTableName.put(tableName, table1 + " COMBINE " + table2);
        } else {
            String source = powerBi_CustomQuery.substring(powerBi_CustomQuery.indexOf("Source = ") + 9, powerBi_CustomQuery.indexOf(","));
            powerBIQueryAgainstTableName.put(tableName, source + "#@ERWIN@#PBISource");
        }

    }
}
