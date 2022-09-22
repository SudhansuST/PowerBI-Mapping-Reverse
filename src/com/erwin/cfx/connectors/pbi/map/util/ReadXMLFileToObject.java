package com.erwin.cfx.connectors.pbi.map.util;

import com.erwin.cfx.connectors.pbi.map.pojo.Entry;
import com.erwin.cfx.connectors.pbi.map.pojo.Item;
import com.erwin.cfx.connectors.pbi.map.pojo.StableEntries;
import com.erwin.cfx.connectors.pbi.map.pojo.ItemLocation;
import com.erwin.cfx.connectors.pbi.map.pojo.Items;
import com.erwin.cfx.connectors.pbi.map.pojo.LocalPackageMetadataFile;
import com.ads.api.beans.mm.MappingSpecificationRow;
import com.erwin.cfx.connectors.pbi.map.v11.DataMashupExtractor;
import com.erwin.cfx.connectors.pbi.map.v11.PowerBIReportParser;
import static com.erwin.cfx.connectors.pbi.map.v11.PowerBIReportParser.exceptionLog;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Logger;

/**
 * @author SudhansuTarai
 *
 */
public class ReadXMLFileToObject {

    public static Map<String, String> mQueryAgainstObjectName = new HashMap<>();
    public static JAXBContext jaxbContext = null;
    public static Map<String, String> itemPathAndQueryMap = new HashMap();
    public static ArrayList<String> queryList = new ArrayList<String>();
    public static Map<String, String> itemPathAndxlPath = new HashMap();
    public static String itemPath = null;
    public static ItemLocation itemLocation;
    private static String obj = "";
    public static Logger logger = Logger.getLogger(ReadXMLFileToObject.class);
    public static Map<String, ArrayList<MappingSpecificationRow>> MergeQuerySpecs = new HashMap<>();
    public static Map<String, String> participatedTableAgainstMergedQuery = new HashMap<>();
    public static Set<String> setOfLogicalTable = new HashSet<>();
    public static HashMap<String, String> synonameHashMap = new HashMap<>();
    public static Map<String, String> schemaNameAgainstTableName = new HashMap<>();
    public static Map<String, String> serverDBAgainstTableName = new HashMap<>();

    public static void clearVariable() {
        jaxbContext = null;
        itemPathAndQueryMap = new HashMap();
        queryList = new ArrayList<String>();

        itemPath = null;
        obj = "";
    }

    public static void createSynonameHashMap() {
        synonameHashMap.put("1st", "First");
        synonameHashMap.put("2nd", "Second");
        synonameHashMap.put("3rd", "Third");
        synonameHashMap.put("4th", "Fourth");
        synonameHashMap.put("5th", "Fifth");
        synonameHashMap.put("6th", "Sixth");
    }

    public void getSchemaNameAgainstTableName(File xmlFile, String[] sysEnvDetail) {
        itemPathAndQueryMap.clear();
        MergeQuerySpecs.clear();
        mQueryAgainstObjectName.clear();
        FileWriter writer = null;
        createSynonameHashMap();
        try {
            clearVariable();
            LocalPackageMetadataFile localPackageMetadataFile = parseXMLFile(xmlFile);
            Items items = localPackageMetadataFile.getItems();
            String tables = "";
            List<Item> item = items.getItem();
            for (Item item2 : item) {
                itemLocation = item2.getItemLocation();
                StableEntries stableEntries = item2.getStableEntries();
                setOfLogicalTable.add(itemLocation.getItemPath().replace("Section1/", "").replaceAll("%20", " "));
                if (!stableEntries.equals(obj)) {
                    List<Entry> entry = stableEntries.getEntry();
                    for (Entry entry2 : entry) {
                        boolean check = false;
                        String finalQuery = "";
                        if (entry2.getType().equals("LastAnalysisServicesFormulaText")) {
                            String query1 = entry2.getValue();
                            String schemaName = "";
                            String objectName = itemLocation.getItemPath().replace("Section1/", "").replaceAll("%20", " ").toUpperCase();
                            for (String query : query1.split("Source")) {
                                if (query.contains("{[Schema=")) {
                                    schemaName = query.split("Schema=")[1].split(",")[0].replace("\"", "").replace("\\", "");
                                    String tableName = "";
                                    try {
                                        tableName = query.split("Item=")[1].split(",")[0].substring(query.split("Item=")[1].split(",")[0].indexOf("\""), query.split("Item=")[1].split(",")[0].lastIndexOf("\\\"")).replace("\"", "").replace("\\", "");
                                    } catch (Exception ex) {
                                    }
                                    if (!tableName.equals("")) {
                                        schemaNameAgainstTableName.put(tableName, schemaName + "." + tableName);
                                    }
                                    schemaNameAgainstTableName.put(objectName, schemaName + "." + objectName);
                                }

                                if (query.contains(".Database")) {
                                    String ServerName = "";
                                    String databaseName = "";
                                    try {
                                        ServerName = query.split("\",")[0].substring(query.split(",")[0].indexOf("(") + 1);
                                        ServerName = ServerName.substring(ServerName.indexOf("\\") + 1, ServerName.length() - 1).replace("\\\\", "\\").replace("\"", "");
                                        try {
                                            databaseName = query.split("\",")[1].substring(query.split("\",")[1].indexOf("\\") + 1, query.split("\",")[1].indexOf(")")).replace("\\", "").replace(")", "").replace("\"", "").trim();
                                        } catch (StringIndexOutOfBoundsException ex) {
                                            databaseName = query.split("\",")[1].substring(query.split("\",")[1].indexOf("\\") + 1, query.split("\",")[1].lastIndexOf("\\")).replace("\\", "").replace(")", "").replace("\"", "").trim();
                                        }
                                    } catch (ArrayIndexOutOfBoundsException ex) {
                                        ServerName = "";
                                        databaseName = "";
                                    }
                                    try {
                                        String tableName = query.split(" ")[query.split(" ").length - 2].substring(query.split(" ")[query.split(" ").length - 2].indexOf("_") + 1);
                                        if (databaseName.equals("") || ServerName.equals("")) {
                                            serverDBAgainstTableName.put(tableName.toUpperCase(), "EMPTY_SERVER" + "*ERWIN*" + "EMPTY_DATABASE");
                                        } else {
                                            serverDBAgainstTableName.put(tableName.toUpperCase(), ServerName + "*ERWIN*" + databaseName);
                                        }
                                    } catch (Exception cex) {
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
            StringWriter exception = new StringWriter();
            e.printStackTrace(new PrintWriter(exception));
            exceptionLog.append("Exception In readXMLFileToObjectForQueryAndFlatFile1() \n" + exception.toString());
            exceptionLog.append("\n ================================");
        }
    }

    public void searchSQLQueries(String query, String reportName) {
        String rePlace = "";
        if (query.contains("]")) {

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
            query = query.replace("&quot;", "\"");
            query = query.replace("\\u0027", "'").replace("\\u003e", "> ").replace("\"", "").replace("\\u003c", "< ");
            query = query.replace("#(lf)", "\n");
            query = query.replace("#(tab)", "\t");
            query = query.replace("=\\", "");
            query = query.replace("\\\\", "\"").replace("\\", " ").trim();
            itemPath = itemLocation.getItemPath();
            itemPath = reportName + "." + itemPath.replace("Section1/", "").replaceAll("%20", " ").toUpperCase();
            if (itemPathAndQueryMap.get(itemPath) != null) {
                itemPathAndQueryMap.put(itemPath, itemPathAndQueryMap.get(itemPath) + "#ERWIN#" + query);
            } else {
                itemPathAndQueryMap.put(itemPath, query);
            }
        } else if (query.contains(";")) {
            int firstIndex = query.indexOf("[Query=");
            rePlace = "[Query=";
            int lastIndex = query.lastIndexOf(";");
            if (firstIndex == -1 || lastIndex == -1) {
                firstIndex = query.indexOf("Query=");
                lastIndex = query.lastIndexOf(";");
                if (firstIndex == -1 || lastIndex == -1) {
                    return;
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
            itemPath = itemLocation.getItemPath();
            itemPath = reportName + "." + itemPath.replace("Section1/", "").replaceAll("%20", " ").toUpperCase();
            if (itemPathAndQueryMap.get(itemPath) != null) {
                itemPathAndQueryMap.put(itemPath.toUpperCase(), itemPathAndQueryMap.get(itemPath) + "#ERWIN#" + query);
            } else {
                itemPathAndQueryMap.put(itemPath, query);
            }
        }

    }

    public void searchFiles(String[] splitString, String reportName) {
        for (int i = 0; i < splitString.length; i++) {
            if (splitString[i].contains("Folder.Files") || splitString[i].toLowerCase().contains(".xlsx") || splitString[i].toLowerCase().contains(".json") || splitString[i].toLowerCase().contains(".pdf") || splitString[i].toLowerCase().contains(".csv") || splitString[i].toLowerCase().contains(".txt") || splitString[i].toLowerCase().contains(".xls")) {
                itemPath = itemLocation.getItemPath();
                itemPath = reportName + "." + itemPath.replace("Section1/", "").replaceAll("%20", " ");
                String flatFileName = "";
                if (splitString[i].contains("Excel.Workbook")) {
                    flatFileName = splitString[i].replace("\\u0027", "'").substring(splitString[i].lastIndexOf("\\\\") + 2).replace("\\", "").replace(")", "").replace("\"", "");
                    String filePath = splitString[i].replace(flatFileName, "");
                    filePath = filePath.substring(filePath.indexOf("\""), filePath.lastIndexOf("\"")).replace("\"", "");
                    String totalPath = "PBI_" + itemPath + "(.XLSX)#erwin@" + filePath + "\\" + flatFileName.replace("\\\\", "\\").replace("\\\\", "\\");
                    itemPathAndxlPath.put(itemPath.toUpperCase(), totalPath);
                    itemPathAndQueryMap.put(itemPath.toUpperCase(), flatFileName);
                } else if (splitString[i].contains("Csv.Document")) {
                    String csvFile = splitString[i].replace("\\\"", "\"").replace("\\\\", "\\");
                    String folderPath = csvFile.substring(csvFile.indexOf("\""), csvFile.lastIndexOf("\\") + 1);
                    csvFile = csvFile.substring(csvFile.lastIndexOf("\\") + 1, csvFile.lastIndexOf("\""));
                    String totalPath = "PBI_" + itemPath + "(.CSV)#erwin@" + folderPath + "\\" + csvFile;
                    itemPathAndxlPath.put(itemPath.toUpperCase(), totalPath);
                    itemPathAndQueryMap.put(itemPath.toUpperCase(), csvFile);
                } else if (splitString[i].contains("Folder.Files")) {
                    String folderFiles = splitString[i].replace("\\\\", "\\").replace("\\\"", "\"");
                    String folderPath = folderFiles.substring(folderFiles.indexOf("\"") + 1, folderFiles.lastIndexOf("\\") + 1);
                    folderFiles = folderFiles.substring(folderFiles.lastIndexOf("\\") + 1, folderFiles.lastIndexOf("\""));
                    String totalPath = "PBI_" + itemPath + "(.FLATFILES)#erwin@" + folderPath + "\\" + folderFiles;
                    itemPathAndxlPath.put(itemPath.toUpperCase(), totalPath);
                    itemPathAndQueryMap.put(itemPath.toUpperCase(), folderFiles + "#@ERWIN@#TEMP_TAB");
                } else if (splitString[i].toLowerCase().contains(itemPath.toLowerCase())) {
                    if (splitString[i].toLowerCase().contains(".xlsx")) {
                        if (splitString[i].contains("Name")) {
                            flatFileName = splitString[i].split("Name")[1].replace("=", "").replace("\\", "");
                            itemPathAndQueryMap.put(itemPath.toUpperCase(), flatFileName);
                        } else if (splitString[i].contains("Excel.Workbook")) {
                            flatFileName = splitString[i].substring(splitString[i].lastIndexOf("\\\\") + 2).replace("\\", "").replace(")", "").replace("\"", "");
                            itemPathAndQueryMap.put(itemPath.toUpperCase(), flatFileName);
                        }
                    } else if (splitString[i].toLowerCase().contains(".json")) {
                        flatFileName = itemPath + ".json";
                        itemPathAndQueryMap.put(itemPath.toUpperCase(), flatFileName);
                    } else if (splitString[i].toLowerCase().contains(".csv")) {
                        flatFileName = itemPath + ".csv";
                        itemPathAndQueryMap.put(itemPath.toUpperCase(), flatFileName);
                    } else if (splitString[i].toLowerCase().contains(".pdf")) {
                        flatFileName = itemPath + ".pdf";
                        itemPathAndQueryMap.put(itemPath.toUpperCase(), flatFileName);
                    } else if (splitString[i].toLowerCase().contains(".txt")) {
                        flatFileName = itemPath + ".txt";
                        itemPathAndQueryMap.put(itemPath.toUpperCase(), flatFileName);
                    } else if (splitString[i].toLowerCase().contains(".xml")) {
                        flatFileName = itemPath + ".xml";
                        itemPathAndQueryMap.put(itemPath.toUpperCase(), flatFileName);
                    } else if (splitString[i].toLowerCase().contains(".xls")) {
                        flatFileName = itemPath + ".xls";
                        itemPathAndQueryMap.put(itemPath.toUpperCase().toUpperCase(), flatFileName);
                    }
                } else {
                    if (splitString[i].contains("Name")) {
                        if (splitString[i].toLowerCase().contains(".xlsx")) {
                            flatFileName = splitString[i].split("Name")[1].replace("=", "").replace("\\", "");
                            itemPathAndQueryMap.put(itemPath.toUpperCase(), flatFileName);
                        } else if (splitString[i].toLowerCase().contains(".json")) {
                            flatFileName = splitString[i].split("Name")[1].replace("=", "").replace("\\", "");
                            itemPathAndQueryMap.put(itemPath.toUpperCase(), flatFileName);
                        } else if (splitString[i].toLowerCase().contains(".csv")) {
                            flatFileName = splitString[i].split("Name")[1].replace("=", "").replace("\\", "");
                            itemPathAndQueryMap.put(itemPath.toUpperCase(), flatFileName);
                        } else if (splitString[i].toLowerCase().contains(".pdf")) {
                            flatFileName = splitString[i].split("Name")[1].replace("=", "").replace("\\", "");
                            itemPathAndQueryMap.put(itemPath.toUpperCase(), flatFileName);
                        } else if (splitString[i].toLowerCase().contains(".txt")) {
                            flatFileName = splitString[i].split("Name")[1].replace("=", "").replace("\\", "");
                            itemPathAndQueryMap.put(itemPath.toUpperCase(), flatFileName);
                        } else if (splitString[i].toLowerCase().contains(".xml")) {
                            flatFileName = splitString[i].split("Name")[1].replace("=", "").replace("\\", "");
                            itemPathAndQueryMap.put(itemPath.toUpperCase().toUpperCase(), flatFileName);
                        } else if (splitString[i].toLowerCase().contains(".xls")) {
                            flatFileName = splitString[i].split("Name")[1].replace("=", "").replace("\\", "");
                            itemPathAndQueryMap.put(itemPath.toUpperCase(), flatFileName);
                        }
                    }
                }
            }
        }
    }

    public LocalPackageMetadataFile parseXMLFile(File xmlFile) {
        FileWriter writer = null;
        try {
            String name = xmlFile.getAbsolutePath();
            String readFileToString = FileUtils.readFileToString(xmlFile);
            String startTag = "<LocalPackageMetadataFile ";
            String endTag = "</LocalPackageMetadataFile>";
            int indexStartTag = 0;
            int indexendTag = 0;
            indexStartTag = readFileToString.indexOf(startTag);
            indexendTag = readFileToString.indexOf(endTag);
            String properXmlFile = readFileToString.substring(indexStartTag, indexendTag);
            properXmlFile = properXmlFile.concat("</LocalPackageMetadataFile>");
            File file = new File(name + "copy.xml");
            writer = new FileWriter(file);
            writer.append(properXmlFile);
            try {
                if (writer != null) {
                    writer.close();
                }
            } catch (Exception e) {
                logger.error(e.getMessage());
            }
            jaxbContext = JAXBContext.newInstance(LocalPackageMetadataFile.class);
            Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
            return (LocalPackageMetadataFile) jaxbUnmarshaller.unmarshal(file);
        } catch (Exception ex) {

        }
        return null;
    }

    public void getServerInfo(String query, String objectName) {
        String dbType = query.substring(query.indexOf("=") + 1, query.indexOf(".Database")).trim();
        String ServerName = "";
        String databaseName = "";
        try {
            ServerName = query.split("\",")[0].substring(query.split(",")[0].indexOf("(") + 1);
            ServerName = ServerName.substring(ServerName.indexOf("\\") + 1, ServerName.length() - 1).replace("\\\\", "\\").replace("\"", "");
            if (ServerName.toUpperCase().contains("EXEC ") || ServerName.toUpperCase().contains(", ")) {
                ServerName = query.split(", ")[0].split("\\(")[1];
            }
            try {
                databaseName = query.split("\",")[1].substring(query.split("\",")[1].indexOf("\\") + 1, query.split("\",")[1].indexOf(")")).replace("\\", "").replace(")", "").replace("\"", "").trim();
            } catch (StringIndexOutOfBoundsException ex) {
                databaseName = query.split("\",")[1].substring(query.split("\",")[1].indexOf("\\") + 1, query.split("\",")[1].lastIndexOf("\\")).replace("\\", "").replace(")", "").replace("\"", "").trim();
            } catch (ArrayIndexOutOfBoundsException ae) {
                databaseName = query.split(", ")[1];
            }
            if (databaseName.contains("exec ")) {
                databaseName = query.split(", ")[1].replace("\\\"", "").trim();
            }
        } catch (ArrayIndexOutOfBoundsException ex) {
            ServerName = "";
            databaseName = "";
        }
        if (!PowerBIReportParser.xmlDBMap.containsKey(objectName)) {
            if ((databaseName.equals("") || ServerName.equals(""))) {
                PowerBIReportParser.xmlDBMap.put(objectName, dbType + "*ERWIN*" + "EMPTY_SERVER" + "*ERWIN*" + "EMPTY_DATABASE");
            } else {
                PowerBIReportParser.xmlDBMap.put(objectName, dbType + "*ERWIN*" + ServerName + "*ERWIN*" + databaseName);
            }
        }
    }

    public Map<String, String> readXMLFileToObjectForQueryAndFlatFile1(File xmlFile, String[] sysEnvDetail, String zipFileName) throws IOException {
        schemaNameAgainstTableName.clear();
        getSchemaNameAgainstTableName(xmlFile, sysEnvDetail);
        itemPathAndQueryMap.clear();
        MergeQuerySpecs.clear();
        mQueryAgainstObjectName.clear();
        createSynonameHashMap();
        clearVariable();
        try {
            String name = xmlFile.getAbsolutePath();
            LocalPackageMetadataFile localPackageMetadataFile = parseXMLFile(xmlFile);
            Items items = localPackageMetadataFile.getItems();
            String tables = "";
            List<Item> item = items.getItem();
            for (Item item2 : item) {
                itemLocation = item2.getItemLocation();
                StableEntries stableEntries = item2.getStableEntries();
                setOfLogicalTable.add(itemLocation.getItemPath().replace("Section1/", "").replaceAll("%20", " "));
                if (!stableEntries.equals(obj)) {
                    List<Entry> entry = stableEntries.getEntry();
                    for (Entry entry2 : entry) {
                        boolean check = false;
                        String finalQuery = "";
                        if (entry2.getType().equals("LastAnalysisServicesFormulaText")) {
                            String query1 = entry2.getValue();
                            String schemaName = "";
                            String objectName = zipFileName + "." + itemLocation.getItemPath().replace("Section1/", "").replaceAll("%20", " ").toUpperCase().replaceAll("[^a-zA-Z0-9_\\s]", "");
                            for (String query : query1.split("Source")) {
                                if (query.contains(".Database")) {
                                    getServerInfo(query, objectName);
                                }
                                if (query.contains("{[Schema=")) {
                                    schemaName = query.split("Schema=")[1].split(",")[0].replace("\"", "").replace("\\", "");
                                    String tableName = "";
                                    try {
                                        tableName = query.split("Item=")[1].split(",")[0].substring(query.split("Item=")[1].split(",")[0].indexOf("\""), query.split("Item=")[1].split(",")[0].lastIndexOf("\\\"")).replace("\"", "").replace("\\", "");
                                    } catch (Exception ex) {
                                    }

                                    if (!tableName.equals("")) {
                                        schemaNameAgainstTableName.put(tableName, schemaName + "." + tableName);
                                    }
                                    schemaNameAgainstTableName.put(objectName, schemaName + "." + objectName);
                                }
                                parsePowerBIDAX(query, sysEnvDetail, schemaName, name, zipFileName);
                                if ((query.toLowerCase().contains("select") || query.toLowerCase().contains("with")) && (!query.toLowerCase().contains("exec ") && !query.toLowerCase().contains("exe "))) {
                                    searchSQLQueries(query, zipFileName);
                                } else {
                                    String[] splitString = query.split(",");
                                    if (query.contains("Folder.Files") || query.toLowerCase().contains(".xlsx") || query.toLowerCase().contains(".json") || query.toLowerCase().contains(".pdf") || query.toLowerCase().contains(".csv") || query.toLowerCase().contains(".txt") || query.toLowerCase().contains(".xls")) {
                                        searchFiles(splitString, zipFileName);
                                    } else {
                                        searchProcedures(schemaName, splitString, zipFileName);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
            StringWriter exception = new StringWriter();
            e.printStackTrace(new PrintWriter(exception));
            exceptionLog.append("Exception In readXMLFileToObjectForQueryAndFlatFile1() \n").append(exception.toString());
            exceptionLog.append("\n ================================");
        }
        return itemPathAndQueryMap;
    }

    public void searchProcedures(String schemaName, String[] splitString, String reportName) {

        String databaseName = "";
        String procedureName = "";
        schemaName = "";

        for (int i = 0; i < splitString.length; i++) {
            itemPath = itemLocation.getItemPath();
            itemPath = reportName + "." + itemPath.replace("Section1/", "").replaceAll("%20", " ").replaceAll("[^a-zA-Z0-9_\\s]", "");

            if (splitString[i].toUpperCase().contains("EXEC ")) {
                try {
                    procedureName = splitString[i].replace("#(lf)", "").toUpperCase().substring(splitString[i].indexOf("["), splitString[i].lastIndexOf("]") + 1).split("EXEC")[1].replace("[", "").replace("]", "").replace(";", "").replace("\\", "").trim().replaceAll("[^a-zA-Z0-9_\\s.]", "");
                } catch (Exception ex) {
                    procedureName = splitString[i].replace("#(lf)", "").toUpperCase().split("EXEC")[1].replace("[", "").replace("\\u0027", "'").replace("]", "").replace(";", "").replace("\\", "").trim().replaceAll("[^a-zA-Z0-9_\\s.]", "");
                }
                if (procedureName.trim().contains(" ")) {
                    procedureName = procedureName.split(" ")[0];
                }
            } else if (splitString[i].contains("EXE ")) {
                procedureName = splitString[i].replace("#(lf)", "").split("EXE")[1].replace("[", "").replace("]", "").replace(";", "").replace("\\", "").trim().replaceAll("[^a-zA-Z0-9_\\s.]", "");
            } else if (splitString[i].contains("Query=") && !splitString[i].contains("IsParameterQuery=")) {
                procedureName = splitString[i].replace("#(lf)", "").split("Query=")[1].replace("[", "").replace("]", "").replace(";", "").replace("\\", "").trim().replaceAll("[^a-zA-Z0-9_\\s.]", "");
            }

        }

        if ("".equals(procedureName)) {
            return;
        } else {
            if (procedureName.split("\\.").length == 3) {
                procedureName = procedureName.split("\\.")[2] + "." + procedureName.split("\\.")[3];
            }
            itemPathAndQueryMap.put(itemPath.toUpperCase(), procedureName + "#@ERWIN@#PROC");
        }
    }

    public static MappingSpecificationRow getMapSpecForMergeQuery(String sourceTable, String sourceColumn, String targetTable, String targetColumn, String[] sysEnvDetail, String reportName) {
        MappingSpecificationRow mapspec = new MappingSpecificationRow();
        String sourceSystemName = sysEnvDetail[0];
        String sourceSystemEnvironmentName = sysEnvDetail[1];
        String targetSystemName = sysEnvDetail[0];
        String targetSystemEnvironmentName = sysEnvDetail[1];

        if (schemaNameAgainstTableName.containsKey(sourceTable.toUpperCase())) {
            sourceTable = schemaNameAgainstTableName.get(sourceTable.toUpperCase());
            String sourceSysEnvInfo1 = SyncMetadataJsonFileDesign.newmetasync(sourceTable, "", "", PowerBIReportParser.metadataJsonPath, sourceSystemName, sourceSystemEnvironmentName, PowerBIReportParser.cacheMap, PowerBIReportParser.allTablesMap, PowerBIReportParser.allDBMap, "", "", "");
            sourceSystemName = sourceSysEnvInfo1.split("##")[0];
            sourceSystemEnvironmentName = sourceSysEnvInfo1.split("##")[1];
            sourceTable = sourceSysEnvInfo1.split("##")[2];
        } else {
            sourceTable = reportName + "." + sourceTable;
        }
        mapspec.setSourceSystemName(sourceSystemName);
        mapspec.setSourceSystemEnvironmentName(sourceSystemEnvironmentName);
        mapspec.setSourceTableName(sourceTable);
        mapspec.setSourceColumnName(sourceColumn);
        mapspec.setTargetSystemName(targetSystemName);
        mapspec.setTargetSystemEnvironmentName(targetSystemEnvironmentName);
        mapspec.setTargetTableName(targetTable);
        mapspec.setTargetColumnName(targetColumn);

        return mapspec;
    }

    public static ArrayList<MappingSpecificationRow> getMappingSpecForExpandedColumns(String sourceTable, String intermediateTable, String targetTable, String expandedColumns, String[] sysEnvDetail, String sourceColumn, String targetColumn, String reportName) {
        MappingSpecificationRow mapspec = new MappingSpecificationRow();
        ArrayList<MappingSpecificationRow> specs = new ArrayList<>();
        String sourceSystemName = sysEnvDetail[0];
        String sourceSystemEnvironmentName = sysEnvDetail[1];
        String targetSystemName = sysEnvDetail[0];
        String targetSystemEnvironmentName = sysEnvDetail[1];
        if (schemaNameAgainstTableName.containsKey(sourceTable.toUpperCase())) {
            sourceTable = schemaNameAgainstTableName.get(sourceTable.toUpperCase());
        } else {
            sourceTable = reportName + "." + sourceTable;
        }
        if (!intermediateTable.equals("")) {
            String intermediateSystem = sysEnvDetail[2];
            String intermediateEnvironment = sysEnvDetail[3];
            if (schemaNameAgainstTableName.containsKey(intermediateTable.toUpperCase())) {
                intermediateTable = schemaNameAgainstTableName.get(intermediateTable.toUpperCase());
            } else {
                intermediateTable = reportName + "." + intermediateTable;
            }
            mapspec.setSourceSystemName(sourceSystemName);
            mapspec.setSourceSystemEnvironmentName(sourceSystemEnvironmentName);
            mapspec.setSourceTableName(sourceTable);
            mapspec.setSourceColumnName(sourceColumn);
            mapspec.setTargetSystemName(intermediateSystem);
            mapspec.setTargetSystemEnvironmentName(intermediateEnvironment);
            mapspec.setTargetTableName(intermediateTable);
            mapspec.setTargetColumnName(targetColumn);
            mapspec = new MappingSpecificationRow();
            mapspec.setSourceSystemName(intermediateSystem);
            mapspec.setSourceSystemEnvironmentName(intermediateEnvironment);
            mapspec.setSourceTableName(intermediateTable);
            mapspec.setSourceColumnName(sourceColumn);
            mapspec.setTargetSystemName(targetSystemName);
            mapspec.setTargetSystemEnvironmentName(targetSystemEnvironmentName);
            mapspec.setTargetTableName(targetTable);
            mapspec.setTargetColumnName(targetColumn);
            specs.add(mapspec);
            for (String columnName : expandedColumns.split(", ")) {
                columnName = columnName.replace("{", "").replace("}", "").replace("\"", "").replace("\\", "").trim();
                mapspec = new MappingSpecificationRow();
                mapspec.setSourceSystemName(sourceSystemName);
                mapspec.setSourceSystemEnvironmentName(sourceSystemEnvironmentName);
                mapspec.setSourceTableName(sourceTable);
                mapspec.setSourceColumnName(columnName);
                mapspec.setTargetSystemName(intermediateSystem);
                mapspec.setTargetSystemEnvironmentName(intermediateEnvironment);
                mapspec.setTargetTableName(intermediateTable);
                mapspec.setTargetColumnName(columnName);
                specs.add(mapspec);
                mapspec = new MappingSpecificationRow();
                mapspec.setSourceSystemName(intermediateSystem);
                mapspec.setSourceSystemEnvironmentName(intermediateEnvironment);
                mapspec.setSourceTableName(intermediateTable);
                mapspec.setSourceColumnName(columnName);
                mapspec.setTargetSystemName(targetSystemName);
                mapspec.setTargetSystemEnvironmentName(targetSystemEnvironmentName);
                mapspec.setTargetTableName(targetTable);
                mapspec.setTargetColumnName(columnName);
                specs.add(mapspec);
            }
        } else {
            mapspec.setSourceSystemName(sourceSystemName);
            mapspec.setSourceSystemEnvironmentName(sourceSystemEnvironmentName);
            mapspec.setSourceTableName(sourceTable);
            mapspec.setSourceColumnName(sourceColumn);
            mapspec.setTargetSystemName(targetSystemName);
            mapspec.setTargetSystemEnvironmentName(targetSystemEnvironmentName);
            mapspec.setTargetTableName(targetTable);
            mapspec.setTargetColumnName(targetColumn);
            specs.add(mapspec);
            for (String columnName : expandedColumns.split(", ")) {
                columnName = columnName.replace("{", "").replace("}", "").replace("\"", "").replace("\\", "").trim();
                mapspec = new MappingSpecificationRow();
                mapspec.setSourceSystemName(sourceSystemName);
                mapspec.setSourceSystemEnvironmentName(sourceSystemEnvironmentName);
                mapspec.setSourceTableName(sourceTable);
                mapspec.setSourceColumnName(columnName);
                mapspec.setTargetSystemName(targetSystemName);
                mapspec.setTargetSystemEnvironmentName(targetSystemEnvironmentName);
                mapspec.setTargetTableName(targetTable);
                mapspec.setTargetColumnName(columnName);
                specs.add(mapspec);
            }
        }

        return specs;
    }

    public void select_TableColumn_DAX(String eachSpecs, String sourceTable, String[] sysEnvDetail, String reportName, String schemaName) {
        String extractedString = eachSpecs.split("=")[1].substring(eachSpecs.split("=")[1].indexOf("(") + 1, eachSpecs.split("=")[1].lastIndexOf(")"));
        String tableName = extractedString.split(",")[0].replace(schemaName + "_", "");
        String columns = extractedString.substring(extractedString.indexOf("{") + 1, extractedString.lastIndexOf("}"));
        ArrayList<MappingSpecificationRow> specs = getMappingSpecForExpandedColumns(tableName, "", sourceTable, columns, sysEnvDetail, "", "", reportName);
        if (MergeQuerySpecs.get(sourceTable) != null) {
            ArrayList<MappingSpecificationRow> mapSpecs = MergeQuerySpecs.get(sourceTable);
            mapSpecs.addAll(specs);
            MergeQuerySpecs.put(sourceTable, mapSpecs);
            if (!participatedTableAgainstMergedQuery.get(sourceTable).contains(tableName)) {
                participatedTableAgainstMergedQuery.put(sourceTable, participatedTableAgainstMergedQuery.get(sourceTable) + "#ERWIN#" + tableName);
            }
        } else {
            ArrayList<MappingSpecificationRow> mapSpecs = new ArrayList<>();
            mapSpecs.addAll(specs);
            MergeQuerySpecs.put(sourceTable, mapSpecs);
            participatedTableAgainstMergedQuery.put(sourceTable, tableName);
        }
    }

    public void powerBI_Expanded_DAX(String customQuery, String sourceTable, String targetTable3, String[] sysEnvDetail, String reportName) {
        String expandedSourceColumns = customQuery.split("=")[1];
        expandedSourceColumns = expandedSourceColumns.substring(expandedSourceColumns.indexOf("{\"") + 2, expandedSourceColumns.indexOf("\"}"));
        ArrayList<MappingSpecificationRow> specs = getMappingSpecForExpandedColumns(targetTable3, "", sourceTable, expandedSourceColumns, sysEnvDetail, "", "", reportName);
        if (MergeQuerySpecs.get(sourceTable) != null) {
            ArrayList<MappingSpecificationRow> mapSpecs = MergeQuerySpecs.get(sourceTable);
            mapSpecs.addAll(specs);
            MergeQuerySpecs.put(sourceTable, mapSpecs);
            if (!participatedTableAgainstMergedQuery.get(sourceTable).contains(targetTable3)) {
                participatedTableAgainstMergedQuery.put(sourceTable, participatedTableAgainstMergedQuery.get(sourceTable) + "#ERWIN#" + targetTable3);
            }
        } else {
            ArrayList<MappingSpecificationRow> mapSpecs = new ArrayList<>();
            MergeQuerySpecs.put(sourceTable, mapSpecs);
            participatedTableAgainstMergedQuery.put(sourceTable, targetTable3);
        }
    }

    public void powerBI_Merge_DAX(String eachSpecs, String[] customQueryArray, int spec_index, String sourceTable, String[] sysEnvDetail, String reportName) {

        String targetTable3 = eachSpecs.split("=")[0].split(" ")[eachSpecs.split("=")[0].split(" ").length - 1].replace("\"", "");
        String targetTable4 = eachSpecs.split("=")[0].split(" ")[eachSpecs.split("=")[0].split(" ").length - 2].replace("\"", "") + " " + eachSpecs.split("=")[0].split(" ")[eachSpecs.split("=")[0].split(" ").length - 1].replace("\"", "");
        if (customQueryArray[spec_index + 1].split("=")[0].contains("Expanded " + targetTable3.trim())) {
            powerBI_Expanded_DAX(customQueryArray[spec_index + 1], sourceTable, targetTable3, sysEnvDetail, reportName);
        } else if (customQueryArray[spec_index + 1].split("=")[0].contains("Expanded " + targetTable4.trim()) && DataMashupExtractor.queriesAgainstLogicalTable.containsKey(targetTable4.trim())) {
            String expandedSourceColumns = customQueryArray[spec_index + 1].split("=")[1];
            expandedSourceColumns = expandedSourceColumns.substring(expandedSourceColumns.indexOf("{"), expandedSourceColumns.indexOf("}"));
            ArrayList<MappingSpecificationRow> specs = getMappingSpecForExpandedColumns(targetTable4, "", sourceTable, expandedSourceColumns, sysEnvDetail, "", "", reportName);
            if (MergeQuerySpecs.get(sourceTable) != null) {
                ArrayList<MappingSpecificationRow> mapSpecs = MergeQuerySpecs.get(sourceTable);
                mapSpecs.addAll(specs);
                MergeQuerySpecs.put(sourceTable, mapSpecs);
                if (!participatedTableAgainstMergedQuery.get(sourceTable).contains(targetTable4)) {
                    participatedTableAgainstMergedQuery.put(sourceTable, participatedTableAgainstMergedQuery.get(sourceTable) + "#ERWIN#" + targetTable4);
                }
            } else {
                ArrayList<MappingSpecificationRow> mapSpecs = new ArrayList<>();
                mapSpecs.addAll(specs);
                MergeQuerySpecs.put(sourceTable, mapSpecs);
                participatedTableAgainstMergedQuery.put(sourceTable, targetTable4);
            }
        }
        if (synonameHashMap.containsKey(eachSpecs.split("=")[0].split(" ")[eachSpecs.split("=")[0].split(" ").length - 2].replace("\"", ""))) {
            String temp = eachSpecs.split("=")[0].replace("Merged ", "").replace("\"", "").replace("#", "").trim();
            String targetTable5 = synonameHashMap.get(eachSpecs.split("=")[0].split(" ")[eachSpecs.split("=")[0].split(" ").length - 2].replace("\"", "")) + " " + eachSpecs.split("=")[0].split(" ")[eachSpecs.split("=")[0].split(" ").length - 1].replace("\"", "");
            if (customQueryArray[spec_index + 1].split("=")[0].contains("Expanded " + temp.trim()) && (DataMashupExtractor.queriesAgainstLogicalTable.containsKey(targetTable5.trim()) || DataMashupExtractor.queriesAgainstLogicalTable.containsKey((targetTable5.replace(" ", "_").trim() + "s").toUpperCase()))) {
                if (DataMashupExtractor.queriesAgainstLogicalTable.containsKey((targetTable5.replace(" ", "_").trim() + "s").toUpperCase())) {
                    targetTable5 = (targetTable5.replace(" ", "_").trim() + "s").toUpperCase();
                }
                String expandedSourceColumns = customQueryArray[spec_index + 1].split("=")[1];
                expandedSourceColumns = expandedSourceColumns.substring(expandedSourceColumns.indexOf("{"), expandedSourceColumns.indexOf("}"));
                ArrayList<MappingSpecificationRow> specs = getMappingSpecForExpandedColumns(targetTable5, "", sourceTable, expandedSourceColumns, sysEnvDetail, "", "", reportName);
                if (MergeQuerySpecs.get(sourceTable) != null) {
                    ArrayList<MappingSpecificationRow> mapSpecs = MergeQuerySpecs.get(sourceTable);
                    mapSpecs.addAll(specs);
                    MergeQuerySpecs.put(sourceTable, mapSpecs);
                    if (!participatedTableAgainstMergedQuery.get(sourceTable).contains(targetTable5)) {
                        participatedTableAgainstMergedQuery.put(sourceTable, participatedTableAgainstMergedQuery.get(sourceTable) + "#ERWIN#" + targetTable5);
                    }
                } else {
                    ArrayList<MappingSpecificationRow> mapSpecs = new ArrayList<>();
                    mapSpecs.addAll(specs);
                    MergeQuerySpecs.put(sourceTable, mapSpecs);
                    participatedTableAgainstMergedQuery.put(sourceTable, targetTable5);
                }
            }
        }
    }

    public void powerBI_Rename_DAX(String eachSpecs, String[] sysEnvDetail, String targetTable, String sourceTable, String reportName) {

        String[] value = new String[0];
        try {
            value = eachSpecs.split("=")[1].split(",");
        } catch (Exception ex) {

        }
        ArrayList<MappingSpecificationRow> specs = new ArrayList<>();
        for (int renamedIndex = 1; renamedIndex < value.length; renamedIndex++) {
            try {
                String sourceCol = value[renamedIndex].replace("{", "").replace("}", "").replace("\"", "").replace("(", "").replace(")", "");
                renamedIndex++;
                String targetCol = value[renamedIndex].replace("{", "").replace("}", "").replace("\"", "").replace("(", "").replace(")", "");
                MappingSpecificationRow row = new MappingSpecificationRow();
                String sourceSystemName = sysEnvDetail[0];
                String sourceSystemEnvironmentName = sysEnvDetail[1];
                String targetSystemName = sysEnvDetail[0];
                String targetSystemEnvironmentName = sysEnvDetail[1];
                if (targetTable.contains(".") && !targetTable.equals("")) {
                    targetTable = targetTable.split("\\.")[1];
                }
                if (!targetTable.equals("")) {
                    row.setSourceSystemName(sourceSystemName);
                    row.setSourceSystemEnvironmentName(sourceSystemEnvironmentName);
                    row.setTargetTableName(sourceTable);
                    row.setSourceColumnName(sourceCol);
                    row.setTargetSystemName(targetSystemName);
                    row.setTargetSystemEnvironmentName(targetSystemEnvironmentName);
                    row.setSourceTableName((reportName + "." + targetTable).toUpperCase());
                    row.setTargetColumnName(targetCol);
                    specs.add(row);
                }
            } catch (ArrayIndexOutOfBoundsException aex) {
            }
        }
        if (MergeQuerySpecs.get(sourceTable.toUpperCase()) != null) {
            ArrayList<MappingSpecificationRow> mapSpecs = MergeQuerySpecs.get(sourceTable.toUpperCase());
            mapSpecs.addAll(specs);
            MergeQuerySpecs.put(sourceTable, mapSpecs);
            if (!participatedTableAgainstMergedQuery.get(sourceTable).contains(sourceTable.toUpperCase())) {
                participatedTableAgainstMergedQuery.put(sourceTable, participatedTableAgainstMergedQuery.get(sourceTable) + "#ERWIN#" + targetTable.toUpperCase());
            }
        } else {
            ArrayList<MappingSpecificationRow> mapSpecs = new ArrayList<>();
            mapSpecs.addAll(specs);
            MergeQuerySpecs.put(sourceTable, mapSpecs);
            participatedTableAgainstMergedQuery.put(sourceTable, targetTable.toUpperCase());
        }
    }

    public void powerBI_Added_DAX(String eachSpecs, String[] sysEnvDetail, String targetTable, String sourceTable, String reportName) {
        String value = eachSpecs.split("=")[1].split(",")[1].replace("\"", "").trim();
        MappingSpecificationRow row = new MappingSpecificationRow();
        String sourceSystemName = sysEnvDetail[0];
        String sourceSystemEnvironmentName = sysEnvDetail[1];
        String targetSystemName = sysEnvDetail[0];
        String targetSystemEnvironmentName = sysEnvDetail[1];
        if (targetTable.contains(".")) {
            targetTable = targetTable.split("\\.")[1];
        }
        row.setSourceSystemName(sourceSystemName);
        row.setSourceSystemEnvironmentName(sourceSystemEnvironmentName);
        row.setTargetTableName(sourceTable);
        row.setSourceColumnName(value);
        row.setTargetSystemName(targetSystemName);
        row.setTargetSystemEnvironmentName(targetSystemEnvironmentName);
        row.setSourceTableName((reportName + "." + targetTable).toUpperCase());
        row.setTargetColumnName(value);
        if (MergeQuerySpecs.get(sourceTable) != null) {
            ArrayList<MappingSpecificationRow> mapSpecs = MergeQuerySpecs.get(sourceTable);
            mapSpecs.add(row);
            MergeQuerySpecs.put(sourceTable, mapSpecs);
            if (!participatedTableAgainstMergedQuery.get(sourceTable).contains(sourceTable)) {
                participatedTableAgainstMergedQuery.put(sourceTable, participatedTableAgainstMergedQuery.get(sourceTable) + "#ERWIN#" + targetTable);
            }
        } else {
            ArrayList<MappingSpecificationRow> mapSpecs = new ArrayList<>();
            mapSpecs.add(row);
            MergeQuerySpecs.put(sourceTable.toUpperCase(), mapSpecs);
            participatedTableAgainstMergedQuery.put(sourceTable.toUpperCase(), targetTable.toUpperCase());
        }
    }

    public void powerBI_Replace_DAX(String eachSpecs, String[] sysEnvDetail, String targetTable, String sourceTable, String reportName) {
        String[] value = new String[0];
        try {
            value = eachSpecs.split("=")[1].split(",");
        } catch (Exception ex) {

        }
        ArrayList<MappingSpecificationRow> specs = new ArrayList<>();
        try {
            String sourceCol = value[value.length - 1].replace("{", "").replace("}", "").replace("\"", "");
            String targetCol = value[value.length - 1].replace("{", "").replace("}", "").replace("\"", "");
            MappingSpecificationRow row = new MappingSpecificationRow();
            String sourceSystemName = sysEnvDetail[0];
            String sourceSystemEnvironmentName = sysEnvDetail[1];
            String targetSystemName = sysEnvDetail[0];
            String targetSystemEnvironmentName = sysEnvDetail[1];
            if (targetTable.contains(".") && !targetTable.equals("")) {
                targetTable = targetTable.split("\\.")[1];
            }
            if (!targetTable.equals("")) {
                row.setSourceSystemName(sourceSystemName);
                row.setSourceSystemEnvironmentName(sourceSystemEnvironmentName);
                row.setTargetTableName(sourceTable);
                row.setSourceColumnName(sourceCol);
                row.setTargetSystemName(targetSystemName);
                row.setTargetSystemEnvironmentName(targetSystemEnvironmentName);
                row.setSourceTableName((reportName + "." + targetTable).toUpperCase());
                row.setTargetColumnName(targetCol);
                specs.add(row);
            }
        } catch (ArrayIndexOutOfBoundsException aex) {
        }
        if (MergeQuerySpecs.get(sourceTable) != null) {
            ArrayList<MappingSpecificationRow> mapSpecs = MergeQuerySpecs.get(sourceTable.toUpperCase());
            mapSpecs.addAll(specs);
            MergeQuerySpecs.put(sourceTable.toUpperCase(), mapSpecs);
            if (!participatedTableAgainstMergedQuery.get(sourceTable.toUpperCase()).contains(sourceTable.toUpperCase())) {
                participatedTableAgainstMergedQuery.put(sourceTable.toUpperCase(), participatedTableAgainstMergedQuery.get(sourceTable.toUpperCase()) + "#ERWIN#" + targetTable.toUpperCase());
            }
        } else {
            ArrayList<MappingSpecificationRow> mapSpecs = new ArrayList<>();
            mapSpecs.addAll(specs);
            MergeQuerySpecs.put(sourceTable.toUpperCase(), mapSpecs);
            participatedTableAgainstMergedQuery.put(sourceTable.toUpperCase(), targetTable.toUpperCase());
        }

    }

    public void powerBI_Changed_DAX(String eachSpecs, String[] sysEnvDetail, String targetTable, String sourceTable, String reportName) {

        String[] value = new String[0];
        try {
            value = eachSpecs.split("=")[1].split(",");
        } catch (Exception ex) {

        }
        ArrayList<MappingSpecificationRow> specs = new ArrayList<>();
        try {
            String sourceCol = value[value.length - 2].replace("{", "").replace("}", "").replace("\"", "");
            String targetCol = value[value.length - 2].replace("{", "").replace("}", "").replace("\"", "");
            MappingSpecificationRow row = new MappingSpecificationRow();
            String sourceSystemName = sysEnvDetail[0];
            String sourceSystemEnvironmentName = sysEnvDetail[1];
            String targetSystemName = sysEnvDetail[0];
            String targetSystemEnvironmentName = sysEnvDetail[1];
            row.setSourceSystemName(sourceSystemName);
            row.setSourceSystemEnvironmentName(sourceSystemEnvironmentName);
            row.setTargetTableName(sourceTable);
            row.setSourceColumnName(sourceCol);
            row.setTargetSystemName(targetSystemName);
            row.setTargetSystemEnvironmentName(targetSystemEnvironmentName);
            row.setSourceTableName((reportName + "." + targetTable).toUpperCase());
            row.setTargetColumnName(targetCol);
            specs.add(row);
        } catch (ArrayIndexOutOfBoundsException aex) {
        }
        if (MergeQuerySpecs.get(sourceTable.toUpperCase()) != null) {
            ArrayList<MappingSpecificationRow> mapSpecs = MergeQuerySpecs.get(sourceTable.toUpperCase());
            mapSpecs.addAll(specs);
            MergeQuerySpecs.put(sourceTable.toUpperCase(), mapSpecs);
            if (!participatedTableAgainstMergedQuery.get(sourceTable).contains(sourceTable.toUpperCase())) {
                participatedTableAgainstMergedQuery.put(sourceTable.toUpperCase(), participatedTableAgainstMergedQuery.get(sourceTable.toUpperCase()) + "#ERWIN#" + targetTable.toUpperCase());
            }
        } else {
            ArrayList<MappingSpecificationRow> mapSpecs = new ArrayList<>();
            mapSpecs.addAll(specs);
            MergeQuerySpecs.put(sourceTable.toUpperCase(), mapSpecs);
            participatedTableAgainstMergedQuery.put(sourceTable.toUpperCase(), targetTable.toUpperCase());
        }
    }

    public void powerBI_Merged_Renamed_DAX(String[] customQueryArray, int spec_index, String sourceTable1, String sourceTable, String sourceColumn2, String targetColumn, String[] sysEnvDetail, String reportName) {

        if (customQueryArray[spec_index + 1].split("=")[0].contains("Renamed Columns")) {
            String renameInfo = customQueryArray[spec_index + 1].split("=")[1].trim();
            renameInfo = renameInfo.substring(renameInfo.lastIndexOf("{"), renameInfo.indexOf("}"));
            String sourceTable3 = renameInfo.split(",")[0].replace("{", "").replace("}", "").replace("\"", "").replace("\\", "").trim();
            if (sourceTable3.trim().equals(sourceTable1)) {
                String targetTable3 = renameInfo.split(",")[1].replace("{", "").replace("}", "").replace("\"", "").replace("\\", "").trim();
                if (customQueryArray[spec_index + 2].split("=")[0].contains("Expanded " + targetTable3.trim())) {
                    String expandedSourceColumns = customQueryArray[spec_index + 2].split("=")[1];
                    expandedSourceColumns = expandedSourceColumns.substring(expandedSourceColumns.indexOf("{"), expandedSourceColumns.indexOf("}"));
                    ArrayList<MappingSpecificationRow> specs = getMappingSpecForExpandedColumns(sourceTable1, targetTable3, sourceTable, expandedSourceColumns, sysEnvDetail, sourceColumn2, targetColumn, reportName);
                    if (MergeQuerySpecs.get(sourceTable.toUpperCase()) != null) {
                        ArrayList<MappingSpecificationRow> mapSpecs = MergeQuerySpecs.get(sourceTable.toUpperCase());
                        mapSpecs.addAll(specs);
                        MergeQuerySpecs.put(sourceTable.toUpperCase(), mapSpecs);
                        if (!participatedTableAgainstMergedQuery.get(sourceTable.toUpperCase()).contains(sourceTable1.toUpperCase())) {
                            participatedTableAgainstMergedQuery.put(sourceTable.toUpperCase(), participatedTableAgainstMergedQuery.get(sourceTable.toUpperCase()) + "#ERWIN#" + sourceTable1.toUpperCase());
                        }
                    } else {
                        ArrayList<MappingSpecificationRow> mapSpecs = new ArrayList<>();
                        mapSpecs.addAll(specs);
                        MergeQuerySpecs.put(sourceTable.toUpperCase(), mapSpecs);
                        participatedTableAgainstMergedQuery.put(sourceTable, sourceTable1);
                    }
                } else {
                    MappingSpecificationRow spec = getMapSpecForMergeQuery(sourceTable1, sourceColumn2, sourceTable, targetColumn, sysEnvDetail, reportName);
                    if (MergeQuerySpecs.get(sourceTable.toUpperCase()) != null) {
                        ArrayList<MappingSpecificationRow> mapSpecs = MergeQuerySpecs.get(sourceTable.toUpperCase());
                        mapSpecs.add(spec);
                        MergeQuerySpecs.put(sourceTable.toUpperCase(), mapSpecs);
                        if (!participatedTableAgainstMergedQuery.get(sourceTable.toUpperCase()).contains(sourceTable1.toUpperCase())) {
                            participatedTableAgainstMergedQuery.put(sourceTable.toUpperCase(), participatedTableAgainstMergedQuery.get(sourceTable.toUpperCase()) + "#ERWIN#" + sourceTable1.toUpperCase());
                        }
                    } else {
                        ArrayList<MappingSpecificationRow> mapSpecs = new ArrayList<>();
                        mapSpecs.add(spec);
                        MergeQuerySpecs.put(sourceTable.toUpperCase(), mapSpecs);
                        participatedTableAgainstMergedQuery.put(sourceTable.toUpperCase(), sourceTable1.toUpperCase());
                    }
                }
            } else {
                MappingSpecificationRow spec = getMapSpecForMergeQuery(sourceTable1, sourceColumn2, sourceTable, targetColumn, sysEnvDetail, reportName);
                if (MergeQuerySpecs.get(sourceTable.toUpperCase()) != null) {
                    ArrayList<MappingSpecificationRow> mapSpecs = MergeQuerySpecs.get(sourceTable.toUpperCase());
                    mapSpecs.add(spec);
                    MergeQuerySpecs.put(sourceTable.toUpperCase().toUpperCase(), mapSpecs);
                    if (!participatedTableAgainstMergedQuery.get(sourceTable.toUpperCase().toUpperCase()).contains(sourceTable1.toUpperCase())) {
                        participatedTableAgainstMergedQuery.put(sourceTable.toUpperCase(), participatedTableAgainstMergedQuery.get(sourceTable.toUpperCase()) + "#ERWIN#" + sourceTable1.toUpperCase());
                    }
                } else {
                    ArrayList<MappingSpecificationRow> mapSpecs = new ArrayList<>();
                    mapSpecs.add(spec);
                    MergeQuerySpecs.put(sourceTable.toUpperCase(), mapSpecs);
                    participatedTableAgainstMergedQuery.put(sourceTable.toUpperCase(), sourceTable1.toUpperCase());
                }
            }
        }
    }

    public void powerBI_Merged_Expanded_DAX(String[] customQueryArray, int spec_index, String sourceTable1, String sourceTable, String[] sysEnvDetail, String sourceColumn2, String targetColumn, String reportName) {
        String expandedSourceColumns = customQueryArray[spec_index + 1].split("=")[1];
        expandedSourceColumns = expandedSourceColumns.substring(expandedSourceColumns.indexOf("{"), expandedSourceColumns.indexOf("}"));
        ArrayList<MappingSpecificationRow> specs = getMappingSpecForExpandedColumns(sourceTable1, "", sourceTable, expandedSourceColumns, sysEnvDetail, sourceColumn2, targetColumn, reportName);
        if (MergeQuerySpecs.get(sourceTable.toUpperCase()) != null) {
            ArrayList<MappingSpecificationRow> mapSpecs = MergeQuerySpecs.get(sourceTable.toUpperCase());
            mapSpecs.addAll(specs);
            MergeQuerySpecs.put(sourceTable.toUpperCase(), mapSpecs);
            if (!participatedTableAgainstMergedQuery.get(sourceTable.toUpperCase()).contains(sourceTable1.toUpperCase())) {
                participatedTableAgainstMergedQuery.put(sourceTable.toUpperCase(), participatedTableAgainstMergedQuery.get(sourceTable.toUpperCase()) + "#ERWIN#" + sourceTable1.toUpperCase());
            }
        } else {
            ArrayList<MappingSpecificationRow> mapSpecs = new ArrayList<>();
            mapSpecs.addAll(specs);
            MergeQuerySpecs.put(sourceTable.toUpperCase(), mapSpecs);
            participatedTableAgainstMergedQuery.put(sourceTable.toUpperCase(), sourceTable1);
        }
    }

    public void parsePowerBIDAX(String query, String[] sysEnvDetail, String schemaName, String name, String reportName) {
        if (query.contains("Merged Queries") || query.contains("Expanded ")) {
            String sourceTable = reportName + "." + itemLocation.getItemPath().replace("Section1/", "").replaceAll("%20", " ").toUpperCase();
            if (mQueryAgainstObjectName.get(sourceTable) != null) {
                mQueryAgainstObjectName.put(sourceTable, mQueryAgainstObjectName.get(sourceTable) + "<\br>" + query);
            } else {
                mQueryAgainstObjectName.put(sourceTable, query);
            }
            String[] customQueryArray = null;
            String query2 = DataMashupExtractor.queriesAgainstLogicalTable.get(sourceTable);
            String targetTable = "";
            if (query2 != null) {
                query = query2;
                customQueryArray = query.split("\n");
            } else {
                customQueryArray = query.split("\\\\r\\\\n #");
            }
            try {
                targetTable = customQueryArray[1].split("=")[1].replace("#", "").replace("\"", "").replace(",", "").trim();
            } catch (ArrayIndexOutOfBoundsException aex) {

            }
            if (!targetTable.toLowerCase().contains("merged columns") && !targetTable.toLowerCase().contains("added custom") && !targetTable.toLowerCase().contains("table.expandtablecolumn") && !targetTable.toLowerCase().contains("table.transformcolumntypes") && !targetTable.toLowerCase().contains("table.combinecolumns") && !targetTable.toLowerCase().contains("table.nestedjoin") && !targetTable.toLowerCase().contains("table.nestedjoin") && !targetTable.toLowerCase().contains("table.renamecolumns") && !targetTable.toLowerCase().contains("renamed columns")) {
                for (int spec_index = 0; spec_index < customQueryArray.length; spec_index++) {
                    String eachSpecs = customQueryArray[spec_index];
                    if (eachSpecs.split("=").length > 1 && eachSpecs.split("=")[1].contains("Table.SelectColumns")) {
                        select_TableColumn_DAX(eachSpecs, sourceTable, sysEnvDetail, reportName, schemaName);
                    }
                    if (eachSpecs.split("=")[0].contains("Merged ") && !eachSpecs.contains("Merged Queries")) {
                        powerBI_Merge_DAX(eachSpecs, customQueryArray, spec_index, sourceTable, sysEnvDetail, reportName);
                    } else if (eachSpecs.split("=")[0].contains("Added ") && !targetTable.toLowerCase().contains("database") && !targetTable.equals("")) {
                        powerBI_Added_DAX(eachSpecs, sysEnvDetail, targetTable, sourceTable, reportName);
                    } else if (eachSpecs.split("=")[0].contains("Renamed ") && !customQueryArray[spec_index - 2].contains("Merged Queries") && !targetTable.toLowerCase().contains("database")) {
                        powerBI_Rename_DAX(eachSpecs, sysEnvDetail, targetTable, sourceTable, reportName);
                    } else if (eachSpecs.split("=")[0].contains("Replace ") && !customQueryArray[spec_index - 2].contains("Merged Queries") && !targetTable.toLowerCase().contains("database")) {
                        powerBI_Replace_DAX(eachSpecs, sysEnvDetail, targetTable, sourceTable, reportName);
                    } else if (eachSpecs.split("=")[0].contains("Changed ") && !targetTable.toLowerCase().contains("database")) {
                        powerBI_Changed_DAX(eachSpecs, sysEnvDetail, targetTable, sourceTable, reportName);
                    }
                    if (eachSpecs.contains("Merged Queries") && eachSpecs.split("=")[0].contains("Merged Queries")) {
                        try {
                            String join = eachSpecs.split("=")[1].trim();
                            join = join.substring(join.indexOf("(") + 1, join.indexOf(")"));
                            String targetColumn = join.split(",")[1].replace("{", "").replace("}", "").replace("\\\"", "").trim();
                            String sourceTable1 = join.split(",")[2].replace(name, eachSpecs).replace("#", "").replace("\\\"", "").replace("\\u0027s", "").trim();
                            String sourceColumn2 = join.split(",")[3].replace("{", "").replace("}", "").replace("\\\"", "").trim();
                            if (customQueryArray[spec_index + 1].contains("Renamed Columns")) {
                                powerBI_Merged_Renamed_DAX(customQueryArray, spec_index, sourceTable1, sourceTable, sourceColumn2, targetColumn, sysEnvDetail, reportName);
                            } else if (customQueryArray[spec_index + 1].split("=")[0].contains("Expanded " + sourceTable1.trim())) {
                                powerBI_Merged_Expanded_DAX(customQueryArray, spec_index, sourceTable1, sourceTable, sysEnvDetail, sourceColumn2, targetColumn, reportName);
                            } else {
                                MappingSpecificationRow spec = getMapSpecForMergeQuery(sourceTable1, sourceColumn2, sourceTable, targetColumn, sysEnvDetail, reportName);
                                if (MergeQuerySpecs.get(sourceTable.toUpperCase()) != null) {
                                    ArrayList<MappingSpecificationRow> mapSpecs = MergeQuerySpecs.get(sourceTable.toUpperCase());
                                    mapSpecs.add(spec);
                                    MergeQuerySpecs.put(sourceTable.toUpperCase(), mapSpecs);
                                    if (!participatedTableAgainstMergedQuery.get(sourceTable.toUpperCase()).contains(sourceTable1)) {
                                        participatedTableAgainstMergedQuery.put(sourceTable.toUpperCase(), participatedTableAgainstMergedQuery.get(sourceTable.toUpperCase()) + "#ERWIN#" + sourceTable1.toUpperCase());
                                    }
                                } else {
                                    ArrayList<MappingSpecificationRow> mapSpecs = new ArrayList<>();
                                    mapSpecs.add(spec);
                                    MergeQuerySpecs.put(sourceTable.toUpperCase(), mapSpecs);
                                    participatedTableAgainstMergedQuery.put(sourceTable.toUpperCase(), sourceTable1);
                                }
                            }
                        } catch (Exception ex) {

                        }
                    }
                }
            }
        }
    }
}
