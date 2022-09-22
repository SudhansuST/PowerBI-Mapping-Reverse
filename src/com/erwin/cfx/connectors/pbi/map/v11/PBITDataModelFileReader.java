package com.erwin.cfx.connectors.pbi.map.v11;

import com.erwin.cfx.connectors.pbi.map.util.SyncMetadataJsonFileDesign;
import com.erwin.cfx.connectors.pbi.map.util.ReadXMLFileToObject;
import com.ads.api.beans.mm.MappingSpecificationRow;
import com.erwin.cfx.connectors.pbi.map.pojo.PowerBI_Bean;
import com.erwin.cfx.connectors.pbi.map.sql.util.PowerBI_DAX_Parser;
import com.erwin.cfx.connectors.pbi.map.util.PowerBI_LOGGER;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.MappingJsonFactory;
import org.codehaus.jackson.node.ArrayNode;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author SudhansuTarai
 */
public class PBITDataModelFileReader {

    public static Map<String, String> expressionAgainstColumnMap = new HashMap<>();
    public static Map<String, String> datatypeAgainstColumnMap = new HashMap<>();
    public static Map<String, String> descriptionAgainstColumnMap = new HashMap<>();
    public static Map<String, String> expressionAgainstMeasureMap = new HashMap<>();
    public static Map<String, String> relationshipMap = new HashMap<>();
    public static Map<String, String> calculatedTablesMeasures = new HashMap<>();
    public static Map<String, String> itemPathAndQueryMap = new HashMap<>();
    public static Map<String, Set<String>> columnsAgainstLogicalTable = new HashMap<>();
    public static Map<String, String> queryAgainstParameter = new HashMap<>();
    static ArrayList<String> queryString = new ArrayList<>();
    public static String snowflakeQuery = "";

    public static void selectedColumn_DAX(String eachSpecs, String schemaName, String sourceTable, String[] sysEnvDetail, String reportName) {
        String extractedString = eachSpecs.split("=")[1].substring(eachSpecs.split("=")[1].indexOf("(") + 1, eachSpecs.split("=")[1].lastIndexOf(")"));
        String tableName2 = extractedString.split(",")[0].replace(schemaName + "dbo_", "dbo.");
        String columns = extractedString.substring(extractedString.indexOf("{") + 1, extractedString.lastIndexOf("}"));
        ArrayList<MappingSpecificationRow> specs = ReadXMLFileToObject.getMappingSpecForExpandedColumns(tableName2, "", sourceTable, columns, sysEnvDetail, "", "", reportName);
        if (ReadXMLFileToObject.MergeQuerySpecs.get(sourceTable) != null) {
            ArrayList<MappingSpecificationRow> mapSpecs = ReadXMLFileToObject.MergeQuerySpecs.get(sourceTable);
            mapSpecs.addAll(specs);
            ReadXMLFileToObject.MergeQuerySpecs.put(sourceTable, mapSpecs);
            if (!ReadXMLFileToObject.participatedTableAgainstMergedQuery.get(sourceTable).contains(tableName2)) {
                ReadXMLFileToObject.participatedTableAgainstMergedQuery.put(sourceTable, ReadXMLFileToObject.participatedTableAgainstMergedQuery.get(sourceTable) + "#ERWIN#" + tableName2);
            }
        } else {
            ArrayList<MappingSpecificationRow> mapSpecs = new ArrayList<>();
            mapSpecs.addAll(specs);
            ReadXMLFileToObject.MergeQuerySpecs.put(sourceTable, mapSpecs);
            ReadXMLFileToObject.participatedTableAgainstMergedQuery.put(sourceTable, tableName2);
        }
    }

    public static void expanded_DAX(String eachSpecs, String sourceTable, String[] sysEnvDetail, String reportName) {
        try {
            String table1 = eachSpecs.split("=")[0].replace("Expanded ", "").replace("#", "").replace("\"", "").trim();
            String expandedSourceColumns = eachSpecs.split("=")[1].substring(eachSpecs.split("=")[1].indexOf("{") + 1, eachSpecs.split("=")[1].indexOf("}"));
            ArrayList<MappingSpecificationRow> specs = ReadXMLFileToObject.getMappingSpecForExpandedColumns(table1, "", sourceTable, expandedSourceColumns, sysEnvDetail, "", "", reportName);
            if (ReadXMLFileToObject.MergeQuerySpecs.get(sourceTable) != null) {
                ArrayList<MappingSpecificationRow> mapSpecs = ReadXMLFileToObject.MergeQuerySpecs.get(sourceTable);
                mapSpecs.addAll(specs);
                ReadXMLFileToObject.MergeQuerySpecs.put(sourceTable, mapSpecs);
                if (!ReadXMLFileToObject.participatedTableAgainstMergedQuery.get(sourceTable).contains(table1)) {
                    ReadXMLFileToObject.participatedTableAgainstMergedQuery.put(sourceTable, ReadXMLFileToObject.participatedTableAgainstMergedQuery.get(sourceTable) + "#ERWIN#" + table1);
                }
            } else {
                ArrayList<MappingSpecificationRow> mapSpecs = new ArrayList<>();
                mapSpecs.addAll(specs);
                ReadXMLFileToObject.MergeQuerySpecs.put(sourceTable, mapSpecs);
                ReadXMLFileToObject.participatedTableAgainstMergedQuery.put(sourceTable, table1);
            }
        } catch (Exception eex) {

        }
    }

    public static void transformColumn_DAX(String eachSpecs, String targetTable12, String[] customQueryArray, int spec_index, String sourceTable, String[] sysEnvDetail, String reportName) {
        String targetTable3 = eachSpecs.split("Table.TransformColumnTypes")[1].split(",")[0].replace("{", "").replace("}", "").replace("(", "").replace(")", "").replace("\"", "").replace("\"", "").trim().replace("_Table", "");
        targetTable12 = targetTable3;
        String expandedSourceColumns = eachSpecs.split("Table.TransformColumnTypes")[1].split(",")[1].replace("{", "").replace("}", "").replace("(", "").replace(")", "").replace("\"", "").replace("\"", "").trim();
        ArrayList<MappingSpecificationRow> specs = ReadXMLFileToObject.getMappingSpecForExpandedColumns(targetTable3, "", sourceTable, expandedSourceColumns, sysEnvDetail, "", "", reportName);
        if (ReadXMLFileToObject.MergeQuerySpecs.get(sourceTable) != null) {
            ArrayList<MappingSpecificationRow> mapSpecs = ReadXMLFileToObject.MergeQuerySpecs.get(sourceTable);
            mapSpecs.addAll(specs);
            ReadXMLFileToObject.MergeQuerySpecs.put(sourceTable, mapSpecs);
            if (!ReadXMLFileToObject.participatedTableAgainstMergedQuery.get(sourceTable).contains(targetTable3)) {
                ReadXMLFileToObject.participatedTableAgainstMergedQuery.put(sourceTable, ReadXMLFileToObject.participatedTableAgainstMergedQuery.get(sourceTable) + "#ERWIN#" + targetTable3);
            }
        } else {
            ArrayList<MappingSpecificationRow> mapSpecs = new ArrayList<>();
            mapSpecs.addAll(specs);
            ReadXMLFileToObject.MergeQuerySpecs.put(sourceTable, mapSpecs);
            ReadXMLFileToObject.participatedTableAgainstMergedQuery.put(sourceTable, targetTable3);
        }
        if (customQueryArray[spec_index + 1].contains("Table.SelectRows")) {
            transform_selectRows_DAX(expandedSourceColumns, customQueryArray, spec_index, sourceTable, targetTable3, specs, sysEnvDetail, reportName);
        }
    }

    public static void transform_selectRows_DAX(String expandedSourceColumns, String[] customQueryArray, int spec_index, String sourceTable, String targetTable3, ArrayList<MappingSpecificationRow> specs, String[] sysEnvDetail, String reportName) {
        expandedSourceColumns = customQueryArray[spec_index + 1].split("Table.SelectRows")[1].split("each")[1].split("and")[0].replace("{", "").replace("}", "").replace("(", "").replace(")", "").replace("\"", "").replace("\"", "").trim();
        if (expandedSourceColumns.contains("[") && expandedSourceColumns.contains("]")) {
            expandedSourceColumns = expandedSourceColumns.substring(expandedSourceColumns.indexOf("[") + 1, expandedSourceColumns.indexOf("]"));
        }
        specs = ReadXMLFileToObject.getMappingSpecForExpandedColumns(targetTable3, "", sourceTable, expandedSourceColumns, sysEnvDetail, "", "", reportName);
        if (ReadXMLFileToObject.MergeQuerySpecs.get(sourceTable) != null) {
            ArrayList<MappingSpecificationRow> mapSpecs = ReadXMLFileToObject.MergeQuerySpecs.get(sourceTable);
            mapSpecs.addAll(specs);
            ReadXMLFileToObject.MergeQuerySpecs.put(sourceTable, mapSpecs);
            if (!ReadXMLFileToObject.participatedTableAgainstMergedQuery.get(sourceTable).contains(targetTable3)) {
                ReadXMLFileToObject.participatedTableAgainstMergedQuery.put(sourceTable, ReadXMLFileToObject.participatedTableAgainstMergedQuery.get(sourceTable) + "#ERWIN#" + targetTable3);
            }
        } else {
            ArrayList<MappingSpecificationRow> mapSpecs = new ArrayList<>();
            mapSpecs.addAll(specs);
            ReadXMLFileToObject.MergeQuerySpecs.put(sourceTable, mapSpecs);
            ReadXMLFileToObject.participatedTableAgainstMergedQuery.put(sourceTable, targetTable3);
        }
    }

    public static void table_SelectRows(String eachSpecs, String sourceTable, String targetTable12, String[] sysEnvDetail, String reportName) {
        String expandedSourceColumns = eachSpecs.split("Table.SelectRows")[1].split("each")[1].split("and")[0].replace("{", "").replace("}", "").replace("(", "").replace(")", "").replace("\"", "").replace("\"", "").trim();
        if (expandedSourceColumns.contains("[") && expandedSourceColumns.contains("]")) {
            expandedSourceColumns = expandedSourceColumns.substring(expandedSourceColumns.indexOf("["), expandedSourceColumns.indexOf("]"));
        }
        String targetTable3 = sourceTable;
        if (!targetTable12.equals("")) {
            targetTable3 = targetTable12;
        }
        ArrayList<MappingSpecificationRow> specs = ReadXMLFileToObject.getMappingSpecForExpandedColumns(targetTable3, "", sourceTable, expandedSourceColumns, sysEnvDetail, "", "", reportName);
        if (ReadXMLFileToObject.MergeQuerySpecs.get(sourceTable) != null) {
            ArrayList<MappingSpecificationRow> mapSpecs = ReadXMLFileToObject.MergeQuerySpecs.get(sourceTable);
            mapSpecs.addAll(specs);
            ReadXMLFileToObject.MergeQuerySpecs.put(sourceTable, mapSpecs);
            if (!ReadXMLFileToObject.participatedTableAgainstMergedQuery.get(sourceTable).contains(targetTable3)) {
                ReadXMLFileToObject.participatedTableAgainstMergedQuery.put(sourceTable, ReadXMLFileToObject.participatedTableAgainstMergedQuery.get(sourceTable) + "#ERWIN#" + targetTable3);
            }
        } else {
            ArrayList<MappingSpecificationRow> mapSpecs = new ArrayList<>();
            mapSpecs.addAll(specs);
            ReadXMLFileToObject.MergeQuerySpecs.put(sourceTable, mapSpecs);
            ReadXMLFileToObject.participatedTableAgainstMergedQuery.put(sourceTable, targetTable3);
        }
    }

    public static void merged_expanded_DAX(String[] customQueryArray, int spec_index, String targetTable3, String sourceTable, String reportName, String[] sysEnvDetail) {
        String expandedSourceColumns = customQueryArray[spec_index + 1].split("=")[1];
        expandedSourceColumns = expandedSourceColumns.substring(expandedSourceColumns.indexOf("{\"") + 2, expandedSourceColumns.indexOf("\"}"));
        String expandedTargetColumns = customQueryArray[spec_index + 1].split("=")[1];
        expandedTargetColumns = expandedTargetColumns.substring(expandedTargetColumns.lastIndexOf("{\"") + 2, expandedTargetColumns.lastIndexOf("\"}"));
        ArrayList<MappingSpecificationRow> specs = ReadXMLFileToObject.getMappingSpecForExpandedColumns(targetTable3, "", sourceTable, expandedSourceColumns, sysEnvDetail, "", "", reportName);
        if (ReadXMLFileToObject.MergeQuerySpecs.get(sourceTable) != null) {
            ArrayList<MappingSpecificationRow> mapSpecs = ReadXMLFileToObject.MergeQuerySpecs.get(sourceTable);
            mapSpecs.addAll(specs);
            ReadXMLFileToObject.MergeQuerySpecs.put(sourceTable, mapSpecs);
            if (!ReadXMLFileToObject.participatedTableAgainstMergedQuery.get(sourceTable).contains(targetTable3)) {
                ReadXMLFileToObject.participatedTableAgainstMergedQuery.put(sourceTable, ReadXMLFileToObject.participatedTableAgainstMergedQuery.get(sourceTable) + "#ERWIN#" + targetTable3);
            }
        } else {
            ArrayList<MappingSpecificationRow> mapSpecs = new ArrayList<>();
            mapSpecs.addAll(specs);
            ReadXMLFileToObject.MergeQuerySpecs.put(sourceTable, mapSpecs);
            ReadXMLFileToObject.participatedTableAgainstMergedQuery.put(sourceTable, targetTable3);
        }
    }

    public static void merged_expanded_DAX_1(String[] customQueryArray, int spec_index, String targetTable4, String sourceTable, String reportName, String[] sysEnvDetail) {
        String expandedSourceColumns = customQueryArray[spec_index + 1].split("=")[1];
        expandedSourceColumns = expandedSourceColumns.substring(expandedSourceColumns.indexOf("{"), expandedSourceColumns.indexOf("}"));
        ArrayList<MappingSpecificationRow> specs = ReadXMLFileToObject.getMappingSpecForExpandedColumns(targetTable4, "", sourceTable, expandedSourceColumns, sysEnvDetail, "", "", reportName);
        if (ReadXMLFileToObject.MergeQuerySpecs.get(sourceTable) != null) {
            ArrayList<MappingSpecificationRow> mapSpecs = ReadXMLFileToObject.MergeQuerySpecs.get(sourceTable);
            mapSpecs.addAll(specs);
            ReadXMLFileToObject.MergeQuerySpecs.put(sourceTable, mapSpecs);
            if (!ReadXMLFileToObject.participatedTableAgainstMergedQuery.get(sourceTable).contains(targetTable4)) {
                ReadXMLFileToObject.participatedTableAgainstMergedQuery.put(sourceTable, ReadXMLFileToObject.participatedTableAgainstMergedQuery.get(sourceTable) + "#ERWIN#" + targetTable4);
            }
        } else {
            ArrayList<MappingSpecificationRow> mapSpecs = new ArrayList<>();
            mapSpecs.addAll(specs);
            ReadXMLFileToObject.MergeQuerySpecs.put(sourceTable, mapSpecs);
            ReadXMLFileToObject.participatedTableAgainstMergedQuery.put(sourceTable, targetTable4);
        }
    }

    public static void merged_expanded_DAX_2(String[] customQueryArray, int spec_index, String targetTable5, String sourceTable, String reportName, String[] sysEnvDetail) {
        if (DataMashupExtractor.queriesAgainstLogicalTable.containsKey((targetTable5.replace(" ", "_").trim() + "s").toUpperCase())) {
            targetTable5 = (targetTable5.replace(" ", "_").trim() + "s").toUpperCase();
        }
        String expandedSourceColumns = customQueryArray[spec_index + 1].split("=")[1];
        expandedSourceColumns = expandedSourceColumns.substring(expandedSourceColumns.indexOf("{"), expandedSourceColumns.indexOf("}"));
        String expandedTargetColumns = customQueryArray[spec_index + 1].split("=")[1];
        expandedTargetColumns = expandedTargetColumns.substring(expandedTargetColumns.lastIndexOf("{"), expandedTargetColumns.lastIndexOf("}"));
        ArrayList<MappingSpecificationRow> specs = ReadXMLFileToObject.getMappingSpecForExpandedColumns(targetTable5, "", sourceTable, expandedSourceColumns, sysEnvDetail, "", "", reportName);
        if (ReadXMLFileToObject.MergeQuerySpecs.get(sourceTable) != null) {
            ArrayList<MappingSpecificationRow> mapSpecs = ReadXMLFileToObject.MergeQuerySpecs.get(sourceTable);
            mapSpecs.addAll(specs);
            ReadXMLFileToObject.MergeQuerySpecs.put(sourceTable, mapSpecs);
            if (!ReadXMLFileToObject.participatedTableAgainstMergedQuery.get(sourceTable).contains(targetTable5)) {
                ReadXMLFileToObject.participatedTableAgainstMergedQuery.put(sourceTable, ReadXMLFileToObject.participatedTableAgainstMergedQuery.get(sourceTable) + "#ERWIN#" + targetTable5);
            }
        } else {
            ArrayList<MappingSpecificationRow> mapSpecs = new ArrayList<>();
            mapSpecs.addAll(specs);
            ReadXMLFileToObject.MergeQuerySpecs.put(sourceTable, mapSpecs);
            ReadXMLFileToObject.participatedTableAgainstMergedQuery.put(sourceTable, targetTable5);
        }
    }

    public static void merged_expanded_DAX_3(String[] customQueryArray, int spec_index, String sourceTable1, String sourceTable, String sourceColumn2, String targetColumn, String reportName, String[] sysEnvDetail) {
        String expandedSourceColumns = customQueryArray[spec_index + 1].split("=")[1];
        expandedSourceColumns = expandedSourceColumns.substring(expandedSourceColumns.indexOf("{"), expandedSourceColumns.indexOf("}"));
        ArrayList<MappingSpecificationRow> specs = ReadXMLFileToObject.getMappingSpecForExpandedColumns(sourceTable1, "", sourceTable, expandedSourceColumns, sysEnvDetail, sourceColumn2, targetColumn, reportName);
        if (ReadXMLFileToObject.MergeQuerySpecs.get(sourceTable.toUpperCase()) != null) {
            ArrayList<MappingSpecificationRow> mapSpecs = ReadXMLFileToObject.MergeQuerySpecs.get(sourceTable.toUpperCase());
            mapSpecs.addAll(specs);
            ReadXMLFileToObject.MergeQuerySpecs.put(sourceTable.toUpperCase(), mapSpecs);
            if (!ReadXMLFileToObject.participatedTableAgainstMergedQuery.get(sourceTable.toUpperCase()).contains(sourceTable1.toUpperCase())) {
                ReadXMLFileToObject.participatedTableAgainstMergedQuery.put(sourceTable.toUpperCase(), ReadXMLFileToObject.participatedTableAgainstMergedQuery.get(sourceTable.toUpperCase()) + "#ERWIN#" + sourceTable1.toUpperCase());
            }
        } else {
            ArrayList<MappingSpecificationRow> mapSpecs = new ArrayList<>();
            mapSpecs.addAll(specs);
            ReadXMLFileToObject.MergeQuerySpecs.put(sourceTable.toUpperCase(), mapSpecs);
            ReadXMLFileToObject.participatedTableAgainstMergedQuery.put(sourceTable.toUpperCase(), sourceTable1);
        }
    }

    public static void added_DAX(String eachSpecs, String[] sysEnvDetail, String sourceTable, String targetTable, String reportName) {
        try {
            String value = eachSpecs.split("=")[1].split(",")[1].replace("\"", "").trim();
            MappingSpecificationRow row = new MappingSpecificationRow();
            String sourceSystemName = sysEnvDetail[0];
            String sourceSystemEnvironmentName = sysEnvDetail[1];
            String targetSystemName = sysEnvDetail[0];
            String targetSystemEnvironmentName = sysEnvDetail[1];
            String sourceSysEnvInfo1 = SyncMetadataJsonFileDesign.newmetasync(sourceTable, "", "", PowerBIReportParser.metadataJsonPath, sourceSystemName, sourceSystemEnvironmentName, PowerBIReportParser.cacheMap, PowerBIReportParser.allTablesMap, PowerBIReportParser.allDBMap, "", "", "");
            sourceSystemName = sourceSysEnvInfo1.split("##")[0];
            sourceSystemEnvironmentName = sourceSysEnvInfo1.split("##")[1];
            sourceTable = sourceSysEnvInfo1.split("##")[2];
            String targetSysEnvInfo1 = SyncMetadataJsonFileDesign.newmetasync(targetTable, "", "", PowerBIReportParser.metadataJsonPath, targetSystemName, targetSystemEnvironmentName, PowerBIReportParser.cacheMap, PowerBIReportParser.allTablesMap, PowerBIReportParser.allDBMap, "", "", "");
            targetSystemName = targetSysEnvInfo1.split("##")[0];
            targetSystemEnvironmentName = targetSysEnvInfo1.split("##")[1];
            targetTable = targetSysEnvInfo1.split("##")[2];
            row.setSourceSystemName(sourceSystemName);
            row.setSourceSystemEnvironmentName(sourceSystemEnvironmentName);
            row.setTargetTableName(sourceTable);
            row.setSourceColumnName(value);
            row.setTargetSystemName(targetSystemName);
            row.setTargetSystemEnvironmentName(targetSystemEnvironmentName);
            row.setSourceTableName(reportName + "." + targetTable);
            row.setTargetColumnName(value);
            if (ReadXMLFileToObject.MergeQuerySpecs.get(sourceTable) != null) {
                ArrayList<MappingSpecificationRow> mapSpecs = ReadXMLFileToObject.MergeQuerySpecs.get(sourceTable);
                mapSpecs.add(row);
                ReadXMLFileToObject.MergeQuerySpecs.put(sourceTable, mapSpecs);
                if (!ReadXMLFileToObject.participatedTableAgainstMergedQuery.get(sourceTable).contains(sourceTable)) {
                    ReadXMLFileToObject.participatedTableAgainstMergedQuery.put(sourceTable, ReadXMLFileToObject.participatedTableAgainstMergedQuery.get(sourceTable) + "#ERWIN#" + targetTable);
                }
            } else {
                ArrayList<MappingSpecificationRow> mapSpecs = new ArrayList<>();
                mapSpecs.add(row);
                ReadXMLFileToObject.MergeQuerySpecs.put(sourceTable.toUpperCase(), mapSpecs);
                ReadXMLFileToObject.participatedTableAgainstMergedQuery.put(sourceTable.toUpperCase(), targetTable.toUpperCase());
            }
        } catch (Exception exxxx) {
            StringWriter exception = new StringWriter();
            exxxx.printStackTrace(new PrintWriter(exception));
            PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Error ::Exception In PBITDataModelFileReader.added_DAX() Method " + exception + "\n");
        }
    }

    public static void renamed_DAX(String eachSpecs, String[] sysEnvDetail, String sourceTable, String targetTable, String reportName) {
        String[] value = new String[0];
        try {
            value = eachSpecs.split("=")[1].split(",");
        } catch (Exception ex) {
            StringWriter exception = new StringWriter();
            ex.printStackTrace(new PrintWriter(exception));
//            PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Error ::Exception In PBITDataModelFileReader.renamed_DAX() Method " + exception + "\n");
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
                String sourceSysEnvInfo1 = SyncMetadataJsonFileDesign.newmetasync(sourceTable, "", "", PowerBIReportParser.metadataJsonPath, sourceSystemName, sourceSystemEnvironmentName, PowerBIReportParser.cacheMap, PowerBIReportParser.allTablesMap, PowerBIReportParser.allDBMap, "", "", "");
                sourceSystemName = sourceSysEnvInfo1.split("##")[0];
                sourceSystemEnvironmentName = sourceSysEnvInfo1.split("##")[1];
                sourceTable = sourceSysEnvInfo1.split("##")[2];
                String targetSysEnvInfo1 = SyncMetadataJsonFileDesign.newmetasync(targetTable, "", "", PowerBIReportParser.metadataJsonPath, targetSystemName, targetSystemEnvironmentName, PowerBIReportParser.cacheMap, PowerBIReportParser.allTablesMap, PowerBIReportParser.allDBMap, "", "", "");
                targetSystemName = targetSysEnvInfo1.split("##")[0];
                targetSystemEnvironmentName = targetSysEnvInfo1.split("##")[1];
                targetTable = targetSysEnvInfo1.split("##")[2];

                row.setSourceSystemName(sourceSystemName);
                row.setSourceSystemEnvironmentName(sourceSystemEnvironmentName);
                row.setTargetTableName(sourceTable);
                row.setSourceColumnName(sourceCol);
                row.setTargetSystemName(targetSystemName);
                row.setTargetSystemEnvironmentName(targetSystemEnvironmentName);
                row.setSourceTableName(reportName + "." + targetTable);
                row.setTargetColumnName(targetCol);
                specs.add(row);
            } catch (ArrayIndexOutOfBoundsException aex) {
            }
        }
        if (ReadXMLFileToObject.MergeQuerySpecs.get(sourceTable.toUpperCase()) != null) {
            ArrayList<MappingSpecificationRow> mapSpecs = ReadXMLFileToObject.MergeQuerySpecs.get(sourceTable.toUpperCase());
            mapSpecs.addAll(specs);
            ReadXMLFileToObject.MergeQuerySpecs.put(sourceTable, mapSpecs);
            if (!ReadXMLFileToObject.participatedTableAgainstMergedQuery.get(sourceTable).contains(sourceTable.toUpperCase())) {
                ReadXMLFileToObject.participatedTableAgainstMergedQuery.put(sourceTable, ReadXMLFileToObject.participatedTableAgainstMergedQuery.get(sourceTable) + "#ERWIN#" + targetTable.toUpperCase());
            }
        } else {
            ArrayList<MappingSpecificationRow> mapSpecs = new ArrayList<>();
            mapSpecs.addAll(specs);
            ReadXMLFileToObject.MergeQuerySpecs.put(sourceTable, mapSpecs);
            ReadXMLFileToObject.participatedTableAgainstMergedQuery.put(sourceTable, targetTable.toUpperCase());
        }
    }

    public static void replace_DAX(String eachSpecs, String[] sysEnvDetail, String sourceTable, String targetTable, String reportName) {
        String[] value = new String[0];
        try {
            value = eachSpecs.split("=")[1].split(",");
        } catch (Exception ex) {
            StringWriter exception = new StringWriter();
            ex.printStackTrace(new PrintWriter(exception));
            PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Error ::Exception In PBITDataModelFileReader.changed_DAX() Method " + exception + "\n");

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
            String sourceSysEnvInfo1 = SyncMetadataJsonFileDesign.newmetasync(sourceTable, "", "", PowerBIReportParser.metadataJsonPath, sourceSystemName, sourceSystemEnvironmentName, PowerBIReportParser.cacheMap, PowerBIReportParser.allTablesMap, PowerBIReportParser.allDBMap, "", "", "");
            sourceSystemName = sourceSysEnvInfo1.split("##")[0];
            sourceSystemEnvironmentName = sourceSysEnvInfo1.split("##")[1];
            sourceTable = sourceSysEnvInfo1.split("##")[2];
            String targetSysEnvInfo1 = SyncMetadataJsonFileDesign.newmetasync(targetTable, "", "", PowerBIReportParser.metadataJsonPath, targetSystemName, targetSystemEnvironmentName, PowerBIReportParser.cacheMap, PowerBIReportParser.allTablesMap, PowerBIReportParser.allDBMap, "", "", "");
            targetSystemName = targetSysEnvInfo1.split("##")[0];
            targetSystemEnvironmentName = targetSysEnvInfo1.split("##")[1];
            targetTable = targetSysEnvInfo1.split("##")[2];

            row.setSourceSystemName(sourceSystemName);
            row.setSourceSystemEnvironmentName(sourceSystemEnvironmentName);
            row.setTargetTableName(sourceTable);
            row.setSourceColumnName(sourceCol);
            row.setTargetSystemName(targetSystemName);
            row.setTargetSystemEnvironmentName(targetSystemEnvironmentName);
            row.setSourceTableName(reportName + "." + targetTable);
            row.setTargetColumnName(targetCol);
            specs.add(row);
        } catch (ArrayIndexOutOfBoundsException aex) {
            StringWriter exception = new StringWriter();
            aex.printStackTrace(new PrintWriter(exception));
            PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Error ::Exception In PBITDataModelFileReader.replace_DAX() Method " + exception + "\n");
        }
        if (ReadXMLFileToObject.MergeQuerySpecs.get(sourceTable) != null) {
            ArrayList<MappingSpecificationRow> mapSpecs = ReadXMLFileToObject.MergeQuerySpecs.get(sourceTable.toUpperCase());
            mapSpecs.addAll(specs);
            ReadXMLFileToObject.MergeQuerySpecs.put(sourceTable.toUpperCase(), mapSpecs);
            if (!ReadXMLFileToObject.participatedTableAgainstMergedQuery.get(sourceTable.toUpperCase()).contains(sourceTable.toUpperCase())) {
                ReadXMLFileToObject.participatedTableAgainstMergedQuery.put(sourceTable.toUpperCase(), ReadXMLFileToObject.participatedTableAgainstMergedQuery.get(sourceTable.toUpperCase()) + "#ERWIN#" + targetTable.toUpperCase());
            }
        } else {
            ArrayList<MappingSpecificationRow> mapSpecs = new ArrayList<>();
            mapSpecs.addAll(specs);
            ReadXMLFileToObject.MergeQuerySpecs.put(sourceTable.toUpperCase(), mapSpecs);
            ReadXMLFileToObject.participatedTableAgainstMergedQuery.put(sourceTable.toUpperCase(), targetTable.toUpperCase());
        }
    }

    public static void changed_DAX(String eachSpecs, String[] sysEnvDetail, String sourceTable, String targetTable, String reportName) {

        String[] value = new String[0];
        try {
            value = eachSpecs.split("=")[1].split(",");
        } catch (Exception ex) {
            StringWriter exception = new StringWriter();
            ex.printStackTrace(new PrintWriter(exception));
            PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Error ::Exception In PBITDataModelFileReader.changed_DAX() Method " + exception + "\n");
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
            String sourceSysEnvInfo1 = SyncMetadataJsonFileDesign.newmetasync(sourceTable, "", "", PowerBIReportParser.metadataJsonPath, sourceSystemName, sourceSystemEnvironmentName, PowerBIReportParser.cacheMap, PowerBIReportParser.allTablesMap, PowerBIReportParser.allDBMap, "", "", "");
            sourceSystemName = sourceSysEnvInfo1.split("##")[0];
            sourceSystemEnvironmentName = sourceSysEnvInfo1.split("##")[1];
            sourceTable = sourceSysEnvInfo1.split("##")[2];
            String targetSysEnvInfo1 = SyncMetadataJsonFileDesign.newmetasync(targetTable, "", "", PowerBIReportParser.metadataJsonPath, targetSystemName, targetSystemEnvironmentName, PowerBIReportParser.cacheMap, PowerBIReportParser.allTablesMap, PowerBIReportParser.allDBMap, "", "", "");
            targetSystemName = targetSysEnvInfo1.split("##")[0];
            targetSystemEnvironmentName = targetSysEnvInfo1.split("##")[1];
            targetTable = targetSysEnvInfo1.split("##")[2];

            row.setSourceSystemName(sourceSystemName);
            row.setSourceSystemEnvironmentName(sourceSystemEnvironmentName);
            row.setTargetTableName(sourceTable);
            row.setSourceColumnName(sourceCol);
            row.setTargetSystemName(targetSystemName);
            row.setTargetSystemEnvironmentName(targetSystemEnvironmentName);
            row.setSourceTableName(reportName + "." + targetTable);
            row.setTargetColumnName(targetCol);
            specs.add(row);
        } catch (ArrayIndexOutOfBoundsException aex) {
            StringWriter exception = new StringWriter();
            aex.printStackTrace(new PrintWriter(exception));
            PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Error ::Exception In PBITDataModelFileReader.changed_DAX() Method " + exception + "\n");
        }

//                                            }
        if (ReadXMLFileToObject.MergeQuerySpecs.get(sourceTable.toUpperCase()) != null) {
            ArrayList<MappingSpecificationRow> mapSpecs = ReadXMLFileToObject.MergeQuerySpecs.get(sourceTable.toUpperCase());
            mapSpecs.addAll(specs);
            ReadXMLFileToObject.MergeQuerySpecs.put(sourceTable.toUpperCase(), mapSpecs);
            if (!ReadXMLFileToObject.participatedTableAgainstMergedQuery.get(sourceTable).contains(sourceTable.toUpperCase())) {
                ReadXMLFileToObject.participatedTableAgainstMergedQuery.put(sourceTable.toUpperCase(), ReadXMLFileToObject.participatedTableAgainstMergedQuery.get(sourceTable.toUpperCase()) + "#ERWIN#" + targetTable.toUpperCase());
            }
        } else {
            ArrayList<MappingSpecificationRow> mapSpecs = new ArrayList<>();
            mapSpecs.addAll(specs);
            ReadXMLFileToObject.MergeQuerySpecs.put(sourceTable.toUpperCase(), mapSpecs);
            ReadXMLFileToObject.participatedTableAgainstMergedQuery.put(sourceTable.toUpperCase(), targetTable.toUpperCase());
        }
    }

    public static void prepareMeasureInfos(JsonNode jsonTableNode, String tableName) {
//        PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Info ::Getting reoprt measures information\n");
        try {
            if (jsonTableNode.has("columns")) {
                JSONArray columnArray = new JSONArray(jsonTableNode.get("columns").toString());
                for (int j = 0; j < columnArray.length(); j++) {
                    JSONObject columnJSON = columnArray.getJSONObject(j);
                    if (columnJSON.has("expression")) {
                        expressionAgainstColumnMap.put(tableName + "@ERWIN@" + columnJSON.getString("name"), columnJSON.getString("expression"));
                    }
                    if (columnJSON.has("description")) {
                        descriptionAgainstColumnMap.put(tableName + "@ERWIN@" + columnJSON.getString("name"), columnJSON.getString("description"));
                    }
                    if (columnJSON.has("dataType")) {
                        int type = 0;
                        if (columnJSON.getString("dataType").equals("string")) {
                            type = 2048;
                        } else if (columnJSON.getString("dataType").equals("dateTime")) {
                            type = 4;
                        } else if (columnJSON.getString("dataType").equals("int64")) {
                            type = 3;
                        }
                        datatypeAgainstColumnMap.put(tableName + "@ERWIN@" + columnJSON.getString("name"), columnJSON.getString("dataType"));
                    }
                }
            }
            if (jsonTableNode.has("measures")) {
                JSONArray measuresJSONArray = new JSONArray(jsonTableNode.get("measures").toString());
                for (int j = 0; j < measuresJSONArray.length(); j++) {
                    JSONObject measureJSON = measuresJSONArray.getJSONObject(j);
                    if (measureJSON.has("expression")) {
                        expressionAgainstMeasureMap.put(tableName + "@ERWIN@" + measureJSON.getString("name"), measureJSON.getString("expression"));
                    }
                    if (measureJSON.has("dataType")) {
                        int type = 0;
                        datatypeAgainstColumnMap.put(tableName + "@ERWIN@" + measureJSON.getString("name"), measureJSON.getString("dataType"));
                    }
                }
            }
        } catch (Exception ex) {
            StringWriter exception = new StringWriter();
            ex.printStackTrace(new PrintWriter(exception));
            PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Error ::Exception In PBITDataModelFileReader.prepareMeasureInfos() Method " + exception + "\n");
        }
    }

    public static void prepareSQLMetadataInfos(String query, String tableName, String schemaName) {

        if (query.contains(".Database")) {
            String dbType = "";
            try {
                dbType = query.substring(query.indexOf("=") + 1, query.indexOf(".Database")).trim();
            } catch (Exception ex) {
                dbType = query.split(".Database")[0].trim();
            }
            String ServerName = "";
            String databaseName = "";
            try {
                ServerName = query.split("\",")[0].substring(query.split("\\,")[0].indexOf("(") + 1);
                if (ServerName.indexOf("\\") != ServerName.indexOf("\\ODS")) {
                    ServerName = ServerName.substring(ServerName.indexOf("\\") + 1, ServerName.length() - 1).replace("\\\\", "\\").replace("\"", "");
                }
                if (ServerName.contains(",")) {
                    ServerName = ServerName.split("\\,")[0];
                }
                ServerName = ServerName.replace("\"", "");
                if (queryAgainstParameter.containsKey(ServerName)) {
                    ServerName = queryAgainstParameter.get(ServerName);
                    ServerName = ServerName.split("DefaultValue=")[1].split("\\,")[0].replace("\"", "");
                }
                try {
                    databaseName = query.split("\",")[1].substring(query.split("\",")[1].indexOf("\\") + 1, query.split("\",")[1].indexOf(")")).replace("\\", "").replace(")", "").replace("\"", "").trim();
                } catch (StringIndexOutOfBoundsException ex) {
                    databaseName = query.split("\",")[1].replace("\"", "");
                } catch (ArrayIndexOutOfBoundsException ae) {
                    databaseName = query.split("\\,")[1].replace("\"", "").trim();
                }
                if (databaseName.toLowerCase().contains("exec ")) {
                    databaseName = query.split("\\,")[1].replace("\"", "").trim();
                }
                if (databaseName.contains(",")) {
                    databaseName = query.split("\\,")[1].replace("\"", "").trim();
                }
                databaseName = databaseName.replaceAll("[^a-zA-Z0-9_\\s]", "");
                if (queryAgainstParameter.containsKey(databaseName)) {
                    databaseName = queryAgainstParameter.get(databaseName);
                    databaseName = databaseName.split("DefaultValue=")[1].split("\\,")[0].replace("\"", "");
                }
            } catch (Exception ex) {
                ServerName = "";
                databaseName = "";
            }
            if (databaseName.equals("") || ServerName.equals("")) {
                PowerBIReportParser.xmlDBMap.put(tableName, dbType + "*ERWIN*" + "EMPTY_SERVER" + "*ERWIN*" + "EMPTY_DATABASE");
            } else {
                PowerBIReportParser.xmlDBMap.put(tableName, dbType + "*ERWIN*" + ServerName + "*ERWIN*" + databaseName);
            }
        }
        if (query.contains("{[Schema=")) {
            schemaName = query.split("Schema=")[1].split(",")[0].replace("\"", "").replace("\\", "");
            String tableName1 = "";
            try {
                tableName1 = query.split("Item=")[1].split(",")[0].substring(query.split("Item=")[1].split(",")[0].indexOf("\""), query.split("Item=")[1].split(",")[0].lastIndexOf("\\\"")).replace("\"", "").replace("\\", "");
            } catch (Exception ex) {
            }

            if (!tableName1.equals("")) {
                ReadXMLFileToObject.schemaNameAgainstTableName.put(tableName1, schemaName + "." + tableName1);
            }
            ReadXMLFileToObject.schemaNameAgainstTableName.put(tableName, schemaName + "." + tableName);
        }
    }

    public static void parsePowerBI_DAX(String query, String expression, String tableName, String schemaName, String reportName, String[] sysEnvDetail) {
        if (query.contains("Merged Queries") || query.contains("Expanded ") || query.contains("#\"Modificato ") || query.contains("#\"Filtrate ") || query.contains("#\"Merge ")) {
            String sourceTable = tableName;
            if (ReadXMLFileToObject.mQueryAgainstObjectName.get(sourceTable) != null) {
                ReadXMLFileToObject.mQueryAgainstObjectName.put(sourceTable, ReadXMLFileToObject.mQueryAgainstObjectName.get(sourceTable) + "<\br>" + query);
            } else {
                ReadXMLFileToObject.mQueryAgainstObjectName.put(sourceTable, query);
            }
            String[] customQueryArray = new String[1];
            String query2 = expression;
            String targetTable = "";
            customQueryArray = query2.split("\n");
            try {
                targetTable = customQueryArray[1].split("=")[1].replace("#", "").replace("\"", "").replace(",", "").trim();
            } catch (ArrayIndexOutOfBoundsException aex) {

            }
            if (!targetTable.toLowerCase().contains("merged columns") && !targetTable.toLowerCase().contains("added custom") && !targetTable.toLowerCase().contains("table.expandtablecolumn") && !targetTable.toLowerCase().contains("table.transformcolumntypes") && !targetTable.toLowerCase().contains("table.combinecolumns") && !targetTable.toLowerCase().contains("table.nestedjoin") && !targetTable.toLowerCase().contains("table.nestedjoin") && !targetTable.toLowerCase().contains("table.renamecolumns") && !targetTable.toLowerCase().contains("renamed columns")) {
                for (int spec_index = 0; spec_index < customQueryArray.length; spec_index++) {
                    String eachSpecs = customQueryArray[spec_index];
                    if (eachSpecs.contains("Kind=\"Table\"")) {
                        sourceTable = eachSpecs.split("=")[0].trim();
                    } else {
                        sourceTable = tableName;
                    }
                    if (eachSpecs.split("=").length > 1 && eachSpecs.split("=")[1].contains("Table.SelectColumns")) {
                        selectedColumn_DAX(eachSpecs, schemaName, sourceTable, sysEnvDetail, reportName);
                    }
                    String targetTable12 = "";
                    if (eachSpecs.split("=")[0].contains("Expanded ")) {
                        expanded_DAX(eachSpecs, sourceTable, sysEnvDetail, reportName);
                    }
                    if (eachSpecs.contains("Table.TransformColumnTypes")) {
                        transformColumn_DAX(eachSpecs, targetTable12, customQueryArray, spec_index, sourceTable, sysEnvDetail, reportName);
                    }
                    if (eachSpecs.contains("Table.SelectRows")) {
                        table_SelectRows(eachSpecs, sourceTable, targetTable12, sysEnvDetail, reportName);
                    }
                    if (eachSpecs.split("=")[0].contains("Merged ") && !eachSpecs.contains("Merged Queries")) {
                        merged_DAX(eachSpecs, customQueryArray, spec_index, sourceTable, reportName, sysEnvDetail);
                    } else if (eachSpecs.split("=")[0].contains("Added ") && !targetTable.toLowerCase().contains("database")) {
                        added_DAX(eachSpecs, sysEnvDetail, sourceTable, targetTable, reportName);
                    } else if (eachSpecs.split("=")[0].contains("Renamed ") && !customQueryArray[spec_index - 2].contains("Merged Queries") && !targetTable.toLowerCase().contains("database")) {
                        renamed_DAX(eachSpecs, sysEnvDetail, sourceTable, targetTable, reportName);
                    } else if (eachSpecs.split("=")[0].contains("Replace ") && !customQueryArray[spec_index - 2].contains("Merged Queries") && !targetTable.toLowerCase().contains("database")) {
                        replace_DAX(eachSpecs, sysEnvDetail, sourceTable, targetTable, reportName);
                    } else if (eachSpecs.split("=")[0].contains("Changed ") && !targetTable.toLowerCase().contains("database")) {
                        changed_DAX(eachSpecs, sysEnvDetail, sourceTable, targetTable, reportName);
                    }
                    if (eachSpecs.contains("Merged Queries")) {
                        mergedQuery_DAX(eachSpecs, customQueryArray, spec_index, sourceTable, reportName, sysEnvDetail);
                    }

                }
            }
        }
    }

    public static void merged_DAX(String eachSpecs, String[] customQueryArray, int spec_index, String sourceTable, String reportName, String[] sysEnvDetail) {
        String pbikey = eachSpecs.split("=")[0];
        String targetTable3 = eachSpecs.split("=")[0].split(" ")[eachSpecs.split("=")[0].split(" ").length - 1].replace("\"", "");
        String targetTable4 = eachSpecs.split("=")[0].split(" ")[eachSpecs.split("=")[0].split(" ").length - 2].replace("\"", "") + " " + eachSpecs.split("=")[0].split(" ")[eachSpecs.split("=")[0].split(" ").length - 1].replace("\"", "");
        if (customQueryArray[spec_index + 1].split("=")[0].contains("Expanded " + targetTable3.trim())) {
            merged_expanded_DAX(customQueryArray, spec_index, targetTable3, sourceTable, reportName, sysEnvDetail);
        } else if (customQueryArray[spec_index + 1].split("=")[0].contains("Expanded " + targetTable4.trim()) && DataMashupExtractor.queriesAgainstLogicalTable.containsKey(targetTable4.trim())) {
            merged_expanded_DAX_1(customQueryArray, spec_index, targetTable4, sourceTable, reportName, sysEnvDetail);
        }
        if (ReadXMLFileToObject.synonameHashMap.containsKey(eachSpecs.split("=")[0].split(" ")[eachSpecs.split("=")[0].split(" ").length - 2].replace("\"", ""))) {
            String temp = eachSpecs.split("=")[0].replace("Merged ", "").replace("\"", "").replace("#", "").trim();
            String targetTable5 = ReadXMLFileToObject.synonameHashMap.get(eachSpecs.split("=")[0].split(" ")[eachSpecs.split("=")[0].split(" ").length - 2].replace("\"", "")) + " " + eachSpecs.split("=")[0].split(" ")[eachSpecs.split("=")[0].split(" ").length - 1].replace("\"", "");
            if (customQueryArray[spec_index + 1].split("=")[0].contains("Expanded " + temp.trim()) && (DataMashupExtractor.queriesAgainstLogicalTable.containsKey(targetTable5.trim()) || DataMashupExtractor.queriesAgainstLogicalTable.containsKey((targetTable5.replace(" ", "_").trim() + "s").toUpperCase()))) {
                merged_expanded_DAX_2(customQueryArray, spec_index, targetTable5, sourceTable, reportName, sysEnvDetail);
            }
        }
    }

    public static void mergedQuery_DAX(String eachSpecs, String[] customQueryArray, int spec_index, String sourceTable, String reportName, String[] sysEnvDetail) {
        if (eachSpecs.split("=")[0].contains("Merged Queries")) {
            try {
                String join = eachSpecs.split("=")[1].trim();
                join = join.substring(join.indexOf("(") + 1, join.indexOf(")"));
                String targetColumn = join.split(",")[1].replace("{", "").replace("}", "").replace("\\\"", "").trim();
                String sourceTable1 = join.split(",")[2].replace("#", "").replace("\\\"", "").replace("\\u0027s", "").trim();
                String sourceColumn2 = join.split(",")[3].replace("{", "").replace("}", "").replace("\\\"", "").trim();
                String joinType = join.split(",")[5].split("\\.")[1].trim();
                String tables = "";
                if (customQueryArray[spec_index + 1].contains("Renamed Columns")) {
                    merged_renamed_DAX(customQueryArray, spec_index, sourceTable, sourceTable1, sourceColumn2, targetColumn, reportName, sysEnvDetail);
                } else if (customQueryArray[spec_index + 1].split("=")[0].contains("Expanded " + sourceTable1.trim())) {
                    merged_expanded_DAX_3(customQueryArray, spec_index, sourceTable1, sourceTable, sourceColumn2, targetColumn, reportName, sysEnvDetail);
                } else {
                    MappingSpecificationRow spec = ReadXMLFileToObject.getMapSpecForMergeQuery(sourceTable1, sourceColumn2, sourceTable, targetColumn, sysEnvDetail, reportName);
                    if (ReadXMLFileToObject.MergeQuerySpecs.get(sourceTable.toUpperCase()) != null) {
                        ArrayList<MappingSpecificationRow> mapSpecs = ReadXMLFileToObject.MergeQuerySpecs.get(sourceTable.toUpperCase());
                        mapSpecs.add(spec);
                        ReadXMLFileToObject.MergeQuerySpecs.put(sourceTable.toUpperCase(), mapSpecs);
                        if (!ReadXMLFileToObject.participatedTableAgainstMergedQuery.get(sourceTable.toUpperCase()).contains(sourceTable1)) {
                            ReadXMLFileToObject.participatedTableAgainstMergedQuery.put(sourceTable.toUpperCase(), ReadXMLFileToObject.participatedTableAgainstMergedQuery.get(sourceTable.toUpperCase()) + "#ERWIN#" + sourceTable1.toUpperCase());
                        }
                    } else {
                        ArrayList<MappingSpecificationRow> mapSpecs = new ArrayList<>();
                        mapSpecs.add(spec);
                        ReadXMLFileToObject.MergeQuerySpecs.put(sourceTable.toUpperCase(), mapSpecs);
                        ReadXMLFileToObject.participatedTableAgainstMergedQuery.put(sourceTable.toUpperCase(), sourceTable1);
                    }
                }
            } catch (Exception ex) {
                StringWriter exception = new StringWriter();
                ex.printStackTrace(new PrintWriter(exception));
//                PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Error ::Exception In PBITDataModelFileReader.mergedQuery_DAX() Method " + exception + "\n");
            }

        }
    }

    public static void merged_renamed_DAX(String[] customQueryArray, int spec_index, String sourceTable, String sourceTable1, String sourceColumn2, String targetColumn, String reportName, String[] sysEnvDetail) {
        if (customQueryArray[spec_index + 1].split("=")[0].contains("Renamed Columns")) {
            String renameInfo = customQueryArray[spec_index + 1].split("=")[1].trim();
            renameInfo = renameInfo.substring(renameInfo.lastIndexOf("{"), renameInfo.indexOf("}"));
            String sourceTable3 = renameInfo.split(",")[0].replace("{", "").replace("}", "").replace("\"", "").replace("\\", "").trim();
            if (sourceTable3.trim().equals(sourceTable1)) {
                String targetTable3 = renameInfo.split(",")[1].replace("{", "").replace("}", "").replace("\"", "").replace("\\", "").trim();
                if (customQueryArray[spec_index + 2].split("=")[0].contains("Expanded " + targetTable3.trim())) {
                    String expandedSourceColumns = customQueryArray[spec_index + 2].split("=")[1];
                    expandedSourceColumns = expandedSourceColumns.substring(expandedSourceColumns.indexOf("{"), expandedSourceColumns.indexOf("}"));
                    String expandedTargetColumns = customQueryArray[spec_index + 2].split("=")[1];
                    expandedTargetColumns = expandedTargetColumns.substring(expandedTargetColumns.lastIndexOf("{"), expandedTargetColumns.lastIndexOf("}"));
                    ArrayList<MappingSpecificationRow> specs = ReadXMLFileToObject.getMappingSpecForExpandedColumns(sourceTable1, targetTable3, sourceTable, expandedSourceColumns, sysEnvDetail, sourceColumn2, targetColumn, reportName);
                    if (ReadXMLFileToObject.MergeQuerySpecs.get(sourceTable.toUpperCase()) != null) {
                        ArrayList<MappingSpecificationRow> mapSpecs = ReadXMLFileToObject.MergeQuerySpecs.get(sourceTable.toUpperCase());
                        mapSpecs.addAll(specs);
                        ReadXMLFileToObject.MergeQuerySpecs.put(sourceTable.toUpperCase(), mapSpecs);
                        if (!ReadXMLFileToObject.participatedTableAgainstMergedQuery.get(sourceTable.toUpperCase()).contains(sourceTable1.toUpperCase())) {
                            ReadXMLFileToObject.participatedTableAgainstMergedQuery.put(sourceTable.toUpperCase(), ReadXMLFileToObject.participatedTableAgainstMergedQuery.get(sourceTable.toUpperCase()) + "#ERWIN#" + sourceTable1.toUpperCase());
                        }
                    } else {
                        ArrayList<MappingSpecificationRow> mapSpecs = new ArrayList<>();
                        mapSpecs.addAll(specs);
                        ReadXMLFileToObject.MergeQuerySpecs.put(sourceTable.toUpperCase(), mapSpecs);
                        ReadXMLFileToObject.participatedTableAgainstMergedQuery.put(sourceTable, sourceTable1);
                    }
                } else {
                    MappingSpecificationRow spec = ReadXMLFileToObject.getMapSpecForMergeQuery(sourceTable1, sourceColumn2, sourceTable, targetColumn, sysEnvDetail, reportName);
                    if (ReadXMLFileToObject.MergeQuerySpecs.get(sourceTable.toUpperCase()) != null) {
                        ArrayList<MappingSpecificationRow> mapSpecs = ReadXMLFileToObject.MergeQuerySpecs.get(sourceTable.toUpperCase());
                        mapSpecs.add(spec);
                        ReadXMLFileToObject.MergeQuerySpecs.put(sourceTable.toUpperCase(), mapSpecs);
                        if (!ReadXMLFileToObject.participatedTableAgainstMergedQuery.get(sourceTable.toUpperCase()).contains(sourceTable1.toUpperCase())) {
                            ReadXMLFileToObject.participatedTableAgainstMergedQuery.put(sourceTable.toUpperCase(), ReadXMLFileToObject.participatedTableAgainstMergedQuery.get(sourceTable.toUpperCase()) + "#ERWIN#" + sourceTable1.toUpperCase());
                        }
                    } else {
                        ArrayList<MappingSpecificationRow> mapSpecs = new ArrayList<>();
                        mapSpecs.add(spec);
                        ReadXMLFileToObject.MergeQuerySpecs.put(sourceTable.toUpperCase(), mapSpecs);
                        ReadXMLFileToObject.participatedTableAgainstMergedQuery.put(sourceTable.toUpperCase(), sourceTable1.toUpperCase());
                    }
                }
            } else {
                MappingSpecificationRow spec = ReadXMLFileToObject.getMapSpecForMergeQuery(sourceTable1, sourceColumn2, sourceTable, targetColumn, sysEnvDetail, reportName);
                if (ReadXMLFileToObject.MergeQuerySpecs.get(sourceTable.toUpperCase()) != null) {
                    ArrayList<MappingSpecificationRow> mapSpecs = ReadXMLFileToObject.MergeQuerySpecs.get(sourceTable.toUpperCase());
                    mapSpecs.add(spec);
                    ReadXMLFileToObject.MergeQuerySpecs.put(sourceTable.toUpperCase().toUpperCase(), mapSpecs);
                    if (!ReadXMLFileToObject.participatedTableAgainstMergedQuery.get(sourceTable.toUpperCase().toUpperCase()).contains(sourceTable1.toUpperCase())) {
                        ReadXMLFileToObject.participatedTableAgainstMergedQuery.put(sourceTable.toUpperCase(), ReadXMLFileToObject.participatedTableAgainstMergedQuery.get(sourceTable.toUpperCase()) + "#ERWIN#" + sourceTable1.toUpperCase());
                    }
                } else {
                    ArrayList<MappingSpecificationRow> mapSpecs = new ArrayList<>();
                    mapSpecs.add(spec);
                    ReadXMLFileToObject.MergeQuerySpecs.put(sourceTable.toUpperCase(), mapSpecs);
                    ReadXMLFileToObject.participatedTableAgainstMergedQuery.put(sourceTable.toUpperCase(), sourceTable1.toUpperCase());
                }
            }
        }
    }

    public static void extractSnowflakeMetainfo(String metainfo, String tableName) {
        if (metainfo.contains("Snowflake.Database")) {
            String dbType = "Snowflake";
            String ServerName = metainfo.substring(metainfo.lastIndexOf("(") + 1, metainfo.lastIndexOf(")")).split(",")[0].replace("\"", "");
            String databaseName = metainfo.substring(metainfo.lastIndexOf("(") + 1, metainfo.lastIndexOf(")")).split(",")[1].replace("\"", "");
            PowerBIReportParser.xmlDBMap.put(tableName, dbType + "*ERWIN*" + ServerName + "*ERWIN*" + databaseName);
        } else {
            PowerBIReportParser.xmlDBMap.put(tableName, "Snowflake" + "*ERWIN*" + "EMPTY_SERVER" + "*ERWIN*" + "EMPTY_DATABASE");
        }
    }

    public static void extractSnowflakeInfo(String query, String tableName) {
        String snowfalkeQuery = query.substring(query.indexOf("("), query.indexOf("])"));
        String metainfo = snowfalkeQuery.split("\\[Data\\]")[0];
        extractSnowflakeMetainfo(metainfo, tableName);
        snowfalkeQuery = snowfalkeQuery.split("\\[Data\\]")[1];
        snowfalkeQuery = snowfalkeQuery.substring(snowfalkeQuery.indexOf("\"") + 1, snowfalkeQuery.lastIndexOf("\""));
        snowfalkeQuery = snowfalkeQuery.replace("\\u0027", "'").replace("\\u003e", "> ").replace("\"", "").replace("\\u003c", "< ");
        snowfalkeQuery = snowfalkeQuery.replace("#(lf)", "\n");
        snowfalkeQuery = snowfalkeQuery.replace("#(tab)", "\t");
        itemPathAndQueryMap.put(tableName, snowfalkeQuery);
    }

    public static void search_SQLQueries(String query, String tableName) {

        String rePlace = "";
        if (query.contains("Snowflake.Databases")) {
            extractSnowflakeInfo(query, tableName);
        }
        if (query.contains("]")) {

            int firstIndex = query.indexOf("[Query=");
            int lastIndex = query.lastIndexOf("\"])");
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
            query = query.replace("\"\"", "'''");
            query = query.replace("\\u0027", "'").replace("\\u003e", "> ").replace("\"", "").replace("\\u003c", "< ");
            query = query.replace("#(lf)", "\n");
            query = query.replace("#(tab)", "\t");
            query = query.replace("=\\", "");
            query = query.replace("\\\\", "\"").replace("\\", " ").trim();
            if (query.contains(", CreateNavigationProperties=false])")) {
                query = query.substring(0, query.indexOf(", CreateNavigationProperties=false])"));
            }
            if (query.contains("#Merged Queries")) {
                query = query.split("\\]\\),")[0];
            }
            query = query.replace("'''", "\"");
            if (itemPathAndQueryMap.get(tableName) != null) {
                itemPathAndQueryMap.put(tableName, itemPathAndQueryMap.get(tableName) + "#ERWIN#" + query);
            } else {
                itemPathAndQueryMap.put(tableName, query);
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
            if (itemPathAndQueryMap.get(tableName) != null) {
                itemPathAndQueryMap.put(tableName.toUpperCase(), itemPathAndQueryMap.get(tableName) + "#ERWIN#" + query);
            } else {
                itemPathAndQueryMap.put(tableName, query);
            }
        }
    }

    public static boolean search_FlatFiles(String[] splitString, int index, String tableName) {

        String flatFileName = "";
        if (splitString[index].contains("Excel.Workbook")) {
            boolean check = false;
            if (splitString[index].contains("Web.Contents")) {
                String totalPath = "PBI_" + tableName + "(.XLSM)#erwin@" + splitString[index].substring(splitString[index].indexOf("Web.Contents(") + 13, splitString[index].indexOf(")")).replace("\"", "");
                flatFileName = totalPath.split("/")[totalPath.split("/").length - 1].replace("%20", " ");
                ReadXMLFileToObject.itemPathAndxlPath.put(tableName.toUpperCase(), totalPath);
                itemPathAndQueryMap.put(tableName.toUpperCase(), flatFileName);
                return true;
            } else {
                flatFileName = splitString[index].replace("\\u0027", "'").substring(splitString[index].lastIndexOf("\\") + 1).replace("\\", "").replace(")", "").replace("\"", "");
                String filePath = splitString[index].replace(flatFileName, "");
                filePath = filePath.substring(filePath.indexOf("\""), filePath.lastIndexOf("\"")).replace("\"", "");
                String totalPath = "PBI_" + tableName + "(.XLSX)#erwin@" + filePath + "\\" + flatFileName.replace("\\\\", "\\").replace("\\\\", "\\");
                ReadXMLFileToObject.itemPathAndxlPath.put(tableName.toUpperCase(), totalPath);
                itemPathAndQueryMap.put(tableName.toUpperCase(), flatFileName);
                return true;
            }
        }

        if (splitString[index].contains("Csv.Document")) {
            boolean check = false;
            flatFileName = splitString[index].replace("\\u0027", "'").substring(splitString[index].lastIndexOf("\\") + 1).replace("\\", "").replace(")", "").replace("\"", "");
            String filePath = splitString[index].replace(flatFileName, "");
            filePath = filePath.substring(filePath.indexOf("\""), filePath.lastIndexOf("\"")).replace("\"", "");
            String totalPath = "PBI_" + tableName + "(.CSV)#erwin@" + filePath + "\\" + flatFileName.replace("\\\\", "\\").replace("\\\\", "\\");
            ReadXMLFileToObject.itemPathAndxlPath.put(tableName.toUpperCase(), totalPath);
            itemPathAndQueryMap.put(tableName.toUpperCase(), flatFileName);
            return true;
        }

        if (splitString[index].contains("Json.Document")) {
            flatFileName = splitString[index].replace("\\u0027", "'").substring(splitString[index].lastIndexOf("\\") + 1).replace("\\", "").replace(")", "").replace("\"", "");
            String filePath = splitString[index].replace(flatFileName, "");
            filePath = filePath.substring(filePath.indexOf("\""), filePath.lastIndexOf("\"")).replace("\"", "");
            String totalPath = "PBI_" + tableName + "(.JSON)#erwin@" + filePath + "\\" + flatFileName.replace("\\\\", "\\").replace("\\\\", "\\");
            ReadXMLFileToObject.itemPathAndxlPath.put(tableName.toUpperCase(), totalPath);
            itemPathAndQueryMap.put(tableName.toUpperCase(), flatFileName);
            return true;
        }

        if (splitString[index].contains("Xml.Tables")) {
            flatFileName = splitString[index].replace("\\u0027", "'").substring(splitString[index].lastIndexOf("\\") + 1).replace("\\", "").replace(")", "").replace("\"", "");
            String filePath = splitString[index].replace(flatFileName, "");
            filePath = filePath.substring(filePath.indexOf("\""), filePath.lastIndexOf("\"")).replace("\"", "");
            String totalPath = "PBI_" + tableName + "(.XML)#erwin@" + filePath + "\\" + flatFileName.replace("\\\\", "\\").replace("\\\\", "\\");
            ReadXMLFileToObject.itemPathAndxlPath.put(tableName.toUpperCase(), totalPath);
            itemPathAndQueryMap.put(tableName.toUpperCase(), flatFileName);
            return true;
        }
        if (splitString[index].contains("Folder.Files")) {
            flatFileName = splitString[index].replace("\\u0027", "'").substring(splitString[index].lastIndexOf("\\") + 1).replace("\\", "").replace(")", "").replace("\"", "");
            String filePath = splitString[index].replace(flatFileName, "");
            filePath = filePath.substring(filePath.indexOf("\""), filePath.lastIndexOf("\"")).replace("\"", "");
            String totalPath = "PBI_" + tableName + "(FOLDER)#erwin@" + filePath + "\\" + flatFileName.replace("\\\\", "\\").replace("\\\\", "\\");
            ReadXMLFileToObject.itemPathAndxlPath.put(tableName.toUpperCase(), totalPath);
            itemPathAndQueryMap.put(tableName.toUpperCase(), flatFileName);
            return true;

        }
        if (splitString[index].toLowerCase().contains(tableName.toLowerCase())) {
            if (splitString[index].toLowerCase().contains(".xlsx")) {
                if (splitString[index].contains("Name")) {
                    flatFileName = splitString[index].split("Name")[1].replace("=", "").replace("\\", "");
                    itemPathAndQueryMap.put(tableName.toUpperCase(), flatFileName);
                    return true;
                } else if (splitString[index].contains("Excel.Workbook")) {
                    flatFileName = splitString[index].substring(splitString[index].lastIndexOf("\\") + 1).replace("\\", "").replace(")", "").replace("\"", "");
                    itemPathAndQueryMap.put(tableName.toUpperCase(), flatFileName);
                    return true;
                }
            } else if (splitString[index].toLowerCase().contains(".json")) {
                flatFileName = tableName + ".json";
                itemPathAndQueryMap.put(tableName.toUpperCase(), flatFileName);
                return true;
            } else if (splitString[index].toLowerCase().contains(".csv")) {
                flatFileName = tableName + ".csv";
                itemPathAndQueryMap.put(tableName.toUpperCase(), flatFileName);
                return true;
            } else if (splitString[index].toLowerCase().contains(".pdf")) {
                flatFileName = tableName + ".pdf";
                itemPathAndQueryMap.put(tableName.toUpperCase(), flatFileName);
                return true;
            } else if (splitString[index].toLowerCase().contains(".txt")) {
                flatFileName = tableName + ".txt";
                itemPathAndQueryMap.put(tableName.toUpperCase(), flatFileName);
                return true;
            } else if (splitString[index].toLowerCase().contains(".xml")) {
                flatFileName = tableName + ".xml";
                itemPathAndQueryMap.put(tableName.toUpperCase(), flatFileName);
                return true;
            } else if (splitString[index].toLowerCase().contains(".xls")) {
                flatFileName = tableName + ".xls";
                itemPathAndQueryMap.put(tableName.toUpperCase().toUpperCase(), flatFileName);
                return true;
            }
        } else {
            if (splitString[index].contains("Name")) {
                if (splitString[index].toLowerCase().contains(".xlsx")) {
                    flatFileName = splitString[index].split("Name")[1].replace("=", "").replace("\\", "").replace("\"", "").replace("]", "").replace(")", "").trim();
                    String path = "";
                    try {
                        path = splitString[index].split("=")[0].split("_")[1].replace("\"", "").trim() + flatFileName;
                    } catch (Exception ex) {
                        if (splitString[index + 2].contains("Folder Path")) {
                            path = splitString[index + 2].split("=")[1].substring(splitString[index + 2].split("=")[1].indexOf("\"") + 1, splitString[index + 2].split("=")[1].lastIndexOf("\"")) + flatFileName;
                        }
                    }
                    String totalPath = "PBI_" + tableName + "(.XLSX)#erwin@" + path;
                    ReadXMLFileToObject.itemPathAndxlPath.put(tableName.toUpperCase(), totalPath);
                    itemPathAndQueryMap.put(tableName.toUpperCase(), flatFileName);
                    return true;
                } else if (splitString[index].toLowerCase().contains(".json")) {
                    flatFileName = splitString[index].split("Name")[1].replace("=", "").replace("\\", "");
                    itemPathAndQueryMap.put(tableName.toUpperCase(), flatFileName);
                    return true;
                } else if (splitString[index].toLowerCase().contains(".csv")) {
                    flatFileName = splitString[index].split("Name")[1].replace("=", "").replace("\\", "");
                    itemPathAndQueryMap.put(tableName.toUpperCase(), flatFileName);
                    return true;
                } else if (splitString[index].toLowerCase().contains(".pdf")) {
                    flatFileName = splitString[index].split("Name")[1].replace("=", "").replace("\\", "");
                    itemPathAndQueryMap.put(tableName.toUpperCase(), flatFileName);
                    return true;
                } else if (splitString[index].toLowerCase().contains(".txt")) {
                    flatFileName = splitString[index].split("Name")[1].replace("=", "").replace("\\", "");
                    itemPathAndQueryMap.put(tableName.toUpperCase(), flatFileName);
                    return true;
                } else if (splitString[index].toLowerCase().contains(".xml")) {
                    flatFileName = splitString[index].split("Name")[1].replace("=", "").replace("\\", "");
                    itemPathAndQueryMap.put(tableName.toUpperCase().toUpperCase(), flatFileName);
                    return true;
                } else if (splitString[index].toLowerCase().contains(".xls")) {
                    flatFileName = splitString[index].split("Name")[1].replace("=", "").replace("\\", "");
                    itemPathAndQueryMap.put(tableName.toUpperCase(), flatFileName);
                    return true;
                }
            }
        }
        return false;
    }

    public static void search_StoredProcedures(String[] splitString, String schemaName, String tableName) {

        String databaseName = "";
        String procedureName = "";
        schemaName = "";
        for (int index = 0; index < splitString.length; index++) {
            if (splitString[index].toUpperCase().contains("EXECUTE ")) {
                procedureName = splitString[index].split("EXECUTE ")[1].replace("]", "").replace(")", "");
            } else if (splitString[index].toUpperCase().contains("EXEC ")) {
                try {
                    procedureName = splitString[index].replace("#(lf)", "").toUpperCase().substring(splitString[index].indexOf("["), splitString[index].lastIndexOf("]") + 1).split("EXEC")[1].replace("[", "").replace("]", "").replace(";", "").replace("\\", "").trim().replaceAll("[^a-zA-Z0-9_\\s.]", "").replaceAll("\nIN", "");
                } catch (Exception ex) {
                    procedureName = splitString[index].replace("#(lf)", "").toUpperCase().split("EXEC")[1].replace("[", "").replace("\\u0027", "'").replace("]", "").replace(";", "").replace("\\", "").trim().replaceAll("[^a-zA-Z0-9_\\s.]", "");
                }
                if (procedureName.trim().contains(" ")) {
                    procedureName = procedureName.split(" ")[0];
                }
            } else if (splitString[index].contains("EXE ")) {
                procedureName = splitString[index].replace("#(lf)", "").split("EXE")[1].replace("[", "").replace("]", "").replace(";", "").replace("\\", "").trim().replaceAll("[^a-zA-Z0-9_\\s.]", "");
            } else if (splitString[index].contains("Query=")) {
                procedureName = splitString[index].replace("#(lf)", "").split("Query=")[1].replace("[", "").replace("]", "").replace(";", "").replace("\\", "").trim().replaceAll("[^a-zA-Z0-9_\\s.]", "");
            }

        }

        if ("".equals(procedureName)) {
            return;
        } else {
            if (procedureName.split("\\.").length == 3) {
                procedureName = procedureName.split("\\.")[1] + "." + procedureName.split("\\.")[2];
            }
            itemPathAndQueryMap.put(tableName.toUpperCase(), procedureName + "#@ERWIN@#PROC");
        }
    }

    public static void parseODataFeed(String expression, String tableName) {
        for (String eachline : expression.split("\n")) {
            if (eachline.contains("Signature=\"table\"")) {
                String tempTableName = eachline.split(",")[0].split("Name=")[1].replace("\"", "");
                itemPathAndQueryMap.put(tableName.toUpperCase(), tempTableName + "#@ERWIN@#ODATA_TAB");
                PowerBIReportParser.xmlDBMap.put(tableName, "OData.Feed" + "*ERWIN*" + "EMPTY_SERVER" + "*ERWIN*" + "EMPTY_DATABASE");
            }
        }
    }

    public static void parseSnowflake(String expression, String tableName) {
        String database = "";
        String schema = "";
        String table = "";
        String serverName = "";
        String dbType = "Snowflake";
        for (String eachline : expression.split("\n")) {
            if (eachline.contains("Snowflake.Databases")) {
                serverName = eachline.split(",")[0].split("\"")[1];
            }

            if ((expression.toLowerCase().contains("select\n") || expression.toLowerCase().contains("select#(lf)") || expression.toLowerCase().contains("select ")) && (expression.toLowerCase().contains(" from ") || expression.toLowerCase().contains(" from\n") || expression.toLowerCase().contains(" from#(lf)") || expression.toLowerCase().contains("\nfrom ") || expression.toLowerCase().contains("#(lf)from ") || expression.toLowerCase().contains("\nfrom\n") || expression.toLowerCase().contains("#(lf)from#(lf)"))) {
                extractSnowflakeSQL(expression, tableName);
                break;
            }

            if (eachline.contains("Kind=\"Database\"") || eachline.contains("Kind = \"Database\"")) {
                try {
                    if (eachline.contains("Kind = \"Database\"")) {
                        database = eachline.split(",")[0].split("Name = ")[1].replace("\"", "");
                    } else {
                        database = eachline.split(",")[0].split("Name=#")[1].replace("\"", "");
                    }
                } catch (Exception ex) {
                    database = eachline.split(",")[0].split("Name=")[1].replace("\"", "");
                }
            }
            if (eachline.contains("Kind=\"Schema\"") || eachline.contains("Kind = \"Schema\"")) {
                if (eachline.contains("Kind = \"Schema\"")) {
                    schema = eachline.split(",")[0].split("Name = ")[1].replace("\"", "");
                } else {
                    schema = eachline.split(",")[0].split("Name=")[1].replace("\"", "");
                }
            }
            if (eachline.contains("Kind=\"Table\"") || eachline.contains("Kind = \"Table\"")) {
                if (eachline.contains("Kind = \"Table\"")) {
                    table = eachline.split(",")[0].split("Name = ")[1].replace("\"", "");
                } else {
                    table = eachline.split(",")[0].split("Name=")[1].replace("\"", "");
                }
                if (!schema.isEmpty()) {
                    table = schema + "." + table;
                }
                itemPathAndQueryMap.put(tableName.toUpperCase(), table + "#@ERWIN@#SNOWFLAKE_TAB");
            }
            if (eachline.contains("Kind=\"View\"") || eachline.contains("Kind = \"View\"")) {
                if (eachline.contains("Kind = \"View\"")) {
                    table = eachline.split(",")[0].split("Name = ")[1].replace("\"", "");
                } else {
                    table = eachline.split(",")[0].split("Name=")[1].replace("\"", "");
                }
                if (!schema.isEmpty()) {
                    table = schema + "." + table;
                }
                itemPathAndQueryMap.put(tableName.toUpperCase(), table + "#@ERWIN@#VIEW");
            }
        }
        if (!table.isEmpty()) {
//            if (!schema.isEmpty()) {
//                table = schema + "." + table;
//            }
//            itemPathAndQueryMap.put(tableName.toUpperCase(), table + "#@ERWIN@#TEMP_TAB");
            if (database.isEmpty()) {
                database = "EMPTY_DATABASE";
            }
            if (serverName.isEmpty()) {
                serverName = "EMPTY_SERVER";
            }
            PowerBIReportParser.xmlDBMap.put(tableName, dbType + "*ERWIN*" + serverName + "*ERWIN*" + database);
        }
    }

    public static void searchForDataflow(String expression, String logicalTableName) {
        String entityName = "";
        if (expression.contains("PowerBI.Dataflows")) {
            for (String eachLine : expression.split("\n")) {
                if (eachLine.contains("EntityName =")) {
                    entityName = eachLine.substring(eachLine.indexOf("EntityName = \"") + 14, eachLine.indexOf("\","));
                    break;
                } else if (eachLine.contains("entity=\"")) {
                    entityName = eachLine.substring(eachLine.indexOf("entity=\"") + 8, eachLine.indexOf("\"]}"));
                    break;
                }
            }
            itemPathAndQueryMap.put(logicalTableName, entityName + "#@ERWIN@#DATAFLOW");
            columnsAgainstLogicalTable.put(entityName, columnsAgainstLogicalTable.get(logicalTableName));
        }
    }

    //Calling Method
    public static void readDataModelFile(File dataModelFile, String sysEnvDetail[], String reportName, PowerBI_Bean bean) {
        try {
            JsonFactory f = new MappingJsonFactory();
            JsonParser jp = f.createJsonParser(dataModelFile);
            JsonNode parsernode = jp.readValueAsTree();
            if (parsernode.has("model")) {
                JsonNode modelNode = parsernode.get("model");
                getExpressionNodeInfo(modelNode);
                if (modelNode.has("tables")) {
                    ArrayNode tableArray = (ArrayNode) modelNode.get("tables");
                    for (int i = 0; i < tableArray.size(); i++) {
                        JsonNode jsonTableNode = tableArray.get(i);
                        getServerDatabaseForDatamodelSchemaFile(tableArray, reportName);
                        String tableName = reportName + "." + (jsonTableNode.get("name").getTextValue().toUpperCase().replaceAll("[^a-zA-Z0-9_\\s\\-]", ""));
                        if (tableName.contains("LOCALDATETABLE") || tableName.contains("DATETABLETEMPLATE")) {
                            continue;
                        }
                        prepareMeasureInfos(jsonTableNode, tableName);
                        getColumnDetails(jsonTableNode, tableName);
                        if (jsonTableNode.has("partitions")) {
                            JSONArray partitionObject = new JSONArray(jsonTableNode.get("partitions").toString());
                            for (int partitionIndex = 0; partitionIndex < partitionObject.length(); partitionIndex++) {
                                JSONObject eachPartitionObject = partitionObject.getJSONObject(partitionIndex);
                                if (eachPartitionObject.has("queryGroup") && eachPartitionObject.getString("queryGroup").equals("Dataflow")) {
                                    if (eachPartitionObject.has("source")) {
                                        JSONObject sourceObject = eachPartitionObject.getJSONObject("source");
                                        String expression = sourceObject.getString("expression");
//                                        pbidu.getDataflowInfo(bean, expression, new JSONArray(jsonTableNode.get("columns").toString()));
                                        searchForDataflow(expression, tableName);
                                    }
                                } else if (eachPartitionObject.has("source") && eachPartitionObject.getJSONObject("source").has("expression") && eachPartitionObject.getJSONObject("source").getString("expression").contains("PowerBI.Dataflows")) {
                                    JSONObject sourceObject = eachPartitionObject.getJSONObject("source");
                                    String expression = sourceObject.getString("expression");
//                                        pbidu.getDataflowInfo(bean, expression, new JSONArray(jsonTableNode.get("columns").toString()));
                                    searchForDataflow(expression, tableName);
                                } else if (eachPartitionObject.has("source") && eachPartitionObject.getJSONObject("source").getString("expression").contains("PowerBI.Dataflows")) {
                                    if (eachPartitionObject.has("source")) {
                                        JSONObject sourceObject = eachPartitionObject.getJSONObject("source");
                                        String expression = sourceObject.getString("expression");
//                                        pbidu.getDataflowInfo(bean, expression, new JSONArray(jsonTableNode.get("columns").toString()));
                                        searchForDataflow(expression, tableName);
                                    }
                                }
                                if (eachPartitionObject.has("source")) {
                                    JSONObject sourceObject = eachPartitionObject.getJSONObject("source");
                                    if (sourceObject.has("expression")) {
                                        String expression = sourceObject.getString("expression");
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
                                                    if (!expression.contains("GoogleBigQuery.Database") && !expression.contains("Odbc.DataSource") && !expression.contains("Snowflake.Databases")) {
                                                        searchPowerBI_Views(expression, tableName);
                                                    }
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
                                            if (expression.contains("GoogleBigQuery.Database")) {
                                                parsePowerBI_BIQuery_Views(expression, tableName);
                                                continue;
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
                                                    parsePowerBI_DAX(query, expression, tableName, schemaName, reportName, sysEnvDetail);
                                                    if ((query.toLowerCase().contains("select ") || query.toLowerCase().contains("with ") || query.toLowerCase().contains("select#(tab)") || query.toLowerCase().contains("select#(lf)"))) {
                                                        search_SQLQueries(query, tableName);
                                                    } else {
                                                        String[] splitString = query.split(",");
                                                        if (query.toLowerCase().contains(".xlsx") || query.toLowerCase().contains(".xlsm") || query.toLowerCase().contains(".xml") || query.toLowerCase().contains("folder.files") || query.toLowerCase().contains(".json") || query.toLowerCase().contains(".pdf") || query.toLowerCase().contains(".csv") || query.toLowerCase().contains(".txt") || query.toLowerCase().contains(".xls")) {
                                                            for (int index = 0; index < splitString.length; index++) {
                                                                if (splitString[index].toLowerCase().contains(".xlsx") || query.toLowerCase().contains(".xlsm") || splitString[index].toLowerCase().contains(".xml") || splitString[index].toLowerCase().contains("folder.files") || splitString[index].toLowerCase().contains(".json") || splitString[index].toLowerCase().contains(".pdf") || splitString[index].toLowerCase().contains(".csv") || splitString[index].toLowerCase().contains(".txt") || splitString[index].toLowerCase().contains(".xls")) {
                                                                    boolean isCompleted = search_FlatFiles(splitString, index, tableName);
                                                                    if (isCompleted) {
                                                                        break;
                                                                    }
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
                                            } else if ((expression.contains("Schema") && expression.contains("Name")) && !expression.contains("GoogleBigQuery.Database") && !expression.contains("Odbc.DataSource") && !expression.contains("Snowflake.Databases")) {
                                                searchMannualTable1(expression, tableName);
                                            }
                                        } else if (expression.contains("ADDCOLUMNS")) {
                                            PowerBI_DAX_Parser bI_DAX_Parser = new PowerBI_DAX_Parser();
//                                            bI_DAX_Parser.dax_ParseDAXQuery(expression, tableName, bean);
                                            itemPathAndQueryMap.put(tableName.toUpperCase(), expression);

                                        } else {
                                            parseMeasuresFromSource(tableName, expression);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

            }
            jp.close();
        } catch (Exception ex) {
            StringWriter exception = new StringWriter();
            ex.printStackTrace(new PrintWriter(exception));
            PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Error ::Exception In PBITDataModelFileReader.readDataModelFile() Method " + ex.toString() + "\n");
        }
    }

    public static void parsePowerBI_BIQuery_Views(String expression, String logicalTableName) {
        String schema = "";
        String tableName = "";
        String serverName = "";
        String databaseName = "";
        if (expression.contains("GoogleBigQuery")) {
            String dbType = "GoogleBigQuery";
            for (String eachline : expression.split("\n")) {
                if (eachline.contains("Name=\"") && !eachline.contains("Kind=\"")) {
                    serverName = eachline.split("Name=\"")[1].split("]")[0].replace("\"", "");
                    databaseName = serverName;
                }
                if (eachline.contains("Kind=\"Schema\"")) {
                    schema = eachline.split("Kind=\"Schema\"")[0].split("Name=")[1].replace("\"", "").replace(",", "");
                }
                if (eachline.contains("Kind=\"View\"")) {
                    tableName = eachline.split("Kind=\"View\"")[0].split("Name=")[1].replace("\"", "").replace(",", "");
                }
            }
//            tableName = schema + "." + tableName;
            itemPathAndQueryMap.put(logicalTableName.toUpperCase(), schema + "." + tableName + "#@ERWIN@#TEMP_TAB");
            if (databaseName.equals("") || serverName.equals("")) {
                PowerBIReportParser.xmlDBMap.put(tableName, dbType + "*ERWIN*" + "EMPTY_SERVER" + "*ERWIN*" + "EMPTY_DATABASE");
            } else {
                PowerBIReportParser.xmlDBMap.put(tableName, dbType + "*ERWIN*" + serverName + "*ERWIN*" + databaseName);
            }
        }

    }

    public static void searchPowerBI_Views(String expression, String tableName) {
        if (expression.contains("Schema") && expression.contains("Item")) {
            String schema = "";
            String tabName = "";
            for (String eachLine : expression.split("\n")) {
                if (eachLine.contains("Schema") && eachLine.contains("Item")) {
                    for (String eachSentence : eachLine.split(",")) {
                        if (eachSentence.contains("Schema")) {
                            schema = eachSentence.substring(eachSentence.indexOf("Schema")).replace("\"", "").split("=")[1];
                        }
                        if (eachSentence.contains("Item")) {
                            tabName = eachSentence.substring(eachSentence.indexOf("Item")).replace("\"", "").split("=")[1].replace("[Data]", "").replace("}", "").replace("]", "");
                        }
                    }
                }
                if (eachLine.contains("Sql.Database")) {
                    prepareSQLMetadataInfos(eachLine, tableName, schema);
                } else if (eachLine.contains("PostgreSQL.Database")) {
                    prepareSQLMetadataInfos(eachLine, tableName, schema);
                }
            }
            itemPathAndQueryMap.put(tableName.toUpperCase(), schema + "." + tabName + "#@ERWIN@#TEMP_TAB");
        }
    }

    public static void searchMannualTable(String expression, String tableName) {
        String schema = "";
        String tabName = "";
        for (String eachLine : expression.split("\n")) {
            if (eachLine.contains("Schema") && eachLine.contains("Item")) {
                for (String eachSentence : eachLine.split(",")) {
                    if (eachSentence.contains("Schema")) {
                        schema = eachSentence.substring(eachSentence.indexOf("Schema")).replace("\"", "").split("=")[1];
                    }
                    if (eachSentence.contains("Item")) {
                        tabName = eachSentence.substring(eachSentence.indexOf("Item")).replace("\"", "").split("=")[1].replace("[Data]", "").replace("}", "").replace("]", "");
                    }
                }
            }
            if (eachLine.contains("Sql.Database")) {
                prepareSQLMetadataInfos(eachLine, tableName, schema);
            } else if (eachLine.contains("PostgreSQL.Database")) {
                prepareSQLMetadataInfos(eachLine, tableName, schema);
            }
        }
        itemPathAndQueryMap.put(tableName.toUpperCase(), schema + "." + tabName + "#@ERWIN@#TEMP_TAB");
    }

    public static void searchMannualTable1(String expression, String tableName) {
        String schema = "";
        String tabName = "";
        for (String eachLine : expression.split("\n")) {
            if (eachLine.contains("Schema")) {
                schema = eachLine.substring(eachLine.indexOf("Schema"), eachLine.indexOf("]}")).replace("\"", "").split("=")[1];
            }
            if (eachLine.contains("Name")) {
                tabName = eachLine.substring(eachLine.indexOf("Name"), eachLine.indexOf("]}")).replace("\"", "").split("=")[1];
            }
//            if (eachLine.contains("Schema") && eachLine.contains("Name")) {
//                for (String eachSentence : eachLine.split(",")) {
//                    if (eachSentence.contains("Schema")) {
//                        schema = eachSentence.substring(eachSentence.indexOf("Schema")).replace("\"", "").split("=")[1];
//                    }
//                    if (eachSentence.contains("Name")) {
//                        tabName = eachSentence.substring(eachSentence.indexOf("Name")).replace("\"", "").split("=")[1].replace("[Data]", "").replace("}", "").replace("]", "");
//                    }
//                }
//            }
            if (eachLine.contains("Sql.Database")) {
                prepareSQLMetadataInfos(eachLine, tableName, schema);
            }
        }
        itemPathAndQueryMap.put(tableName.toUpperCase(), schema + "." + tabName + "#@ERWIN@#TEMP_TAB");
    }

    public static void putQueriesInDataMashup(File dataModelFile, String reportName) {
        try {
            JsonFactory f = new MappingJsonFactory();
            JsonParser jp = f.createJsonParser(dataModelFile);
            JsonNode parsernode = jp.readValueAsTree();
            if (parsernode.has("model")) {
                JsonNode modelNode = parsernode.get("model");
                if (modelNode.has("tables")) {
                    ArrayNode tableArray = (ArrayNode) modelNode.get("tables");
                    for (int i = 0; i < tableArray.size(); i++) {
                        JsonNode jsonTableNode = tableArray.get(i);
                        getServerDatabaseForDatamodelSchemaFile(tableArray, reportName);
                        String tableName = reportName + "." + jsonTableNode.get("name").getTextValue().toUpperCase();
                        if (jsonTableNode.has("partitions")) {
                            JSONArray partitionObject = new JSONArray(jsonTableNode.get("partitions").toString());
                            for (int partitionIndex = 0; partitionIndex < partitionObject.length(); partitionIndex++) {
                                JSONObject eachPartitionObject = partitionObject.getJSONObject(partitionIndex);
                                if (eachPartitionObject.has("source")) {
                                    JSONObject sourceObject = eachPartitionObject.getJSONObject("source");
                                    if (sourceObject.has("expression")) {
                                        String expression = sourceObject.getString("expression");
                                        DataMashupExtractor.queriesAgainstLogicalTable.put(tableName, expression);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            jp.close();
        } catch (Exception ex) {
            StringWriter exception = new StringWriter();
            ex.printStackTrace(new PrintWriter(exception));
            PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Error ::Exception Occured in PBITDataModelFileReader.putQueriesInDataMashup() Method " + exception.toString() + "\n");
        }
    }

    public static void getServerDatabaseForDatamodelSchemaFile(ArrayNode tableArray, String reportName) {
        try {
            for (int i = 0; i < tableArray.size(); i++) {
                JsonNode jsonTableNode = tableArray.get(i);
                String tableName = reportName + "." + jsonTableNode.get("name").getTextValue().toUpperCase();
                if (jsonTableNode.has("partitions")) {
                    JSONArray partitionObject = new JSONArray(jsonTableNode.get("partitions").toString());
                    for (int partitionIndex = 0; partitionIndex < partitionObject.length(); partitionIndex++) {
                        JSONObject eachPartitionObject = partitionObject.getJSONObject(partitionIndex);
                        if (eachPartitionObject.has("source")) {
                            JSONObject sourceObject = eachPartitionObject.getJSONObject("source");
                            if (sourceObject.has("expression")) {
                                String query = sourceObject.getString("expression");
                                query = query.replace("#(lf)", "\n");
                                query = query.replace("#(tab)", "\t");
                                query = query + "\n";
                            }
                        }
                    }
                }

            }
        } catch (Exception ex) {
            StringWriter exception = new StringWriter();
            ex.printStackTrace(new PrintWriter(exception));
            PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Error ::Exception Occured in PBITDataModelFileReader.getServerDatabaseForDatamodelSchemaFile() Method " + exception.toString() + "\n");
        }
    }

    public static void getRelationshipSpecs(File dataModelFile) {
        PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Info :: Getting model view relationships\n");
        PowerBI_LOGGER.overviewLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Info :: Getting model view relationships\n");
        try {
            JsonFactory f = new MappingJsonFactory();
            JsonParser jp = f.createJsonParser(dataModelFile);
            JsonNode parsernode = jp.readValueAsTree();
            if (parsernode.has("model")) {
                JsonNode modelNode = parsernode.get("model");
                if (modelNode.has("tables")) {
                    ArrayNode tableArray = (ArrayNode) modelNode.get("tables");
                    for (int i = 0; i < tableArray.size(); i++) {
                        JsonNode jsonTableNode = tableArray.get(i);
                        String tableName = jsonTableNode.get("name").getTextValue();
                        if (jsonTableNode.has("partitions")) {
                            ArrayNode partitionsArray = (ArrayNode) jsonTableNode.get("partitions");
                            JSONObject partitionNode = new JSONObject(partitionsArray.get(0).toString());
                            if (partitionNode.has("source")) {
                                JSONObject sourceNode = partitionNode.getJSONObject("source");
                                if (sourceNode.has("expression")) {
                                    sourceNode.getString("type");
                                    if (sourceNode.getString("type").equals("calculated")) {
                                        String expression = sourceNode.getString("expression");
                                        calculatedTablesMeasures.put(tableName, expression);
                                    }
                                }
                            }
                        }
                        if (jsonTableNode.has("columns")) {
                            JSONArray columnArray = new JSONArray(jsonTableNode.get("columns").toString());
                            for (int j = 0; j < columnArray.length(); j++) {
                                JSONObject columnJSON = columnArray.getJSONObject(j);
                                if (columnJSON.has("columnOriginTable")) {
                                    prepareRelationship(columnJSON, tableName);
                                }
                            }
                        }
                    }
                }
            }
        } catch (JSONException ex) {
            StringWriter exception = new StringWriter();
            ex.printStackTrace(new PrintWriter(exception));
            PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Error ::Exception Occured in PBITDataModelFileReader.getRelationshipSpecs() Method " + exception.toString() + "\n");
        } catch (IOException ex) {
            StringWriter exception = new StringWriter();
            ex.printStackTrace(new PrintWriter(exception));
            PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Error ::Exception Occured in PBITDataModelFileReader.getRelationshipSpecs() Method " + exception.toString() + "\n");
        }
    }

    public static void prepareRelationship(JSONObject columnJSON, String tableName) {
        try {
            String sourceTableName = columnJSON.getString("columnOriginTable");
            if (columnJSON.has("sourceColumn")) {
                String sourceCol = columnJSON.getString("sourceColumn");
                sourceCol = sourceCol.substring(sourceCol.indexOf("[") + 1, sourceCol.indexOf("]"));
                if (!tableName.contains("LocalDateTable") && !sourceTableName.contains("LocalDateTable")) {
                    if (PBITDataModelFileReader.relationshipMap.get(sourceTableName + "." + sourceCol) != null) {
                        try {
                            String targetInfo = PBITDataModelFileReader.relationshipMap.get(sourceTableName + "." + sourceCol);
                            for (String actualTargetInfo : targetInfo.split("#ERWIN#")) {
                                String actualTargetInfo1 = actualTargetInfo.split("@ERWIN@")[0];
                                String temp_src_table = actualTargetInfo1.split("\\.")[0];
                                String temp_src_col = actualTargetInfo1.split("\\.")[1];
                                String sourceRules = "";
                                try {
                                    sourceRules = actualTargetInfo.split("@ERWIN@")[1];
                                } catch (Exception ex) {
                                }
                            }
                        } catch (Exception ex) {
                        }
                    }
                    if (!tableName.contains("LocalDateTable") && !sourceTableName.contains("LocalDateTable")) {
                        if (PBITDataModelFileReader.relationshipMap.get(sourceTableName + "." + sourceCol) != null) {
                            try {
                                String targetInfo = PBITDataModelFileReader.relationshipMap.get(sourceTableName + "." + sourceCol);
                                for (String actualTargetInfo : targetInfo.split("#ERWIN#")) {
                                    String actualTargetInfo1 = actualTargetInfo.split("@ERWIN@")[0];
                                    String temp_src_table = actualTargetInfo1.split("\\.")[0];
                                    String temp_src_col = actualTargetInfo1.split("\\.")[1];
                                    String sourceRules = "";
                                    try {
                                        sourceRules = actualTargetInfo.split("@ERWIN@")[1];
                                    } catch (Exception ex) {
                                    }
                                }
                            } catch (Exception ex) {
                            }
                        }
                        if (PBITDataModelFileReader.relationshipMap.get(tableName + "." + columnJSON.getString("name")) != null) {
                            try {
                                String targetInfo = PBITDataModelFileReader.relationshipMap.get(tableName + "." + columnJSON.getString("name"));
                                for (String actualTargetInfo : targetInfo.split("#ERWIN#")) {
                                    String actualTargetInfo1 = actualTargetInfo.split("@ERWIN@")[0];
                                    String temp_src_table = actualTargetInfo1.split("\\.")[0];
                                    String temp_src_col = actualTargetInfo1.split("\\.")[1];
                                    String sourceRules = "";
                                    try {
                                        sourceRules = actualTargetInfo.split("@ERWIN@")[1];
                                    } catch (Exception ex) {
                                    }
                                }
                            } catch (Exception ex) {

                            }
                        }
                    }
                }
            }
        } catch (JSONException ex) {

        }
    }

    public static void getRelationships(String relationshipJSON) {
        try {
            JSONArray relationshipArray = new JSONArray(relationshipJSON);
            for (int index = 0; index < relationshipArray.length(); index++) {
                JSONObject eachReationshipObject = relationshipArray.getJSONObject(index);
                String sourceTableName = "";
                String sourceColumnName = "";
                String targetTableName = "";
                String tagetColumnName = "";

                if (eachReationshipObject.has("fromTable")) {
                    sourceTableName = eachReationshipObject.getString("fromTable");
                } else {
                    continue;
                }

                if (eachReationshipObject.has("toTable")) {
                    targetTableName = eachReationshipObject.getString("toTable");

                } else {
                    continue;

                }

                if (eachReationshipObject.has("fromColumn")) {
                    sourceColumnName = eachReationshipObject.getString("fromColumn");

                } else {
                    continue;

                }

                if (eachReationshipObject.has("toColumn")) {
                    tagetColumnName = eachReationshipObject.getString("toColumn");

                } else {
                    continue;

                }

                if (relationshipMap.get(targetTableName + "." + tagetColumnName) != null) {
                    relationshipMap.put(targetTableName + "." + tagetColumnName, relationshipMap.get(targetTableName + "." + tagetColumnName) + "#ERWIN#" + targetTableName + "." + tagetColumnName);
                } else {
                    relationshipMap.put(sourceTableName + "." + sourceColumnName, targetTableName + "." + tagetColumnName);
                }
            }
        } catch (Exception ex) {
            ex.getMessage();
        }
    }

    public static void parseMeasuresFromSource(String tableName, String measure) {

        Pattern pattern = Pattern.compile("[\\'a-zA-Z0-9\\s\\_\\-']*\\[.*?\\]");
        Matcher matcher = pattern.matcher(measure);
        while (matcher.find()) {
            String matchString = matcher.group();
            matchString = matchString.replace("'", "");
//            String tableName = matchString.split("\\[")[0].trim();
            String columnName = matchString.substring(matchString.indexOf("[") + 1, matchString.indexOf("]"));
            expressionAgainstMeasureMap.put(tableName + "@ERWIN@" + columnName.toUpperCase(), measure);
        }

    }

    public static void getColumnDetails(JsonNode jsonTableNode, String tableName) {
        try {
            JSONArray columnArray = new JSONArray(jsonTableNode.get("columns").toString());
            for (int j = 0; j < columnArray.length(); j++) {
                JSONObject columnJSON = columnArray.getJSONObject(j);
                String columnName = columnJSON.getString("name");
                if (columnsAgainstLogicalTable.containsKey(tableName)) {
                    Set<String> columns = columnsAgainstLogicalTable.get(tableName);
                    columns.add(columnName);
                    columnsAgainstLogicalTable.put(tableName, columns);
                } else {
                    Set<String> columns = new HashSet<String>();
                    columns.add(columnName);
                    columnsAgainstLogicalTable.put(tableName, columns);
                }

            }
        } catch (Exception ex) {

        }
    }

    public static void extractSnowflakeSQL(String expression, String tableName) {
        String serverName;
        String database;
        String dbType = "Snowflake";
        String query = expression;
        query = query.substring(query.indexOf("(") + 1, query.lastIndexOf(")"));
        query = query.split("\\[Data\\],")[1];
        query = query.split("\\[EnableFolding=true\\]")[0];
        query = query.substring(query.indexOf("\"") + 1, query.lastIndexOf("\""));
        query = query.replace("#(lf)", "\n").replace("#(tab)", "\t");
        query = query.replace("\"\"", "\"");
        snowflakeQuery = (snowflakeQuery.equals("")) ? snowflakeQuery + query + "#erwin#" + tableName.toUpperCase() : snowflakeQuery + "@erwin@" + query + "#erwin#" + tableName.toUpperCase();

        itemPathAndQueryMap.put("DataModelSchema_Snowflake_Query", "Table");
        String serverDBName = expression.substring(expression.indexOf("(") + 1, expression.lastIndexOf(")"));
        serverDBName = serverDBName.substring(serverDBName.indexOf("(") + 1, serverDBName.indexOf(")"));
        serverName = serverDBName.split(",")[0].replace("\"", "");
        database = serverDBName.split(",")[1].replace("\"", "");
        if (database.isEmpty()) {
            database = "EMPTY_DATABASE";
        }
        if (serverName.isEmpty()) {
            serverName = "EMPTY_SERVER";
        }
        PowerBIReportParser.xmlDBMap.put(tableName, dbType + "*ERWIN*" + serverName + "*ERWIN*" + database);

    }

    public static void getExpressionNodeInfo(JsonNode modelNode) {
        if (modelNode.has("expressions")) {
            ArrayNode expressionsArray = (ArrayNode) modelNode.get("expressions");
            for (int i = 0; i < expressionsArray.size(); i++) {
                JsonNode expressionNode = expressionsArray.get(i);
                String expressionName = expressionNode.get("name").getTextValue();
                String expressionString = expressionNode.get("expression").getTextValue();
                if (expressionString.contains("IsParameterQuery=true") && expressionString.contains("BinaryIdentifier=#")) {
                    String parameter = expressionString.split("BinaryIdentifier=#")[1].substring(0, expressionString.split("BinaryIdentifier=#")[1].indexOf(",")).replace("\"", "");
                    queryAgainstParameter.put(expressionName, parameter + "#ERWIN#PARAM");
                } else {
                    queryAgainstParameter.put(expressionName, expressionString);
                }
            }
            summeriseExpressionNodeMap();
        }
    }

    public static void parserParameterisedCSVDocument(String expression, String tableName) {
        if (!expression.contains(".csv")) {
            String parameter = expression.split("Csv\\.Document\\(")[1].substring(0, expression.split("Csv\\.Document\\(")[1].indexOf(","));
            String query = queryAgainstParameter.get(parameter);
            String path = query.split("Path\"=")[1].substring(0, query.split("Path\"=")[1].indexOf(",")).replace("\"", "");
            String fileName = query.split("Name=")[1].substring(0, query.split("Name=")[1].indexOf("]")).replace("\"", "");
            String totalPath = "PBI_" + tableName + "(.CSV)#erwin@" + path + "\\" + fileName.replace("\\\\", "\\").replace("\\\\", "\\");
            ReadXMLFileToObject.itemPathAndxlPath.put(tableName.toUpperCase(), totalPath);
            itemPathAndQueryMap.put(tableName.toUpperCase(), fileName);
        }

    }

    public static void summeriseExpressionNodeMap() {
        HashMap<String, String> newQueryAgainstParameter = new HashMap<>();
        for (Map.Entry<String, String> entry : queryAgainstParameter.entrySet()) {
            String parameter = entry.getKey();
            String query = entry.getValue();
            if (query.contains("#ERWIN#PARAM")) {
                if (queryAgainstParameter.containsKey(query.split("#ERWIN#")[0])) {
                    newQueryAgainstParameter.put(parameter, queryAgainstParameter.get(query.split("#ERWIN#")[0]));
                } else {
                    newQueryAgainstParameter.put(parameter, query);
                }
            } else {
                newQueryAgainstParameter.put(parameter, query);
            }
        }
        queryAgainstParameter = newQueryAgainstParameter;
    }

    public static void parseODBC(String expression, String tableName) {
        String database = "";
        String schema = "";
        String table = "";
        String serverName = "";
        String dbType = "ODBC";
        for (String eachline : expression.split("\n")) {
            if (eachline.contains("Odbc.DataSource")) {
                serverName = eachline.split(",")[0].split("\"")[1];
                try {
                    serverName = serverName.split("=")[1];
                } catch (Exception ex) {
                    serverName = eachline.split(",")[0].split("\"")[2].replace("&", "");
                    if (queryAgainstParameter.containsKey(serverName)) {
                        serverName = queryAgainstParameter.get(serverName);
                        serverName = serverName.split("DefaultValue=")[1].split("\\,")[0].replace("\"", "");
                    }
                }
            }

            if (eachline.contains("Kind=\"Database\"") || eachline.contains("Kind = \"Database\"")) {
                try {
                    if (eachline.contains("Kind = \"Database\"")) {
                        database = eachline.split(",")[0].split("Name = ")[1].replace("\"", "");
                    } else {
                        database = eachline.split(",")[0].split("Name=#")[1].replace("\"", "");
                    }
                } catch (Exception ex) {
                    database = eachline.split(",")[0].split("Name=")[1].replace("\"", "");
                    if (queryAgainstParameter.containsKey(database)) {
                        database = queryAgainstParameter.get(database);
                        database = database.split("DefaultValue=")[1].split("\\,")[0].replace("\"", "");
                    }
                }
            }
            if (eachline.contains("Kind=\"Schema\"") || eachline.contains("Kind = \"Schema\"")) {
                if (eachline.contains("Kind = \"Schema\"")) {
                    schema = eachline.split(",")[0].split("Name = ")[1].replace("\"", "");
                } else {
                    schema = eachline.split(",")[0].split("Name=")[1].replace("\"", "");
                }
            }
            if (eachline.contains("Kind=\"Table\"") || eachline.contains("Kind = \"Table\"")) {
                if (eachline.contains("Kind = \"Table\"")) {
                    table = eachline.split(",")[0].split("Name = ")[1].replace("\"", "");
                } else {
                    table = eachline.split(",")[0].split("Name=")[1].replace("\"", "");
                }
                if (!schema.isEmpty()) {
                    table = schema + "." + table;
                }
                itemPathAndQueryMap.put(tableName.toUpperCase(), table + "#@ERWIN@#TEMP_TAB");
            }
            if (eachline.contains("Kind=\"View\"") || eachline.contains("Kind = \"View\"")) {
                if (eachline.contains("Kind = \"View\"")) {
                    table = eachline.split(",")[0].split("Name = ")[1].replace("\"", "");
                } else {
                    table = eachline.split(",")[0].split("Name=")[1].replace("\"", "");
                }
                if (!schema.isEmpty()) {
                    table = schema + "." + table;
                }
                itemPathAndQueryMap.put(tableName.toUpperCase(), table + "#@ERWIN@#VIEW");
            }
        }
        if (!table.isEmpty()) {
//            if (!schema.isEmpty()) {
//                table = schema + "." + table;
//            }
//            itemPathAndQueryMap.put(tableName.toUpperCase(), table + "#@ERWIN@#TEMP_TAB");
            if (database.isEmpty()) {
                database = "EMPTY_DATABASE";
            }
            if (serverName.isEmpty()) {
                serverName = "EMPTY_SERVER";
            }
            PowerBIReportParser.xmlDBMap.put(tableName, dbType + "*ERWIN*" + serverName + "*ERWIN*" + database);
        }
    }
}
