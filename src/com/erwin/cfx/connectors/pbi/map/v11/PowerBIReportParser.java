/* 05:52PM
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
 /*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.connectors.pbi.map.v11;

import com.erwin.cfx.connectors.pbi.map.util.PowerBIConfigPropertiesBuilder;
import com.erwin.cfx.connectors.pbi.map.util.SyncMetadataJsonFileDesign;
import com.erwin.cfx.connectors.pbi.map.util.ReadXMLFileToObject;
import com.erwin.cfx.connectors.pbi.map.util.PowerBIQueryPropertiesBuilder;
import com.erwin.cfx.connectors.pbi.map.util.PowerBIDataTransformsBuilder;
import com.erwin.cfx.connectors.pbi.map.util.PowerBIFiltersPropertiesBuilder;
import com.erwin.cfx.connectors.pbi.map.pojo.PowerBI_Bean;
import com.ads.api.beans.common.APIConstants;
import com.ads.api.beans.common.Document;
import com.ads.api.beans.mm.Mapping;
import com.ads.api.beans.mm.MappingSpecificationRow;
import com.ads.api.beans.mm.Subject;
import com.ads.api.util.KeyValueUtil;
import com.ads.api.util.MappingManagerUtil;
import com.ads.api.util.SystemManagerUtil;
import com.erwin.cfx.connectors.json.syncup.SyncupWithServerDBSchamaSysEnvCPT;
import com.erwin.cfx.connectors.pbi.map.sql.util.PowerBI_DataflowUtil;
import com.erwin.cfx.connectors.pbi.map.sql.util.PowerBI_SQLUtil;
import static com.erwin.cfx.connectors.pbi.map.v11.PBITDataModelFileReader.itemPathAndQueryMap;
import static com.erwin.cfx.connectors.pbi.map.util.PowerBIConfigPropertiesBuilder.actualTableAgainstEntity;
import com.erwin.cfx.connectors.pbi.map.util.PowerBI_ConnectionFile;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.apache.commons.io.FileUtils;
import com.erwin.cfx.connectors.sqlparser.v3.MappingCreator;
import com.erwin.cfx.connectors.pbi.map.util.PowerBI_LOGGER;
import com.erwin.cfx.connectors.pbi.map.util.PowerBI_MappingArrangement;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * This class is the main class for PowerBI smart connector
 *
 * @author Sukhendu/Sudhansu
 *
 */
public class PowerBIReportParser {

    public static StringBuffer exceptionLog = null;
    public static StringBuffer logMessage = null;
    public static List<String> colList = null;
    public static int parentSubjectId;
    public static String subjectName = "";
    public static MappingManagerUtil mappingManagerUtil = null;
    public static KeyValueUtil keyValueUtil = null;
    public static final String OBJECT_TYPE_ID = "8";
    public static Map<String, String> tableNameAgainstQueryMap = new LinkedHashMap<>();
    public static String replaceForTableCheckValue = "";
    public static Map<String, String> powerBIMetadata = new HashMap<>();
    public static boolean selectioncheck;
    public static Map<String, Map<String, String>> dashboardMap = new HashMap<>();
    public static ArrayList<MappingSpecificationRow> finalMappingSpecifications = null;
    public static String defSchema = "";
    public static Map<String, Set<String>> QueryTabelCoumnsMap = new LinkedHashMap<String, Set<String>>();
    public static Map<String, String> renamedCol = new HashMap<String, String>();
    public static Map<String, Map<String, ArrayList<MappingSpecificationRow>>> objectAgainstMapSpec = new HashMap<>();
    public static LinkedHashMap<String, LinkedHashMap<String, LinkedHashMap<String, String>>> columnPropertiesAgainstTargetTable = new LinkedHashMap();
    public static Map<String, LinkedHashMap<String, HashSet<String>>> keyValuesDeailsMapAgainstObject = new LinkedHashMap<>();
    public static Logger logger = Logger.getLogger(PowerBIReportParser.class);
    public static Map<String, String> expressionAgainstColumnMap = new HashMap<>();
    public static Map<String, String> datatypeAgainstColumnMap = new HashMap<>();
    public static Map<String, String> expressionAgainstMeasureMap = new HashMap<>();
    public static Map<String, String> descriptionAgainstColumnMap = new HashMap<>();
    public static HashMap<String, String> allTablesMap = null;
    public static HashMap<String, String> allDBMap = null;
    public static HashMap<String, String> cacheMap = null;
    public static String metadataJsonPath = "";
    public static File revisedxmlFile = null;
    private static String mappingReloadtype = "";
    public static Map<String, String> xmlDBMap = new HashMap<>();
    static List<Float> latestMappingVersion = null;
    static List<Float> latestMapVersion = null;
    static float updateMappingVersion = 0;
    static float latestMapV = 0;
    public static List<String> subDirectoryfilesList = null;
    public static Map<String, Map<String, Map<String, Set<String>>>> metadataMap = null;
    public static Map<String, Map<String, String>> extratecQueriesAgainstObject = new HashMap<>();
    public static Map<String, String> targetRealtionshipMap = new HashMap<>();
    public static Set<String> extreamResultSet = new HashSet<>();
    public static SystemManagerUtil systemManaegerUtil = null;
    public static PowerBI_Bean pbib = null;
    public static String DOCUMENT_PATH = "";
    public static String DOCUMENT_OBJECT = "";
    public static boolean IS_SYNC_ON = false;
    public static Map<String, Set<String>> columnsAgainstLogicalTable = new HashMap<>();
    Map<String, Set<String>> powerBIDashboardColumns = new HashMap<>();
    Map<String, Map<String, Set<String>>> powerBIPageColumns = new HashMap<>();
    Map<String, Map<String, Map<String, Set<String>>>> powerBILogicalTableColumns = new HashMap<>();
    public static boolean isSSAS = false;

    private static Map<String, Map<String, String>> REPORT_expressionAgainstColumnMap = new HashMap<>();
    private static Map<String, Map<String, String>> REPORT_datatypeAgainstColumnMap = new HashMap<>();
    private static Map<String, Map<String, String>> REPORT_expressionAgainstMeasureMap = new HashMap<>();
    private static Map<String, Map<String, String>> REPORT_descriptionAgainstColumnMap = new HashMap<>();

    public static MappingSpecificationRow getspecs(String src_table, String src_column, String tgt_tab, String tgt_col, String businessRule, String reportName) {

        String sourceSystemName = pbib.getSQL_SYSTEM_NAME();
        String sourceEnvironmentName = pbib.getSQL_ENVIRONMENT_NAME();
        String targetSystemName = pbib.getSQL_SYSTEM_NAME();
        String targetEnvironment = pbib.getSQL_ENVIRONMENT_NAME();
        String colDatatype = "";
        src_table = reportName + "." + src_table.replaceAll("[^a-zA-Z0-9_\\s\\-]", "").trim();
        MappingSpecificationRow spec = new MappingSpecificationRow();
        spec.setSourceSystemName(sourceSystemName);
        spec.setSourceSystemEnvironmentName(sourceEnvironmentName);
        spec.setSourceTableName(src_table);
        spec.setSourceColumnName(src_column);
        spec.setSourceColumnDatatype(colDatatype);
        spec.setTargetSystemName(targetSystemName);
        spec.setTargetSystemEnvironmentName(targetEnvironment);
        spec.setTargetTableName(tgt_tab);
        spec.setTargetColumnName(tgt_col);
        spec.setTargetColumnDatatype(colDatatype);
        spec.setExtendedBusinessRule(businessRule);
        return spec;

    }

    public static void parseMeasures() {
        PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Info::Preparing meeasure informations" + "\n");
        PowerBI_LOGGER.overviewLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Info::Preparing meeasure informations" + "\n");
        Pattern pattern = Pattern.compile("[\\'a-zA-Z0-9\\s\\_\\-']*\\[.*?\\]");
        Set<String> columns = new HashSet<>();
        for (Map.Entry<String, String> entry : expressionAgainstColumnMap.entrySet()) {
            try {
                String key = entry.getKey();
                if (key.contains("DateTable") || key.contains("LocalDateTable")) {
                    continue;
                }
                String value = entry.getValue();
                Matcher matcher = pattern.matcher(value);
                while (matcher.find()) {
                    String matchString = matcher.group();
                    matchString = matchString.replace("'", "");
                    String tableName = matchString.split("\\[")[0].trim();
                    String columnName = matchString.substring(matchString.indexOf("[") + 1, matchString.indexOf("]"));
                    String tgt_tableName = key.split("@ERWIN@")[0];
                    String tgt_columnName = key.split("@ERWIN@")[1];
                    if (!(tableName + "." + columnName).equals(tgt_tableName + "." + tgt_columnName)) {
                        if (PBITDataModelFileReader.relationshipMap.get(tableName + "." + columnName) != null && !PBITDataModelFileReader.relationshipMap.get(tableName + "." + columnName).contains(tgt_tableName + "." + tgt_columnName)) {
                            PBITDataModelFileReader.relationshipMap.put(tableName + "." + columnName, PBITDataModelFileReader.relationshipMap.get(tableName + "." + columnName) + "#ERWIN#" + tgt_tableName + "." + tgt_columnName + "@ERWIN@" + value);
                        } else {
                            PBITDataModelFileReader.relationshipMap.put(tableName + "." + columnName, tgt_tableName + "." + tgt_columnName + "@ERWIN@" + value);
                        }
                        if (targetRealtionshipMap.get(tgt_tableName + "." + tgt_columnName) != null && !targetRealtionshipMap.get(tgt_tableName + "." + tgt_columnName).contains(tableName + "." + columnName)) {
                            targetRealtionshipMap.put(tgt_tableName + "." + tgt_columnName, targetRealtionshipMap.get(tgt_tableName + "." + tgt_columnName) + "#ERWIN#" + tableName + "." + columnName + "@ERWIN@" + value);
                        } else {
                            targetRealtionshipMap.put(tgt_tableName + "." + tgt_columnName, tableName + "." + columnName + "@ERWIN@" + value);
                        }
                    }
                }
            } catch (Exception e) {
                logger.error(e.getMessage());
                StringWriter exception = new StringWriter();
                e.printStackTrace(new PrintWriter(exception));
                PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Error::Exception In PowerBIReportParser.parseMeasures() Method " + exception + "\n");
                exceptionLog.append("Exception In parseMeasures() \n" + exception.toString());
                exceptionLog.append("\n ================================");
            }
        }

        for (Map.Entry<String, String> entry : expressionAgainstMeasureMap.entrySet()) {
            try {
                String key = entry.getKey();
                String value = entry.getValue();
                Matcher matcher = pattern.matcher(value);
                while (matcher.find()) {
                    String matchString = matcher.group();
                    matchString = matchString.replace("'", "");
                    String tableName = matchString.split("\\[")[0];
                    String columnName = matchString.substring(matchString.indexOf("[") + 1, matchString.indexOf("]"));
                    String tgt_tableName = key.split("@ERWIN@")[0];
                    String tgt_columnName = key.split("@ERWIN@")[1];
                    if (!(tableName + "." + columnName).equals(tgt_tableName + "." + tgt_columnName)) {
                        if (PBITDataModelFileReader.relationshipMap.get(tableName + "." + columnName) != null && !PBITDataModelFileReader.relationshipMap.get(tableName + "." + columnName).contains(tgt_tableName + "." + tgt_columnName)) {
                            PBITDataModelFileReader.relationshipMap.put(tableName + "." + columnName, PBITDataModelFileReader.relationshipMap.get(tableName + "." + columnName) + "#ERWIN#" + tgt_tableName + "." + tgt_columnName + "@ERWIN@" + value);
                        } else {
                            PBITDataModelFileReader.relationshipMap.put(tableName + "." + columnName, tgt_tableName + "." + tgt_columnName + "@ERWIN@" + value);
                        }
                        if (targetRealtionshipMap.get(tgt_tableName + "." + tgt_columnName) != null && !targetRealtionshipMap.get(tgt_tableName + "." + tgt_columnName).contains(tableName + "." + columnName)) {
                            targetRealtionshipMap.put(tgt_tableName + "." + tgt_columnName, targetRealtionshipMap.get(tgt_tableName + "." + tgt_columnName) + "#ERWIN#" + tableName + "." + columnName + "@ERWIN@" + value);
                        } else {
                            targetRealtionshipMap.put(tgt_tableName + "." + tgt_columnName, tableName + "." + columnName + "@ERWIN@" + value);
                        }
                    }
                }
            } catch (Exception e) {
                logger.error(e.getMessage());
                StringWriter exception = new StringWriter();
                e.printStackTrace(new PrintWriter(exception));
                PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Error::Exception In PowerBIReportParser.parseMeasures() Method " + exception + "\n");

            }
        }

    }

    /**
     * This will set all the required parameter for the MappingSpecificationRow
     * and return that
     *
     * @param sourceSystemName
     * @param sourceSystemEnvironmentName
     * @param sourceTableName
     * @param sourceColumnName
     * @param targetSystemName
     * @param targetSystemEnvironmentName
     * @param targetTableName
     * @param targetColumnName
     * @param datatype
     * @param length
     * @param scale
     * @param precesion
     * @return MappingSpecificationRow
     */
    public static MappingSpecificationRow setMappingSpecificationForPBI(String sourceSystemName, String sourceSystemEnvironmentName, String sourceTableName, String sourceColumnName, String targetSystemName, String targetSystemEnvironmentName, String targetTableName, String targetColumnName, String displayName, String reportName) {

        MappingSpecificationRow mapspec = new MappingSpecificationRow();
        mapspec.setSourceSystemName(sourceSystemName);
        mapspec.setSourceSystemEnvironmentName(sourceSystemEnvironmentName);
        mapspec.setSourceTableName(sourceTableName.replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim());
        mapspec.setSourceColumnName(sourceColumnName.split("\\.")[0]);
        mapspec.setTargetSystemName(targetSystemName);
        mapspec.setTargetSystemEnvironmentName(targetSystemEnvironmentName);
        mapspec.setTargetTableName(targetTableName.replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim());
        mapspec.setTargetColumnName(targetColumnName);
        if (!sourceColumnName.equals(targetColumnName)) {
            sourceColumnName = targetColumnName;
        }
        if (PowerBIConfigPropertiesBuilder.businessRuleMap != null && !PowerBIConfigPropertiesBuilder.businessRuleMap.isEmpty()) {
            if (PowerBIConfigPropertiesBuilder.businessRuleMap.get(sourceTableName.toLowerCase().trim() + "#ERWIN#" + sourceColumnName.toLowerCase().trim() + "#ERWIN#" + displayName.toLowerCase().trim()) != null) {
                mapspec.setBusinessRule(PowerBIConfigPropertiesBuilder.businessRuleMap.get(sourceTableName.toLowerCase().trim() + "#ERWIN#" + sourceColumnName.toLowerCase().trim() + "#ERWIN#" + displayName.toLowerCase().trim()));
            } else if (PowerBIConfigPropertiesBuilder.businessRuleMap.get(sourceTableName.toLowerCase().trim() + "#ERWIN#" + targetColumnName.toLowerCase().trim() + "#ERWIN#" + displayName.toLowerCase().trim()) != null) {
                mapspec.setBusinessRule(PowerBIConfigPropertiesBuilder.businessRuleMap.get(sourceTableName.toLowerCase().trim() + "#ERWIN#" + targetColumnName.toLowerCase().trim() + "#ERWIN#" + displayName.toLowerCase().trim()));
            } else if (PowerBIConfigPropertiesBuilder.businessRuleMap.get(sourceTableName.toLowerCase().trim() + "#ERWIN#" + sourceColumnName.toLowerCase().replace(" ", "").trim() + "#ERWIN#" + displayName.toLowerCase().trim()) != null) {
                mapspec.setBusinessRule(PowerBIConfigPropertiesBuilder.businessRuleMap.get(sourceTableName.toLowerCase().trim() + "#ERWIN#" + sourceColumnName.toLowerCase().replace(" ", "").trim() + "#ERWIN#" + displayName.toLowerCase().trim()));
            } else if (PowerBIConfigPropertiesBuilder.businessRuleMap.get(sourceTableName.toLowerCase().trim() + "#ERWIN#" + targetColumnName.toLowerCase().replace(" ", "").trim() + "#ERWIN#" + displayName.toLowerCase().trim()) != null) {
                mapspec.setBusinessRule(PowerBIConfigPropertiesBuilder.businessRuleMap.get(sourceTableName.toLowerCase().trim() + "#ERWIN#" + targetColumnName.toLowerCase().replace(" ", "").trim() + "#ERWIN#" + displayName.toLowerCase().trim()));
            }
        }

        return mapspec;
    }

    public static MappingSpecificationRow setMappingSpecificationForPBI3(String sourceSystemName, String sourceSystemEnvironmentName, String sourceTableName, String sourceColumnName, String targetSystemName, String targetSystemEnvironmentName, String targetTableName, String targetColumnName, String displayName) {

        MappingSpecificationRow mapspec = new MappingSpecificationRow();
        mapspec.setSourceSystemName(sourceSystemName);
        mapspec.setSourceSystemEnvironmentName(sourceSystemEnvironmentName);
        mapspec.setSourceTableName(sourceTableName.replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim());
        mapspec.setSourceColumnName(sourceColumnName);
        mapspec.setTargetSystemName(targetSystemName);
        mapspec.setTargetSystemEnvironmentName(targetSystemEnvironmentName);
        mapspec.setTargetTableName(targetTableName.replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim());
        if (targetColumnName.toLowerCase().contains("date hierarchy")) {
            mapspec.setTargetColumnName(targetColumnName.split("\\.")[targetColumnName.split("\\.").length - 1]);
        } else {
            mapspec.setTargetColumnName(targetColumnName.split("\\.")[0]);
        }
        if (!sourceColumnName.equals(targetColumnName)) {
            sourceColumnName = targetColumnName;
        }
        if (PowerBIConfigPropertiesBuilder.businessRuleMap != null && !PowerBIConfigPropertiesBuilder.businessRuleMap.isEmpty()) {
            if (PowerBIConfigPropertiesBuilder.businessRuleMap.get(sourceTableName.toLowerCase().trim() + "#ERWIN#" + sourceColumnName.toLowerCase().trim() + "#ERWIN#" + displayName.toLowerCase().trim()) != null) {
                mapspec.setBusinessRule(PowerBIConfigPropertiesBuilder.businessRuleMap.get(sourceTableName.toLowerCase().trim() + "#ERWIN#" + sourceColumnName.toLowerCase().trim() + "#ERWIN#" + displayName.toLowerCase().trim()));
            } else if (PowerBIConfigPropertiesBuilder.businessRuleMap.get(sourceTableName.toLowerCase().trim() + "#ERWIN#" + targetColumnName.toLowerCase().trim() + "#ERWIN#" + displayName.toLowerCase().trim()) != null) {
                mapspec.setBusinessRule(PowerBIConfigPropertiesBuilder.businessRuleMap.get(sourceTableName.toLowerCase().trim() + "#ERWIN#" + targetColumnName.toLowerCase().trim() + "#ERWIN#" + displayName.toLowerCase().trim()));
            } else if (PowerBIConfigPropertiesBuilder.businessRuleMap.get(sourceTableName.toLowerCase().trim() + "#ERWIN#" + sourceColumnName.toLowerCase().replace(" ", "").trim() + "#ERWIN#" + displayName.toLowerCase().trim()) != null) {
                mapspec.setBusinessRule(PowerBIConfigPropertiesBuilder.businessRuleMap.get(sourceTableName.toLowerCase().trim() + "#ERWIN#" + sourceColumnName.toLowerCase().replace(" ", "").trim() + "#ERWIN#" + displayName.toLowerCase().trim()));
            } else if (PowerBIConfigPropertiesBuilder.businessRuleMap.get(sourceTableName.toLowerCase().trim() + "#ERWIN#" + targetColumnName.toLowerCase().replace(" ", "").trim() + "#ERWIN#" + displayName.toLowerCase().trim()) != null) {
                mapspec.setBusinessRule(PowerBIConfigPropertiesBuilder.businessRuleMap.get(sourceTableName.toLowerCase().trim() + "#ERWIN#" + targetColumnName.toLowerCase().replace(" ", "").trim() + "#ERWIN#" + displayName.toLowerCase().trim()));
            }
        }

        return mapspec;
    }

    public static MappingSpecificationRow setMappingSpecificationForSSAS(String sourceSystemName, String sourceSystemEnvironmentName, String sourceTableName, String sourceColumnName, String targetSystemName, String targetSystemEnvironmentName, String targetTableName, String targetColumnName, String displayName, String sourceTableForBusinessRule) {

        MappingSpecificationRow mapspec = new MappingSpecificationRow();
        mapspec.setSourceSystemName(sourceSystemName);
        mapspec.setSourceSystemEnvironmentName(sourceSystemEnvironmentName);
        mapspec.setSourceTableName(sourceTableName.replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim());
        mapspec.setSourceColumnName(sourceColumnName);
        mapspec.setTargetSystemName(targetSystemName);
        mapspec.setTargetSystemEnvironmentName(targetSystemEnvironmentName);
        mapspec.setTargetTableName(targetTableName.replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim());
        if (targetColumnName.toLowerCase().contains("date hierarchy")) {
            mapspec.setTargetColumnName(targetColumnName.split("\\.")[targetColumnName.split("\\.").length - 1]);
        } else {
            mapspec.setTargetColumnName(targetColumnName.split("\\.")[0]);
        }
        if (!sourceColumnName.equals(targetColumnName)) {
            sourceColumnName = targetColumnName;
        }
        sourceTableName = sourceTableForBusinessRule;
        if (PowerBIConfigPropertiesBuilder.businessRuleMap != null && !PowerBIConfigPropertiesBuilder.businessRuleMap.isEmpty()) {
            if (PowerBIConfigPropertiesBuilder.businessRuleMap.get(sourceTableName.toLowerCase().trim() + "#ERWIN#" + sourceColumnName.toLowerCase().trim() + "#ERWIN#" + displayName.toLowerCase().trim()) != null) {
                mapspec.setBusinessRule(PowerBIConfigPropertiesBuilder.businessRuleMap.get(sourceTableName.toLowerCase().trim() + "#ERWIN#" + sourceColumnName.toLowerCase().trim() + "#ERWIN#" + displayName.toLowerCase().trim()));
            } else if (PowerBIConfigPropertiesBuilder.businessRuleMap.get(sourceTableName.toLowerCase().trim() + "#ERWIN#" + targetColumnName.toLowerCase().trim() + "#ERWIN#" + displayName.toLowerCase().trim()) != null) {
                mapspec.setBusinessRule(PowerBIConfigPropertiesBuilder.businessRuleMap.get(sourceTableName.toLowerCase().trim() + "#ERWIN#" + targetColumnName.toLowerCase().trim() + "#ERWIN#" + displayName.toLowerCase().trim()));
            } else if (PowerBIConfigPropertiesBuilder.businessRuleMap.get(sourceTableName.toLowerCase().trim() + "#ERWIN#" + sourceColumnName.toLowerCase().replace(" ", "").trim() + "#ERWIN#" + displayName.toLowerCase().trim()) != null) {
                mapspec.setBusinessRule(PowerBIConfigPropertiesBuilder.businessRuleMap.get(sourceTableName.toLowerCase().trim() + "#ERWIN#" + sourceColumnName.toLowerCase().replace(" ", "").trim() + "#ERWIN#" + displayName.toLowerCase().trim()));
            } else if (PowerBIConfigPropertiesBuilder.businessRuleMap.get(sourceTableName.toLowerCase().trim() + "#ERWIN#" + targetColumnName.toLowerCase().replace(" ", "").trim() + "#ERWIN#" + displayName.toLowerCase().trim()) != null) {
                mapspec.setBusinessRule(PowerBIConfigPropertiesBuilder.businessRuleMap.get(sourceTableName.toLowerCase().trim() + "#ERWIN#" + targetColumnName.toLowerCase().replace(" ", "").trim() + "#ERWIN#" + displayName.toLowerCase().trim()));
            }
        }

        return mapspec;
    }

    public static MappingSpecificationRow setMappingSpecificationForPBI2(String sourceSystemName, String sourceSystemEnvironmentName, String sourceTableName, String sourceColumnName, String targetSystemName, String targetSystemEnvironmentName, String targetTableName, String targetColumnName, String displayName) {

        MappingSpecificationRow mapspec = new MappingSpecificationRow();
        if (sourceColumnName.toLowerCase().contains("date hierarchy")) {
            mapspec.setSourceColumnName(sourceColumnName.split("\\.")[sourceColumnName.split("\\.").length - 1].replace("\"", "").replace("*", ""));
        } else {
            mapspec.setSourceColumnName(sourceColumnName.split("\\.")[0].replace("\"", "").replace("*", ""));
        }
        mapspec.setSourceSystemName(sourceSystemName);
        mapspec.setSourceSystemEnvironmentName(sourceSystemEnvironmentName);
        mapspec.setSourceTableName(sourceTableName.replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim());
        mapspec.setTargetSystemName(targetSystemName);
        mapspec.setTargetSystemEnvironmentName(targetSystemEnvironmentName);
        mapspec.setTargetTableName(targetTableName.replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim());
        mapspec.setTargetColumnName(targetColumnName.replace("\"", "").replace("*", ""));
        if (!sourceColumnName.equals(targetColumnName)) {
            sourceColumnName = targetColumnName;
        }
        if (PowerBIConfigPropertiesBuilder.businessRuleMap != null && !PowerBIConfigPropertiesBuilder.businessRuleMap.isEmpty()) {
            if (PowerBIConfigPropertiesBuilder.businessRuleMap.get(sourceTableName.toLowerCase().trim() + "#ERWIN#" + sourceColumnName.toLowerCase().trim() + "#ERWIN#" + displayName.toLowerCase().trim()) != null) {
                mapspec.setBusinessRule(PowerBIConfigPropertiesBuilder.businessRuleMap.get(sourceTableName.toLowerCase().trim() + "#ERWIN#" + sourceColumnName.toLowerCase().trim() + "#ERWIN#" + displayName.toLowerCase().trim()));
            } else if (PowerBIConfigPropertiesBuilder.businessRuleMap.get(sourceTableName.toLowerCase().trim() + "#ERWIN#" + targetColumnName.toLowerCase().trim() + "#ERWIN#" + displayName.toLowerCase().trim()) != null) {
                mapspec.setBusinessRule(PowerBIConfigPropertiesBuilder.businessRuleMap.get(sourceTableName.toLowerCase().trim() + "#ERWIN#" + targetColumnName.toLowerCase().trim() + "#ERWIN#" + displayName.toLowerCase().trim()));
            } else if (PowerBIConfigPropertiesBuilder.businessRuleMap.get(sourceTableName.toLowerCase().trim() + "#ERWIN#" + sourceColumnName.toLowerCase().replace(" ", "").trim() + "#ERWIN#" + displayName.toLowerCase().trim()) != null) {
                mapspec.setBusinessRule(PowerBIConfigPropertiesBuilder.businessRuleMap.get(sourceTableName.toLowerCase().trim() + "#ERWIN#" + sourceColumnName.toLowerCase().replace(" ", "").trim() + "#ERWIN#" + displayName.toLowerCase().trim()));
            } else if (PowerBIConfigPropertiesBuilder.businessRuleMap.get(sourceTableName.toLowerCase().trim() + "#ERWIN#" + targetColumnName.toLowerCase().replace(" ", "").trim() + "#ERWIN#" + displayName.toLowerCase().trim()) != null) {
                mapspec.setBusinessRule(PowerBIConfigPropertiesBuilder.businessRuleMap.get(sourceTableName.toLowerCase().trim() + "#ERWIN#" + targetColumnName.toLowerCase().replace(" ", "").trim() + "#ERWIN#" + displayName.toLowerCase().trim()));
            }
        }
        return mapspec;
    }

    public static void setInitialLog(Map<String, String> optionsProperties) {
        logMessage = new StringBuffer();
        String logHeader = "\n#############################################################\n" + "################ POWERBI Reverse Engineer Log ###############\n"
                + "#############################################################\n\n";
        logMessage.append(logHeader);
        logMessage.append("ProjecName  : " + optionsProperties.get(PowerBI_Constanst.OPTION_KEY_PROJECT_NAME) + "\n");
        logMessage.append("Subject Name : " + optionsProperties.get(PowerBI_Constanst.OPTION_KEY_UPLOAD_SUBJECT) + "\n");
        logMessage.append("Version Control : " + optionsProperties.get(PowerBI_Constanst.OPTION_KEY_MAPPING_VERSION) + "\n");
        logMessage.append("Default Schema Name : " + optionsProperties.get(PowerBI_Constanst.OPTION_KEY_DEF_SCHEMA) + "\n");
        logMessage.append("Input File Option : " + optionsProperties.get(PowerBI_Constanst.OPTION_KEY_INPUT_OPTION) + "\n");
        logMessage.append("ArchivePath : " + optionsProperties.get(PowerBI_Constanst.OPTION_KEY_ARCHIVE_FOLDER_PATH) + "\n");
        logMessage.append("\n\n=============================================================================================================\n\n");
        logMessage.append("********************************************\nMAPPING LOG\n********************************************\n\n");
    }

    public static void declareGlobalReferences(MappingManagerUtil managerUtil, KeyValueUtil kvUtil, SystemManagerUtil smutil, Map<String, String> optionsProperties) {
        if (optionsProperties.get(PowerBI_Constanst.OPTION_KEY_SYNC_STATUS).equals("true")) {
            IS_SYNC_ON = true;
        } else {
            IS_SYNC_ON = false;
        }
        xmlDBMap.clear();
        DOCUMENT_PATH = "\\Projects\\" + optionsProperties.get(PowerBI_Constanst.OPTION_KEY_PROJECT_NAME) + "\\";
        DOCUMENT_OBJECT = optionsProperties.get(PowerBI_Constanst.OPTION_KEY_SQL_SUPPORT_PATH) + "\\Projects\\" + optionsProperties.get(PowerBI_Constanst.OPTION_KEY_PROJECT_NAME) + "\\";
        metadataMap = new HashMap();
        metadataMap.clear();
        exceptionLog = new StringBuffer();
        systemManaegerUtil = smutil;
        pbib = new PowerBI_Bean();
        pbib.setMappingManagerUtil(managerUtil);
        pbib.setSystemManagerUtil(smutil);
        pbib.setKeyValueUtil(kvUtil);

        pbib.setSQL_SYSTEM_NAME(optionsProperties.get(PowerBI_Constanst.OPTION_KEY_DEF_SQL_SYSTEM));
        pbib.setSQL_ENVIRONMENT_NAME(optionsProperties.get(PowerBI_Constanst.OPTION_KEY_DEF_SQL_ENV));
        pbib.setPOWERBI_SYSTEM(optionsProperties.get(PowerBI_Constanst.OPTION_KEY_DEF_POWERBI_SYSTEM));
        pbib.setPOWERBI_ENV_PAGE(optionsProperties.get(PowerBI_Constanst.OPTION_KEY_DEF_PAGE_ENV));
        pbib.setPOWERBI_ENV_DASHBOARD(optionsProperties.get(PowerBI_Constanst.OPTION_KEY_DEF_DASHBOARD_ENV));
        pbib.setSSAS_SYSTEM_NAME(optionsProperties.get(PowerBI_Constanst.OPTION_KEY_DEF_SSAS_SYSTEM));
        pbib.setPOWERBI_ENV_LOGICAL(optionsProperties.get(PowerBI_Constanst.OPTION_KEY_DEF_LOGICAL_ENV));

        subjectName = optionsProperties.get(PowerBI_Constanst.OPTION_KEY_SUBJECT_NAME);
        defSchema = optionsProperties.get(PowerBI_Constanst.OPTION_KEY_DEF_SCHEMA);
        subDirectoryfilesList = new ArrayList();
        mappingReloadtype = optionsProperties.get(PowerBI_Constanst.OPTION_KEY_MAPPING_VERSION);
        pbib.setODATA_SYSTEM_NAME(optionsProperties.get(PowerBI_Constanst.OPTION_KEY_ODATA_DEF_SYS));
        pbib.setODATA_ENVIRONEMNT_NAME(optionsProperties.get(PowerBI_Constanst.OPTION_KEY_ODATA_DEF_ENV));
        allTablesMap = new HashMap<String, String>();
        cacheMap = new HashMap<String, String>();
        allDBMap = new HashMap<String, String>();
        metadataJsonPath = optionsProperties.get(PowerBI_Constanst.OPTION_KEY_METADATA_JSON);
        if (metadataJsonPath.lastIndexOf("\\") != metadataJsonPath.length() - 1) {
            metadataJsonPath = metadataJsonPath + "\\";
        }
        tableNameAgainstQueryMap = new LinkedHashMap<>();
        replaceForTableCheckValue = optionsProperties.get(PowerBI_Constanst.OPTION_KEY_REPLACE_TABLE);
        mappingManagerUtil = managerUtil;
        keyValueUtil = kvUtil;
    }

    /**
     * This id the main method of our PowerBI Smart Connector, In this method we
     * call all the method from different classes perform multiple operation and
     * create mapping here
     *
     * @param filePath
     * @param managerUtil
     * @param smutil
     * @param projectId
     * @param zipDir
     * @param kvUtil
     * @param sourceSystemName
     * @param sourceEnvironmentName
     * @param targetSystemName
     * @param targetEnvironmentName
     * @param envMap
     * @param replaceForTableCheck
     * @param propertiesFilePath
     * @param connectionSystemName
     * @param yamlFilePath
     * @param subName
     * @param defaultSchema
     * @param fileSaperator
     * @return returns the result String
     *
     * @throws IOException
     */
    public static String buildZipPBIXFile(MappingManagerUtil managerUtil, SystemManagerUtil smutil, KeyValueUtil kvUtil, LinkedHashMap<String, String> envMap, Map<String, String> optionsProperties) {
        declareGlobalReferences(managerUtil, kvUtil, smutil, optionsProperties);
        PowerBI_LOGGER.initializeLogger(optionsProperties);
        long startTime = System.currentTimeMillis();
        String filePath = optionsProperties.get(PowerBI_Constanst.OPTION_KEY_FILE_PATH);
        Date date = Calendar.getInstance().getTime();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyymmdd_hhmmss");
        String fileDate = dateFormat.format(date);
        ReadXMLFileToObject readXMLFileToObject = new ReadXMLFileToObject();
        PBITDataModelFileReader dataModelFileReader = new PBITDataModelFileReader();
        setInitialLog(optionsProperties);
        int uparentSubjectId = -1;
        int pbixfileCount = 0;
        int pbitfileCount = 0;
        ObjectMapper mapper = new ObjectMapper();
        boolean hiddenPageInfo = Boolean.parseBoolean(optionsProperties.get(PowerBI_Constanst.OPTION_KEY_HIDDEN_PAGE_INFO));
        boolean isQueryCreate = Boolean.parseBoolean(optionsProperties.get(PowerBI_Constanst.OPTION_KEY_IS_QUERY_CREATE));
        boolean isPageCreate = Boolean.parseBoolean(optionsProperties.get(PowerBI_Constanst.OPTION_KEY_IS_PAGE_CREATE));
        boolean isDashboardCreate = Boolean.parseBoolean(optionsProperties.get(PowerBI_Constanst.OPTION_KEY_IS_DASHBOARD_CREATE));
        pbib.setCheckDashboardEnvironmentCreation(isDashboardCreate);
        pbib.setCheckLogicalEnvironmentCreation(isQueryCreate);
        pbib.setCheckPageEnvironmentCreation(isPageCreate);
        pbib.setHiddenPageInfo(hiddenPageInfo);
        PowerBI_DataflowUtil pbidu = new PowerBI_DataflowUtil();
        pbib.setDataflowUtil(pbidu);
        pbib.setSTORED_PROC_SYSTEM_NAME(optionsProperties.get(PowerBI_Constanst.OPTION_KEY_STORED_PROC_SYSTEM));
        pbib.setSTORED_PROC_ENVIRONMENT_EXTENSION(optionsProperties.get(PowerBI_Constanst.OPTION_KEY_STORED_PROC_ENV_EXTENSION));
        pbib.setPHYSICAL_SUBJECT(optionsProperties.get(PowerBI_Constanst.OPTION_KEY_PHYSICAL_INFO_SUBJECT_NAME));
        pbib.setPROJECT_ID(optionsProperties.get(PowerBI_Constanst.OPTION_KEY_PROJECT_ID));
        pbib.setMAP_RELOAD_TYPE(optionsProperties.get(PowerBI_Constanst.OPTION_KEY_MAPPING_VERSION));
        File inputMetaJsonFile = new File(metadataJsonPath);
        pbib.setMetadataJsonPath(metadataJsonPath);
        if (!"".equals(metadataJsonPath)
                && (inputMetaJsonFile.exists())) {
            allTablesMap = SyncupWithServerDBSchamaSysEnvCPT.getMap(metadataJsonPath, "Tables");
            allDBMap = SyncupWithServerDBSchamaSysEnvCPT.getMap(metadataJsonPath, "Databases");
            PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "Info::MetadatJson File Path=" + metadataJsonPath + "\n");
            PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "Info::Parsing Metadata Json File to get metadata info for Synup of Mappings " + "\n");
            PowerBI_LOGGER.overviewLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "Info::MetadatJson File Path=" + metadataJsonPath + "\n");
            PowerBI_LOGGER.overviewLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "Info::Parsing Metadata Json File to get metadata info for Synup of Mappings " + "\n");
        } else if (inputMetaJsonFile.exists()) {
            PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "Warn::given metadatajson file is not present in the directory " + metadataJsonPath + " \n");
        }
        pbib.setAllDBMap(allDBMap);
        pbib.setAllTablesMap(allTablesMap);
        pbib.setMetadataMap(metadataMap);
        pbib.setDEFAULT_SCHEMA(defSchema);
        String keeporDeleteOrArchiveSourceFile = optionsProperties.get(PowerBI_Constanst.OPTION_KEY_DELETE_ARCHIVE_STATUS).trim();
        String archiveFolderPath = optionsProperties.get(PowerBI_Constanst.OPTION_KEY_ARCHIVE_FOLDER_PATH).trim();
        Map<String, String> queryFromXmlAgainstObjectMap = new HashMap<String, String>();
        StringBuilder sb = new StringBuilder();

        File sourceFileDir = new File(optionsProperties.get(PowerBI_Constanst.OPTION_KEY_FILE_PATH));
        File files[] = null;
        if (sourceFileDir.isDirectory()) {
            files = sourceFileDir.listFiles();
        } else {
            files = new File[1];
            files[0] = sourceFileDir;
        }
        boolean is_file = true;
        if (sourceFileDir.isDirectory() && optionsProperties.get(PowerBI_Constanst.OPTION_KEY_INPUT_OPTION).equalsIgnoreCase("File path")) {
            is_file = true;
        } else {
            is_file = false;
        }
        if (files.length == 0) {
            PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "Warn::There is no files provided file path" + "\n");
            return "There is no files provided file path.";
        }
        try {
            File sourceFile = null;
            Map<String, List<Map<String, String>>> bruleMap = new HashMap<>();
            if (!StringUtils.isBlank(optionsProperties.get(PowerBI_Constanst.OPTION_KEY_UPLOAD_SUBJECT))) {
                PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "Info::Api call for Subject Creation with the SubjectName " + optionsProperties.get(PowerBI_Constanst.OPTION_KEY_UPLOAD_SUBJECT) + "\n");
                PowerBI_LOGGER.overviewLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "Info::Api call for Subject Creation with the SubjectName " + optionsProperties.get(PowerBI_Constanst.OPTION_KEY_UPLOAD_SUBJECT) + "\n");
                uparentSubjectId = CreationOfSubjectHirerachy.parentSubjectId(optionsProperties.get(PowerBI_Constanst.OPTION_KEY_UPLOAD_SUBJECT), mappingManagerUtil, optionsProperties.get(PowerBI_Constanst.OPTION_KEY_PROJECT_NAME), Integer.parseInt(optionsProperties.get(PowerBI_Constanst.OPTION_KEY_PROJECT_ID)));
            }
            for (File file : files) {
                if (file.isDirectory()) {
                    callSubdirectories(file);
                } else {
                    subDirectoryfilesList.add(file.getAbsolutePath());
                }
            }
            for (String eachFile : subDirectoryfilesList) {
                File file = new File(eachFile);
                sourceFile = file;
                String zipDirPath = sourceFile.getPath();
                String zipFileName = zipDirPath.substring(zipDirPath.lastIndexOf(optionsProperties.get(PowerBI_Constanst.OPTION_KEY_FILE_SEPARATOR)) + 1);
                if (zipFileName.contains(PowerBI_Constanst.EXTENSION_PBIX)) {
                    pbixfileCount++;
                } else if (zipFileName.contains(PowerBI_Constanst.EXTENSION_PBIT)) {
                    pbitfileCount++;
                }
            }
            logMessage.append("\nThe Total File Count : " + subDirectoryfilesList.size() + "\n\n");
            logMessage.append("\nThe pbix File Count : " + pbixfileCount + "\n\n");
            logMessage.append("\nThe pbit File Count : " + pbitfileCount + "\n\n");
            PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + " ::Info:: Total File Count " + subDirectoryfilesList.size() + "\n");
            PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + " ::Info:: The pbix File Count " + pbixfileCount + "\n");
            PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + " ::Info:: The pbit File Count " + pbitfileCount + "\n");
            PowerBI_LOGGER.overviewLogger.append(PowerBI_LOGGER.getTimestampForLogging() + " ::Info:: Total File Count " + subDirectoryfilesList.size() + "\n");
            PowerBI_LOGGER.overviewLogger.append(PowerBI_LOGGER.getTimestampForLogging() + " ::Info:: The pbix File Count " + pbixfileCount + "\n");
            PowerBI_LOGGER.overviewLogger.append(PowerBI_LOGGER.getTimestampForLogging() + " ::Info:: The pbit File Count " + pbitfileCount + "\n");
            for (String eachFile : subDirectoryfilesList) {
                try {
                    targetRealtionshipMap.clear();
                    isSSAS = false;
                    pbib.setIsSSAS(isSSAS);
                    ReadXMLFileToObject.MergeQuerySpecs.clear();
                    int uparentSubjectId1 = -1;
                    finalMappingSpecifications = new ArrayList<>();
                    File file = new File(eachFile);
                    sourceFile = file;
                    String absolutefilePath = file.getAbsolutePath();
                    logMessage.append("\nPower BI Report : " + absolutefilePath + "\n\n");
                    PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Info::Current Processing PowerBI File " + file.getName() + "\n");
                    PowerBI_LOGGER.overviewLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Info::Current Processing PowerBI File " + file.getName() + "\n");
                    String zipDirPath = sourceFile.getPath();
                    String zipFileName = zipDirPath.substring(zipDirPath.lastIndexOf(optionsProperties.get(PowerBI_Constanst.OPTION_KEY_FILE_SEPARATOR)) + 1);
                    if (!zipFileName.contains(PowerBI_Constanst.EXTENSION_PBIX) && !zipFileName.contains(PowerBI_Constanst.EXTENSION_PBIT)) {
                        PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::warn::given file is invalid  does not contain pbit or pbix file extension" + "\n");
                        logMessage.append("\nPower BI Report : " + absolutefilePath + "\n\nInvalid File");
                        continue;
                    }
                    zipFileName = zipFileName.replace(PowerBI_Constanst.EXTENSION_PBIT, "").replace(PowerBI_Constanst.EXTENSION_PBIX, "");
                    filePath = FilenameUtils.normalizeNoEndSeparator(filePath, true);
                    filePath = filePath + "/";
                    absolutefilePath = FilenameUtils.normalizeNoEndSeparator(absolutefilePath, true);
                    absolutefilePath = absolutefilePath.replace(filePath, "");
                    absolutefilePath = absolutefilePath.replace(zipFileName, "").replace(PowerBI_Constanst.EXTENSION_PBIX, "").replace(PowerBI_Constanst.EXTENSION_PBIT, "").trim();
                    if (optionsProperties.get(PowerBI_Constanst.OPTION_KEY_INPUT_OPTION).equalsIgnoreCase("File path") && !StringUtils.isBlank(absolutefilePath) && is_file) {
                        PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "Info::Api call for Subject Creation with the SubjectName " + FilenameUtils.normalizeNoEndSeparator(absolutefilePath, true) + "\n");
                        PowerBI_LOGGER.overviewLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "Info::Api call for Subject Creation with the SubjectName " + FilenameUtils.normalizeNoEndSeparator(absolutefilePath, true) + "\n");
                        uparentSubjectId1 = CreationOfSubjectHirerachy.parentSubjectId(absolutefilePath, mappingManagerUtil, optionsProperties.get(PowerBI_Constanst.OPTION_KEY_PROJECT_NAME), Integer.parseInt(optionsProperties.get(PowerBI_Constanst.OPTION_KEY_PROJECT_ID)), uparentSubjectId);
                    }
                    String indivisualZipDir = optionsProperties.get(PowerBI_Constanst.OPTION_KEY_ZIP_DIR) + "/" + zipFileName + "/";
                    File targetDir = new File(indivisualZipDir);
                    if (!targetDir.isDirectory()) {
                        targetDir.mkdir();
                    }
                    String targetZipFile = targetDir + "/" + zipFileName + ".zip";
                    File sourceLayoutFile = new File(targetDir + "/Report/Layout");
                    revisedxmlFile = new File(targetDir + "/DataMashup");
                    File connectionFile = new File(targetDir + "/Connections");
                    File datamodelSchemaFile = new File(targetDir + "/DataModelSchema");
                    FileUtils.copyFile(sourceFile, new File(targetDir + "/" + zipFileName + ".zip"));
                    unzip(targetZipFile, indivisualZipDir);
                    String[] sysEnvDetail = {pbib.getSQL_SYSTEM_NAME(), pbib.getSQL_ENVIRONMENT_NAME(), pbib.getSQL_SYSTEM_NAME(), pbib.getSQL_ENVIRONMENT_NAME()};
                    zipFileName = zipFileName.replaceAll("[^a-zA-Z0-9_\\s]", "_");
                    zipFileName = zipFileName.replace(".", "_");
                    pbib.setPOWERBI_REPORT_NAME(zipFileName.toUpperCase());
                    PowerBI_ConnectionFile bI_ConnectionFile = new PowerBI_ConnectionFile();
                    isSSAS = bI_ConnectionFile.readConnectionFile(connectionFile);
                    if (isSSAS) {
                        pbib.setIsSSAS(isSSAS);
                        pbib.setSSAS_ENVIRONMENT_NAME(bI_ConnectionFile.connectionEnvName);
                    }
                    if (datamodelSchemaFile.exists()) {
                        PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Info::DataModelSchema File Present in given powerbi file" + "\n");
                        PowerBI_LOGGER.overviewLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Info::DataModelSchema File Present in given powerbi file" + "\n");
                        ReadXMLFileToObject.MergeQuerySpecs.clear();
                        ReadXMLFileToObject.participatedTableAgainstMergedQuery.clear();
                        com.erwin.cfx.connectors.pbi.map.v11.PBITDataModelFileReader.expressionAgainstColumnMap.clear();
                        com.erwin.cfx.connectors.pbi.map.v11.PBITDataModelFileReader.datatypeAgainstColumnMap.clear();
                        com.erwin.cfx.connectors.pbi.map.v11.PBITDataModelFileReader.expressionAgainstMeasureMap.clear();
                        com.erwin.cfx.connectors.pbi.map.v11.PBITDataModelFileReader.descriptionAgainstColumnMap.clear();
                        PBITDataModelFileReader.itemPathAndQueryMap.clear();
                        PBITDataModelFileReader.snowflakeQuery = "";
                        PBITDataModelFileReader.putQueriesInDataMashup(datamodelSchemaFile, zipFileName.toUpperCase());
                        pbib.getDataflowUtil().parseDataflowJSON(subDirectoryfilesList, File.pathSeparator);
                        PBITDataModelFileReader.readDataModelFile(datamodelSchemaFile, sysEnvDetail, zipFileName.toUpperCase(), pbib);
                        expressionAgainstColumnMap = com.erwin.cfx.connectors.pbi.map.v11.PBITDataModelFileReader.expressionAgainstColumnMap;
                        datatypeAgainstColumnMap = com.erwin.cfx.connectors.pbi.map.v11.PBITDataModelFileReader.datatypeAgainstColumnMap;
                        expressionAgainstMeasureMap = com.erwin.cfx.connectors.pbi.map.v11.PBITDataModelFileReader.expressionAgainstMeasureMap;
                        descriptionAgainstColumnMap = com.erwin.cfx.connectors.pbi.map.v11.PBITDataModelFileReader.descriptionAgainstColumnMap;
                        parseMeasures();
                        queryFromXmlAgainstObjectMap = PBITDataModelFileReader.itemPathAndQueryMap;
//                        getMapSpecAndTableColumnsParsingQuery2(queryFromXmlAgainstObjectMap, sysEnvDetail, envMap, smutil, pbib.getSQL_SYSTEM_NAME(), pbib.getSQL_ENVIRONMENT_NAME(), optionsProperties.get(PowerBI_Constanst.OPTION_KEY_DEF_SCHEMA));
                        datamodelSchemaFile.delete();
                    } else if (revisedxmlFile.exists()) {
                        PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Info::DataMashup File Present in given powerbi file" + "\n");
                        PowerBI_LOGGER.overviewLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Info::DataMashup File Present in given powerbi file" + "\n");
                        ReadXMLFileToObject.MergeQuerySpecs.clear();
                        ReadXMLFileToObject.participatedTableAgainstMergedQuery.clear();
                        DataMashupExtractor.parseDataMashupXML(revisedxmlFile, sysEnvDetail, smutil, zipFileName.toUpperCase());
                        queryFromXmlAgainstObjectMap = readXMLFileToObject.readXMLFileToObjectForQueryAndFlatFile1(revisedxmlFile, sysEnvDetail, zipFileName.toUpperCase());
//                        getMapSpecAndTableColumnsParsingQuery2(queryFromXmlAgainstObjectMap, sysEnvDetail, envMap, smutil, pbib.getSQL_SYSTEM_NAME(), pbib.getSQL_ENVIRONMENT_NAME(), optionsProperties.get(PowerBI_Constanst.OPTION_KEY_DEF_SCHEMA));
                    }

                    pbib.setExpressionAgainstColumnMap(expressionAgainstColumnMap);
                    pbib.setExpressionAgainstMeasureMap(expressionAgainstMeasureMap);
                    pbib.setDatatypeAgainstColumnMap(datatypeAgainstColumnMap);

                    Map<String, String> expressionAgainstColumnMap_1 = new HashMap<>();
                    expressionAgainstColumnMap_1.putAll(expressionAgainstColumnMap);
                    Map<String, String> datatypeAgainstColumnMap_1 = new HashMap<>();
                    datatypeAgainstColumnMap_1.putAll(datatypeAgainstColumnMap);
                    Map<String, String> expressionAgainstMeasureMap_1 = new HashMap<>();
                    expressionAgainstMeasureMap_1.putAll(expressionAgainstMeasureMap);
                    Map<String, String> descriptionAgainstColumnMap_1 = new HashMap<>();
                    descriptionAgainstColumnMap_1.putAll(descriptionAgainstColumnMap);
                    REPORT_expressionAgainstColumnMap.put(pbib.getPOWERBI_REPORT_NAME(), expressionAgainstColumnMap_1);
                    REPORT_expressionAgainstMeasureMap.put(pbib.getPOWERBI_REPORT_NAME(), expressionAgainstMeasureMap_1);
                    REPORT_datatypeAgainstColumnMap.put(pbib.getPOWERBI_REPORT_NAME(), datatypeAgainstColumnMap_1);
                    REPORT_descriptionAgainstColumnMap.put(pbib.getPOWERBI_REPORT_NAME(), descriptionAgainstColumnMap_1);
                    pbib.setSqlQuery(queryFromXmlAgainstObjectMap);
                    pbib.setDbTypes(xmlDBMap);
                    revisedxmlFile.delete();
                    int subId = 0;
                    int actualParentSubjectId = 0;
                    if (optionsProperties.get(PowerBI_Constanst.OPTION_KEY_INPUT_OPTION).equalsIgnoreCase("File path") && !StringUtils.isBlank(absolutefilePath) && is_file) {
                        subId = mappingManagerUtil.getSubjectId(uparentSubjectId1, com.ads.api.beans.common.Node.NodeType.MM_SUBJECT, zipFileName);
                    } else if (StringUtils.isBlank(optionsProperties.get(PowerBI_Constanst.OPTION_KEY_UPLOAD_SUBJECT))) {
                        subId = mappingManagerUtil.getSubjectId(Integer.parseInt(optionsProperties.get(PowerBI_Constanst.OPTION_KEY_PROJECT_ID)), com.ads.api.beans.common.Node.NodeType.MM_PROJECT, zipFileName);
                    }
                    if (subId > 0) {
                        parentSubjectId = subId;
                    } else {
                        Subject subject = new Subject();
                        subject.setProjectId(Integer.parseInt(optionsProperties.get(PowerBI_Constanst.OPTION_KEY_PROJECT_ID)));
                        if (optionsProperties.get(PowerBI_Constanst.OPTION_KEY_INPUT_OPTION).equalsIgnoreCase("File path") && !StringUtils.isBlank(absolutefilePath) && is_file) {
                            subject.setParentSubjectId(uparentSubjectId1);
                            actualParentSubjectId = uparentSubjectId1;
                        } else {
                            subject.setParentSubjectId(uparentSubjectId);
                            actualParentSubjectId = uparentSubjectId;
                        }

                        subject.setSubjectName(zipFileName);
                        mappingManagerUtil.createSubject(subject);
                        if ((optionsProperties.get(PowerBI_Constanst.OPTION_KEY_INPUT_OPTION).equalsIgnoreCase("File path") && !StringUtils.isBlank(absolutefilePath)) || !StringUtils.isBlank(optionsProperties.get(PowerBI_Constanst.OPTION_KEY_UPLOAD_SUBJECT))) {
                            parentSubjectId = mappingManagerUtil.getSubjectId(actualParentSubjectId, com.ads.api.beans.common.Node.NodeType.MM_SUBJECT, zipFileName);
                        } else if (StringUtils.isBlank(optionsProperties.get(PowerBI_Constanst.OPTION_KEY_UPLOAD_SUBJECT))) {
                            parentSubjectId = mappingManagerUtil.getSubjectId(Integer.parseInt(optionsProperties.get(PowerBI_Constanst.OPTION_KEY_PROJECT_ID)), com.ads.api.beans.common.Node.NodeType.MM_PROJECT, zipFileName);
                        }
                    }
                    pbib.setPARENT_SUBJECT_ID(parentSubjectId);
                    PowerBI_SQLUtil bI_SQLUtil = new PowerBI_SQLUtil();
                    PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Info::Creating SQL Mapping for PowerBI report : " + zipFileName + "\n");
                    PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Info::Reading Layout file for PowerBI report : " + zipFileName + "\n");
                    PowerBI_LOGGER.overviewLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Info::Reading Layout file for PowerBI report : " + zipFileName + "\n");
                    sb.append(getLayoutJsonContent(smutil, sourceLayoutFile, zipFileName, kvUtil, queryFromXmlAgainstObjectMap, envMap, sysEnvDetail, optionsProperties)).append("\n");
                    pbib.setLogicalMap(columnsAgainstLogicalTable);
//                    if (!isSSAS) {
                    bI_SQLUtil.processQuery(pbib);
//                    }
                    try {
                        connectionFile.delete();
                        sourceLayoutFile.delete();
                    } catch (Exception ie) {
                        StringWriter exception = new StringWriter();
                        ie.printStackTrace(new PrintWriter(exception));
                        PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Error::Exception In PowerBIReportParser.buildZipPBIXFile() Method " + exception + "\n");
                    }
                    Mapping finalMap = null;
                    PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Info::Creating Dashborad Mapping for PowerBI report : " + zipFileName + "\n");
                    PowerBI_LOGGER.overviewLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Info::Creating Dashborad Mapping for PowerBI report : " + zipFileName + "\n");
                    createFinalMapping(zipFileName, optionsProperties.get(PowerBI_Constanst.OPTION_KEY_PROJECT_ID), subId, sb);
                    PowerBI_LOGGER.overviewLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Info::Dashbord Mapping created\n");
                    PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Info::Dashbord Mapping created\n");
                } catch (Exception exxx) {
                    logger.error(exxx.getMessage());
                    StringWriter exception = new StringWriter();
                    exxx.printStackTrace(new PrintWriter(exception));
                    PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Error::Exception In PowerBIReportParser.buildZipPBIXFile() Method " + exception + "\n");
                    exceptionLog.append("Exception In buildZipPBIXFile() \n" + exception.toString());
                    exceptionLog.append("\n ================================");
                }
                expressionAgainstColumnMap.clear();
                expressionAgainstMeasureMap.clear();
                datatypeAgainstColumnMap.clear();
                descriptionAgainstColumnMap.clear();
            }
            pbib.setREPORT_datatypeAgainstColumnMap(REPORT_datatypeAgainstColumnMap);
            pbib.setREPORT_expressionAgainstColumnMap(REPORT_expressionAgainstColumnMap);
            pbib.setREPORT_expressionAgainstMeasureMap(REPORT_expressionAgainstMeasureMap);
            pbib.setREPORT_descriptionAgainstColumnMap(REPORT_descriptionAgainstColumnMap);
            allDBMap.clear();
            allTablesMap.clear();
            if (optionsProperties.get(PowerBI_Constanst.OPTION_KEY_CHECK_METADATA_CREATION).equals("true")) {
                PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Info::Creating Logical Metadata\n");
                PowerBI_LOGGER.overviewLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Info::Creating Logical Metadata\n");
                MetadataInsertion_PowerBI.insertMetadata(smutil, metadataMap, optionsProperties.get(PowerBI_Constanst.OPTION_KEY_CHECK_METADATA_VERSIONING), pbib.getPOWERBI_SYSTEM(), pbib.getPOWERBI_SYSTEM(), pbib);
                PowerBI_LOGGER.overviewLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Info::Logical Metadata creation completed\n");
                PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Info::Logical Metadata creation completed\n");
                logMessage.append("********************************************\nMETADATA LOG\n********************************************\n" + MetadataInsertion_PowerBI.message.toString());
            }
            String fileName = optionsProperties.get(PowerBI_Constanst.OPTION_KEY_LOG_FILE_PATH) + "PowerBI_Log_" + fileDate + ".log";
            logMessage.append("********************************************\nEXCEPTION LOG\n********************************************\n" + exceptionLog.toString());
            sb.append(MetadataInsertion_PowerBI.message);
            allTablesMap.clear();
            try {
                File file = new File(fileName);
                if (!file.exists()) {
                    FileUtils.touch(file);
                }
                PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "Footer::Total Connector Execution Time in milliseconds::" + (startTime - System.currentTimeMillis()) + "\n");
                PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "Footer::Execution Completed Time=" + (new Date().toString()) + "\n");
                PowerBI_LOGGER.overviewLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "Footer::Total Connector Execution Time in milliseconds::" + (startTime - System.currentTimeMillis()) + "\n");
                PowerBI_LOGGER.overviewLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "Footer::Execution Completed Time=" + (new Date().toString()) + "\n");
                if (optionsProperties.get(PowerBI_Constanst.OPTION_KEY_LOG_TYPE).equalsIgnoreCase("detail log")) {
                    FileUtils.writeStringToFile(file, PowerBI_LOGGER.detailLogger.toString(), "UTF8");
                } else {
                    FileUtils.writeStringToFile(file, PowerBI_LOGGER.overviewLogger.toString(), "UTF8");
                }
            } catch (Exception e) {
                StringWriter exception = new StringWriter();
                e.printStackTrace(new PrintWriter(exception));
                PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Error::Exception In PowerBIReportParser.buildZipPBIXFile() Method " + exception + "\n");
                exceptionLog.append(exception.toString());
                exceptionLog.append("\n ================================");
            }
            if ("Delete Source File".equals(keeporDeleteOrArchiveSourceFile)) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        for (File file1 : file.listFiles()) {
                            if (file1.getName().contains(PowerBI_Constanst.EXTENSION_PBIX) || file1.getName().contains(PowerBI_Constanst.EXTENSION_PBIT)) {
                                if (file1.delete()) {
                                    logger.info(file.getName() + " deleted successfully");
                                } else {
                                    logger.info(file.getName() + " not deleted  ");
                                }
                            }
                        }
                    }
                    if (file.getName().contains(".pbix") || file.getName().contains(".pbit")) {
                        if (file.delete()) {
                            logger.info(file.getName() + " deleted successfully");
                        } else {
                            logger.info(file.getName() + " not deleted  ");
                        }
                    }
                }
            } else if ("Archive Source File".equals(keeporDeleteOrArchiveSourceFile)) {
                for (File file : files) {
                    File directory = new File(archiveFolderPath + "/Archive");
                    if (file.isDirectory()) {
                        directory = new File(archiveFolderPath + "/Archive/" + file.getName());
                        for (File file1 : file.listFiles()) {
                            if (file1.getName().contains(".pbix") || file1.getName().contains(".pbit")) {
                                if (!directory.exists()) {
                                    directory.mkdir();
                                }
                                FileUtils.copyFile(file1, new File(archiveFolderPath + "/Archive/" + file.getName() + "/" + file1.getName()));

                                if (file1.delete()) {
                                    logger.info(file1.getName() + " deleted successfully");
                                } else {
                                    logger.info(file1.getName() + " not deleted  ");
                                }
                            }

                        }
                    } else if (file.getName().contains(".pbix") || file.getName().contains(".pbit")) {
                        if (!directory.exists()) {
                            directory.mkdir();
                        }
                        FileUtils.copyFile(file, new File(archiveFolderPath + "/Archive/" + file.getName()));
                        if (file.delete()) {
                            logger.info(file.getName() + " deleted successfully");
                        } else {
                            logger.info(file.getName() + " not deleted  ");
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
            StringWriter exception = new StringWriter();
            e.printStackTrace(new PrintWriter(exception));
            PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Error::Exception In PowerBIReportParser.buildZipPBIXFile() Method " + exception + "\n");
            exceptionLog.append("Exception In buildZipPBIXFile() \n" + exception.toString());
            exceptionLog.append("\n ================================");
        } finally {
            REPORT_datatypeAgainstColumnMap.clear();
            REPORT_expressionAgainstColumnMap.clear();
            REPORT_expressionAgainstMeasureMap.clear();
            powerBIMetadata.clear();
            defSchema = "";
            return sb.toString();
        }

    }

    public static void createFinalMapping(String zipFileName, String projectId, int subId, StringBuilder sb) {
        try {
            Mapping finalMap = new Mapping();
            int mapId = mappingManagerUtil.getMappingId(parentSubjectId, zipFileName, com.ads.api.beans.common.Node.NodeType.MM_SUBJECT);
            if (mapId > 0) {
                if (mappingReloadtype.equals("Versioning")) {
                    finalMap = creatingMapVersion(Integer.parseInt(projectId), zipFileName, parentSubjectId, mappingManagerUtil, new StringBuffer());
                    if (IS_SYNC_ON) {
                        finalMap.setUpdateSourceMetadata(true);
                        finalMap.setUpdateTargetMetadata(true);
                    }
                    sb.append(mappingManagerUtil.addMappingSpecifications(finalMap.getMappingId(), finalMappingSpecifications).getStatusMessage()).append("\n");
                } else {
                    if (getMappingVersions(parentSubjectId, zipFileName, mappingManagerUtil).size() > 1) {
                        finalMap = deleteMappingsOnFreshLoad(Integer.parseInt(projectId), parentSubjectId, zipFileName, mappingManagerUtil);
                        if (IS_SYNC_ON) {
                            finalMap.setUpdateSourceMetadata(true);
                            finalMap.setUpdateTargetMetadata(true);
                        }
                        sb.append(mappingManagerUtil.addMappingSpecifications(finalMap.getMappingId(), finalMappingSpecifications).getStatusMessage()).append("\n");
                    } else {
                        mappingManagerUtil.deleteMapping(mapId);
                        finalMap = new Mapping();
                        finalMap.setMappingName(zipFileName);
                        finalMap.setProjectId(Integer.parseInt(projectId));
                        finalMap.setSubjectId(parentSubjectId);
                        finalMap.setMappingSpecifications(finalMappingSpecifications);
                        if (IS_SYNC_ON) {
                            finalMap.setUpdateSourceMetadata(true);
                            finalMap.setUpdateTargetMetadata(true);
                        }
                        mappingManagerUtil.createMapping(finalMap);
                    }

                }
            } else {
                finalMap = new Mapping();
                finalMap.setMappingName(zipFileName);
                finalMap.setProjectId(Integer.parseInt(projectId));
                finalMap.setSubjectId(parentSubjectId);
                finalMap.setMappingSpecifications(finalMappingSpecifications);
                if (IS_SYNC_ON) {
                    finalMap.setUpdateSourceMetadata(true);
                    finalMap.setUpdateTargetMetadata(true);
                }
                mappingManagerUtil.createMapping(finalMap);
            }
        } catch (Exception ex) {
            StringWriter exception = new StringWriter();
            ex.printStackTrace(new PrintWriter(exception));
            PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Error::Exception In PowerBIReportParser.createFinalMapping() Method " + exception + "\n");
        }
    }

    public static void callSubdirectories(File directory) {

        try {
            File subDirectories[] = directory.listFiles();
            for (File eachsubDirectories : subDirectories) {
                if (eachsubDirectories.isDirectory()) {
                    callSubdirectories(eachsubDirectories);
                } else {
                    subDirectoryfilesList.add(eachsubDirectories.getAbsolutePath());

                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
            StringWriter exception = new StringWriter();
            e.printStackTrace(new PrintWriter(exception));
            PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Error::Exception In PowerBIReportParser.callSubdirectories() Method " + exception + "\n");
            exceptionLog.append("Exception In callSubdirectories() \n" + exception.toString());
            exceptionLog.append("\n ================================");
        }
    }

    public static Mapping creatingMapVersion(int projectId, String mappingName, int parentSubectId, MappingManagerUtil mappingManagerUtil, StringBuffer message) {
        Mapping latestMappingObj = null;

        try {
            latestMappingVersion = getMappingVersions(parentSubectId, mappingName, mappingManagerUtil);
            updateMappingVersion = latestMappingVersion.get(latestMappingVersion.size() - 1);
            Mapping mappingObj = mappingManagerUtil.getMapping(parentSubectId, com.ads.api.beans.common.Node.NodeType.MM_SUBJECT, mappingName, updateMappingVersion);
            int mappId = mappingObj.getMappingId();
            mappingObj.setProjectId(projectId);
            mappingObj.setSubjectId(parentSubectId);
            mappingObj.setMappingId(mappId);
            if (IS_SYNC_ON) {
                mappingObj.setUpdateSourceMetadata(true);
                mappingObj.setUpdateTargetMetadata(true);
            }
            mappingObj.setChangedDescription("Mapping " + mappingName + " changed! as Version Done: " + updateMappingVersion);
            PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Info::Initiated Versioning" + mappingName + "(" + updateMappingVersion + ")\n");
            PowerBI_LOGGER.overviewLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Info::Initiated Versioning" + mappingName + "(" + updateMappingVersion + ")\n");
            String status = mappingManagerUtil.versionMapping(mappingObj).getStatusMessage();
            PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Info::" + status + "for Page : " + mappingName + "\n");
            PowerBI_LOGGER.overviewLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Info::" + status + "for Page : " + mappingName + "\n");
            message.append(mappingName + " : " + status + "\n");
            logMessage.append(mappingName + " : " + status + "\n");
            latestMapVersion = getMappingVersions(parentSubectId, mappingName, mappingManagerUtil);
            latestMapV = latestMapVersion.get(latestMapVersion.size() - 1);
            latestMappingObj = mappingManagerUtil.getMapping(parentSubectId, com.ads.api.beans.common.Node.NodeType.MM_SUBJECT, mappingName, latestMapV);
            int latestMapId = latestMappingObj.getMappingId();
            mappingManagerUtil.deleteMappingSpecifications(latestMapId);
        } catch (Exception e) {
            logger.error(e.getMessage());
            StringWriter exception = new StringWriter();
            e.printStackTrace(new PrintWriter(exception));
            PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Error::Exception In PowerBIReportParser.creatingMapVersion() Method " + exception + "\n");
        }
        return latestMappingObj;
    }

    public static List<Float> getMappingVersions(int subjectId, String mapName, MappingManagerUtil mappingManagerUtil) {
        List<Float> mapVersionList = new ArrayList();
        try {
            ArrayList<Mapping> mappings = mappingManagerUtil.getMappings(subjectId, com.ads.api.beans.common.Node.NodeType.MM_SUBJECT);

            if (!mappings.isEmpty()) {
                for (int map = 0; map < mappings.size(); map++) {
                    String mappingName = mappings.get(map).getMappingName();
                    float mappingVersion = mappings.get(map).getMappingSpecVersion();
                    if (mapName.equalsIgnoreCase(mappingName)) {
                        mapVersionList.add(mappingVersion);
                    }
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
            StringWriter exception = new StringWriter();
            e.printStackTrace(new PrintWriter(exception));
            PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Error::Exception In PowerBIReportParser.getMappingVersions() Method " + exception + "\n");
            exceptionLog.append("Exception In getMappingVersions() \n" + exception.toString());
            exceptionLog.append("\n ================================");
        }
        return mapVersionList;
    }

    public static Mapping deleteMappingsOnFreshLoad(int projectID, int subjectId, String mapName, MappingManagerUtil mappingManagerUtil) {
        latestMapVersion = getMappingVersions(subjectId, mapName, mappingManagerUtil);
        latestMapV = latestMapVersion.get(latestMapVersion.size() - 1);
        mappingManagerUtil.deleteMapping(subjectId, com.ads.api.beans.common.Node.NodeType.MM_SUBJECT, mapName, APIConstants.VersionMode.SPECIFIC_VERSION, latestMapV);
        return creatingMapVersion(projectID, mapName, subjectId, mappingManagerUtil, new StringBuffer());

    }

    public static String repairQuery(String query) {
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

    public static HashMap setMetadataMap(String mappingJSON, String databaseName, String serverName, String metadataJSONPath, String defSystemName, String defEnvironmentName, Map allDBMap, Map allTablesMap, Map metadatChacheHM, String defSchema, SystemManagerUtil systemManagerUtil) {
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

    public static HashMap setMetadataMap1(String mappingJSON, String databaseName, String serverName, String metadataJSONPath, String defSystemName, String defEnvironmentName, Map allDBMap, Map allTablesMap, Map metadatChacheHM, String defSchema, SystemManagerUtil systemManagerUtil, String tableName) {
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
        inputsMap.put("tableName", tableName);
        return inputsMap;
    }

    /**
     * This method sync the metadata with the mapping specification which we get
     * after parsing the query and powerBi informations.
     *
     * @param objectName
     * @param powerBISrcCol
     * @param queryFromXmlAgainstObjectMap
     * @param mappingSpecificationRows
     * @param targetSystemName
     * @param targetEnvironmentName
     * @param datatypes
     * @param yamlDatatypes
     * @return
     */
    public static boolean integrateQueryAndPowerBIMappingSpecifications(String objectName, String powerBISrcCol, Map<String, String> queryFromXmlAgainstObjectMap, ArrayList<MappingSpecificationRow> mappingSpecificationRows, String targetSystemName, String targetEnvironmentName, String datatypes, String query_key) {

        String returnStatement = "ERDG#ERWIN#0#ERWIN#0#ERWIN#0";
        boolean check = false;
        if (columnPropertiesAgainstTargetTable.get(objectName) != null) {
            LinkedHashMap<String, LinkedHashMap<String, String>> columnPropertiesAgainstColumnName_Queries = columnPropertiesAgainstTargetTable.get(objectName);
            LinkedHashMap<String, String> columnPropertiesAgainstColumnName = columnPropertiesAgainstColumnName_Queries.get(query_key);
            if (columnPropertiesAgainstColumnName.get(powerBISrcCol.toUpperCase().replace(" ", "")) != null) {
                check = true;
                String sourceColumnInformations = columnPropertiesAgainstColumnName.get(powerBISrcCol.toUpperCase().replace(" ", ""));
                if (sourceColumnInformations != null) {
                    MappingSpecificationRow mapSpecRow = new MappingSpecificationRow();
                    mapSpecRow.setSourceTableName(sourceColumnInformations.split("#ERWIN#")[2]);
                    mapSpecRow.setSourceColumnName(powerBISrcCol.toUpperCase().replace(" ", ""));
                    if (sourceColumnInformations.split("#ERWIN#")[0].equals("EMPTY_SYSTEM")) {
                        mapSpecRow.setSourceSystemName(pbib.getSQL_SYSTEM_NAME());
                        mapSpecRow.setTargetSystemName(pbib.getSQL_SYSTEM_NAME());
                    } else {
                        mapSpecRow.setSourceSystemName(sourceColumnInformations.split("#ERWIN#")[0]);
                        mapSpecRow.setTargetSystemName(sourceColumnInformations.split("#ERWIN#")[0]);
                    }
                    if (sourceColumnInformations.split("#ERWIN#")[1].equals("EMPTY_ENVIRONMENT")) {
                        mapSpecRow.setSourceSystemEnvironmentName(pbib.getSQL_ENVIRONMENT_NAME());
                    } else {
                        mapSpecRow.setSourceSystemEnvironmentName(sourceColumnInformations.split("#ERWIN#")[1]);
                    }
                    mapSpecRow.setTargetSystemName(targetSystemName);
                    mapSpecRow.setTargetSystemEnvironmentName(targetEnvironmentName);
                    mapSpecRow.setTargetTableName(objectName);
                    mapSpecRow.setTargetColumnName(powerBISrcCol);
                }

            } else if (columnPropertiesAgainstColumnName.get(powerBISrcCol.toUpperCase().replace(" ", "_")) != null) {
                check = true;
                String sourceColumnInformations = columnPropertiesAgainstColumnName.get(powerBISrcCol.toUpperCase().replace(" ", "_"));
                if (sourceColumnInformations != null) {
                    MappingSpecificationRow mapSpecRow = new MappingSpecificationRow();
                    mapSpecRow.setSourceTableName(sourceColumnInformations.split("#ERWIN#")[2]);
                    mapSpecRow.setSourceColumnName(powerBISrcCol.toUpperCase().replace(" ", "_"));
                    if (sourceColumnInformations.split("#ERWIN#")[0].equals("EMPTY_SYSTEM")) {
                        mapSpecRow.setSourceSystemName(pbib.getSQL_SYSTEM_NAME());
                        mapSpecRow.setTargetSystemName(pbib.getSQL_SYSTEM_NAME());
                    } else {
                        mapSpecRow.setSourceSystemName(sourceColumnInformations.split("#ERWIN#")[0]);
                        mapSpecRow.setTargetSystemName(sourceColumnInformations.split("#ERWIN#")[0]);
                    }
                    if (sourceColumnInformations.split("#ERWIN#")[1].equals("EMPTY_ENVIRONMENT")) {
                        mapSpecRow.setSourceSystemEnvironmentName(pbib.getSQL_ENVIRONMENT_NAME());
                    } else {
                        mapSpecRow.setSourceSystemEnvironmentName(sourceColumnInformations.split("#ERWIN#")[1]);
                    }
                    mapSpecRow.setTargetSystemName(targetSystemName);
                    mapSpecRow.setTargetSystemEnvironmentName(targetEnvironmentName);
                    mapSpecRow.setTargetTableName(objectName);
                    mapSpecRow.setTargetColumnName(powerBISrcCol);
                    mappingSpecificationRows.add(mapSpecRow);
                }
            }
            if (!check) {
                if (columnPropertiesAgainstColumnName.get(powerBISrcCol.toUpperCase().trim()) != null) {
                    check = true;
                    String sourceColumnInformations = columnPropertiesAgainstColumnName.get(powerBISrcCol.toUpperCase().replace(" ", ""));
                    if (sourceColumnInformations != null) {
                        MappingSpecificationRow mapSpecRow = new MappingSpecificationRow();
                        mapSpecRow.setSourceTableName(sourceColumnInformations.split("#ERWIN#")[2]);
                        mapSpecRow.setSourceColumnName(powerBISrcCol.toUpperCase().replace(" ", ""));
                        if (sourceColumnInformations.split("#ERWIN#")[0].equals("EMPTY_SYSTEM")) {
                            mapSpecRow.setSourceSystemName(pbib.getSQL_SYSTEM_NAME());
                            mapSpecRow.setTargetSystemName(pbib.getSQL_SYSTEM_NAME());
                        } else {
                            mapSpecRow.setSourceSystemName(sourceColumnInformations.split("#ERWIN#")[0]);
                            mapSpecRow.setTargetSystemName(sourceColumnInformations.split("#ERWIN#")[0]);
                        }
                        if (sourceColumnInformations.split("#ERWIN#")[1].equals("EMPTY_ENVIRONMENT")) {
                            mapSpecRow.setSourceSystemEnvironmentName(pbib.getSQL_ENVIRONMENT_NAME());
                        } else {
                            mapSpecRow.setSourceSystemEnvironmentName(sourceColumnInformations.split("#ERWIN#")[1]);
                        }
                        mapSpecRow.setTargetSystemName(targetSystemName);
                        mapSpecRow.setTargetSystemEnvironmentName(targetEnvironmentName);
                        mapSpecRow.setTargetTableName(objectName);
                        mapSpecRow.setTargetColumnName(powerBISrcCol);
                    }

                }
            }
        }
        return check;

    }

    /**
     * This method sync the metadata with the mapping specification which we get
     * after parsing the query
     *
     * @param mapSpecs
     * @param envMap
     * @param ObjectName
     * @param smutil
     */
    public static void updateQueryMapSpec(ArrayList<MappingSpecificationRow> mapSpecs,
            LinkedHashMap<String, String> envMap, String ObjectName, SystemManagerUtil smutil, String keyName, Map<String, String> dbTypes, String sourceSystemName1, String sourceEnvName1) {
        ArrayList<MappingSpecificationRow> extraSpecs = new ArrayList<>();
        LinkedHashMap<String, String> columnPropertiesAgainstColumnName = new LinkedHashMap();
        LinkedHashMap<String, LinkedHashMap<String, String>> columnPropertiesWithKeyName = new LinkedHashMap();
        try {
            for (int i = 0; i < mapSpecs.size(); i++) {
                String srcTbl = "";
                String tgtTbl = "";
                String sourceSystemName = "";
                String sourceEnvironmentName = "";
                String srcCol = " ";
                String tgtCol = " ";
                String srcCol1 = " ";
                String colDatatype = "ERDG";
                int colLength = -1;
                int colPrec = -1;
                int colScale = -1;
                MappingSpecificationRow eachSpec = mapSpecs.get(i);
                if (extreamResultSet.contains(eachSpec.getTargetTableName())) {
//                if (eachSpec.getTargetTableName().contains("RS_")) {
                    MappingSpecificationRow spec = new MappingSpecificationRow();
                    srcTbl = eachSpec.getSourceTableName();
                    srcCol = eachSpec.getSourceColumnName();
                    tgtTbl = ObjectName;
                    String serverName = dbTypes.get(ObjectName.replaceAll("[^a-zA-Z0-9_\\.\\s\\-]", "")).split("\\*ERWIN\\*")[1].replaceAll("[^a-zA-Z0-9_\\s]", "_");
                    String databaseName = dbTypes.get(ObjectName.replaceAll("[^a-zA-Z0-9_\\.\\s\\-]", "")).split("\\*ERWIN\\*")[2].replaceAll("[^a-zA-Z0-9_\\s]", "_");
//                    String sourceSysEnvInfo1 = SyncMetadataJsonFileDesign.newmetasync(tgtTbl, "", "", metadataJsonPath, sourceSystemName1, sourceEnvName1, cacheMap, allTablesMap, allDBMap, "", "", "");
//                    String sourceSystemName_temp = sourceSysEnvInfo1.split("##")[0];
//                    String sourceEnvironmentName_temp = sourceSysEnvInfo1.split("##")[1];
                    tgtCol = eachSpec.getTargetColumnName();
                    sourceSystemName = eachSpec.getSourceSystemName();
                    sourceEnvironmentName = eachSpec.getSourceSystemEnvironmentName();
                    String tgtSys = "";
                    String tgtEnv = "";
                    spec.setSourceSystemName(eachSpec.getTargetSystemName());
                    spec.setSourceSystemEnvironmentName(eachSpec.getTargetSystemEnvironmentName());
                    spec.setSourceTableName(eachSpec.getTargetTableName());
                    spec.setSourceColumnName(eachSpec.getTargetColumnName());
                    spec.setTargetSystemName(sourceSystemName1);
                    spec.setTargetSystemEnvironmentName(sourceEnvName1);
                    spec.setTargetTableName(ObjectName);
                    spec.setTargetColumnName(eachSpec.getTargetColumnName());
                    extraSpecs.add(spec);
                    if (eachSpec.getTargetSystemName() != null || eachSpec.getTargetSystemName() != "") {
                        tgtSys = eachSpec.getTargetSystemName();
                    } else {
                        tgtSys = "EMPTY_SYSTEM";
                    }
                    if (eachSpec.getTargetSystemEnvironmentName() != null || eachSpec.getTargetSystemEnvironmentName() != "") {
                        tgtEnv = eachSpec.getTargetSystemEnvironmentName();
                    } else {
                        tgtEnv = "EMPTY_ENVIRONMENT";
                    }
                    String finalValue = colDatatype + "#ERWIN#" + colLength + "#ERWIN#" + colPrec + "#ERWIN#" + colScale;
                    columnPropertiesAgainstColumnName.put(tgtCol.toUpperCase(), eachSpec.getTargetSystemName() + "#ERWIN#" + eachSpec.getTargetSystemEnvironmentName() + "#ERWIN#" + tgtTbl + "#ERWIN#" + finalValue);

                } else if (eachSpec.getTargetTableName().contains("#@NIWRE@#")) {
                    srcTbl = eachSpec.getSourceTableName();
                    srcCol = eachSpec.getSourceColumnName();
                    tgtTbl = ObjectName;
                    String serverName = dbTypes.get(ObjectName.replaceAll("[^a-zA-Z0-9_\\.\\s\\-]", "")).split("\\*ERWIN\\*")[1].replaceAll("[^a-zA-Z0-9_\\s]", "_");
                    String databaseName = dbTypes.get(ObjectName.replaceAll("[^a-zA-Z0-9_\\.\\s\\-]", "")).split("\\*ERWIN\\*")[2].replaceAll("[^a-zA-Z0-9_\\s]", "_");
//                    String sourceSysEnvInfo1 = SyncMetadataJsonFileDesign.newmetasync(tgtTbl, "", "", metadataJsonPath, sourceSystemName1, sourceEnvName1, cacheMap, allTablesMap, allDBMap, "", "", "");
//                    String sourceSystemName_temp = sourceSysEnvInfo1.split("##")[0];
//                    String sourceEnvironmentName_temp = sourceSysEnvInfo1.split("##")[1];
//                    eachSpec.setTargetTableName(sourceSysEnvInfo1.split("##")[2]);
//                    eachSpec.setTargetSystemName(sourceSystemName_temp);
//                    eachSpec.setTargetSystemEnvironmentName(sourceEnvironmentName_temp);
                    tgtCol = eachSpec.getTargetColumnName();
                    sourceSystemName = eachSpec.getSourceSystemName();
                    sourceEnvironmentName = eachSpec.getSourceSystemEnvironmentName();
                    String tgtSys = "";
                    String tgtEnv = "";
                    if (eachSpec.getTargetSystemName() != null || eachSpec.getTargetSystemName() != "") {
                        tgtSys = eachSpec.getTargetSystemName();
                    } else {
                        tgtSys = "EMPTY_SYSTEM";
                    }
                    if (eachSpec.getTargetSystemEnvironmentName() != null || eachSpec.getTargetSystemEnvironmentName() != "") {
                        tgtEnv = eachSpec.getTargetSystemEnvironmentName();
                    } else {
                        tgtEnv = "EMPTY_ENVIRONMENT";
                    }
                    String finalValue = colDatatype + "#ERWIN#" + colLength + "#ERWIN#" + colPrec + "#ERWIN#" + colScale;
                    columnPropertiesAgainstColumnName.put(tgtCol.toUpperCase(), eachSpec.getTargetSystemName() + "#ERWIN#" + eachSpec.getTargetSystemEnvironmentName() + "#ERWIN#" + tgtTbl + "#ERWIN#" + finalValue);
                }

            }
            columnPropertiesWithKeyName.put(keyName, columnPropertiesAgainstColumnName);
            if (!columnPropertiesWithKeyName.isEmpty()) {
                if (columnPropertiesAgainstTargetTable.get(ObjectName) != null) {
                    LinkedHashMap<String, LinkedHashMap<String, String>> columnPropertiesAgainstColumnName1 = columnPropertiesAgainstTargetTable.get(ObjectName);
                    columnPropertiesAgainstColumnName1.putAll(columnPropertiesWithKeyName);
                    columnPropertiesAgainstTargetTable.put(ObjectName, columnPropertiesAgainstColumnName1);
                } else {
                    columnPropertiesAgainstTargetTable.put(ObjectName, columnPropertiesWithKeyName);
                }
            }

        } catch (Exception e) {
            logger.error(e.getMessage());
            StringWriter exception = new StringWriter();
            e.printStackTrace(new PrintWriter(exception));
            PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Error::Exception In PowerBIReportParser.updateQueryMapSpec() Method " + exception + "\n");
        }

        mapSpecs.addAll(extraSpecs);

    }

    /**
     * This method return the System and Environment name of the specific table
     * and column.
     *
     * @param envMap
     * @param srcTbl
     * @return String
     */
    public static String getPowerbiMetadataInfo(LinkedHashMap<String, String> envMap, String srcTbl) {
        String sourceSystemName = pbib.getSQL_SYSTEM_NAME();
        String sourceEnvironmentName = pbib.getSQL_ENVIRONMENT_NAME();
        if (envMap.get(srcTbl.toUpperCase()) != null) {
            String sysEnvAndSchema = envMap.get(srcTbl.toUpperCase());
            if (sysEnvAndSchema.split("#").length == 3) {
                sourceSystemName = sysEnvAndSchema.split("#")[1];
                sourceEnvironmentName = sysEnvAndSchema.split("#")[0];
//                                                int tabId = smutil.getTableId(sourceSystemName, sourceEnvironmentName, srcTbl);
                int tabId = Integer.parseInt(sysEnvAndSchema.split("#")[2]);
                if (tabId > 0) {
                    return sourceSystemName + "#ERWIN#" + sourceEnvironmentName;
                }
            } else if (sysEnvAndSchema.split("#").length == 4) {
                sourceSystemName = sysEnvAndSchema.split("#")[1];
                sourceEnvironmentName = sysEnvAndSchema.split("#")[0];
//                                                int tabId = smutil.getTableId(sourceSystemName, sourceEnvironmentName, srcTbl);
                int tabId = Integer.parseInt(sysEnvAndSchema.split("#")[2]);
                if (tabId > 0) {
                    return sourceSystemName + "#ERWIN#" + sourceEnvironmentName;
                }
            }

        }
        return sourceSystemName + "#ERWIN#" + sourceEnvironmentName;
    }

    /**
     * Return the PowerBI datatype of the specific column.
     *
     * @param tableName
     * @param columnName
     * @param actualColumn
     * @param dateCheck
     * @param datatypes
     * @return
     */
    public static String powerbiDatatypeInfo(String tableName, String columnName, String actualColumn, boolean dateCheck, Map datatypes) {
        String format = "";
        String type = "";
        String datatype = "";
        if (powerBIMetadata.containsKey(tableName + "." + actualColumn)) {
            type = powerBIMetadata.get(tableName + "." + actualColumn).split("!Erwin!")[0];
            try {
                format = powerBIMetadata.get(tableName + "." + actualColumn).split("!Erwin!")[1];
            } catch (Exception e) {
                format = "";
            }
            if ((String) datatypes.get(type) != null) {
                if (type.equals("1")) {
                    if (format.equals("") || format.equals("0.0%;-0.0%;0.0%")) {
                        datatype = ((String) datatypes.get(type)).split("!Erwin!")[1];
                    } else {
                        datatype = ((String) datatypes.get(type)).split("!Erwin!")[0];
                    }
                } else if (type.equals("4") || dateCheck) {
                    if (format.equals("") || dateCheck) {
                        datatype = "Date#4#0#0";
                    } else if (format.equals("hh:mm:ss tt")) {
                        datatype = ((String) datatypes.get(type)).split("!Erwin!")[1];
                    } else {
                        datatype = ((String) datatypes.get(type)).split("!Erwin!")[2];
                    }
                } else {
                    datatype = (String) datatypes.get(type);
                }

            } else {
                datatype = type;
            }
        }
        if (powerBIMetadata.containsKey(tableName + "." + columnName) && !datatype.equals("")) {
//                                            srcCol=srcCol1;
            type = powerBIMetadata.get(tableName + "." + columnName).split("!Erwin!")[0];
            try {
                format = powerBIMetadata.get(tableName + "." + columnName).split("!Erwin!")[1];
            } catch (Exception e) {
                format = "";
            }
            if ((String) datatypes.get(type) != null) {
                if (type.equals("1")) {
                    if (format.equals("") || format.equals("0.0%;-0.0%;0.0%")) {
                        datatype = ((String) datatypes.get(type)).split("!Erwin!")[1];
                    } else {
                        datatype = ((String) datatypes.get(type)).split("!Erwin!")[0];
                    }
                } else if (type.equals("4") || dateCheck) {
                    if (format.equals("") || dateCheck) {
                        datatype = "Date#4#0#0";
                    } else if (format.equals("hh:mm:ss tt")) {
                        datatype = ((String) datatypes.get(type)).split("!Erwin!")[1];
                    } else {
                        datatype = ((String) datatypes.get(type)).split("!Erwin!")[2];
                    }
                } else {
                    datatype = (String) datatypes.get(type);
                }

            } else {
                datatype = type;
            }
        } else {
            datatype = "";
        }

        return datatype;
    }

    /**
     * This method parses the Layout JSON which we get after unzipping the pbix
     * file
     *
     * @param smutil
     * @param layoutFilePath
     * @param bruleMap
     * @param pbixName
     * @param projectId
     * @param kvUtil
     * @param sourceSystemName
     * @param sourceEnvironmentName
     * @param targetSystemName
     * @param targetEnvironmentName
     * @param queryFromXmlAgainstObjectMap
     * @param datatypes
     * @param connectionFile
     * @param connectionSystemName
     * @param envMap
     * @param ymlDatatypes
     * @return String
     */
    public static String getLayoutJsonContent(SystemManagerUtil smutil, File layoutFilePath, String pbixName, KeyValueUtil kvUtil, Map<String, String> queryFromXmlAgainstObjectMap, LinkedHashMap<String, String> envMap, String[] sysEnvDetail, Map<String, String> optionsProperties) {
        PowerBIConfigPropertiesBuilder builder = new PowerBIConfigPropertiesBuilder();
        PowerBIQueryPropertiesBuilder builder1 = new PowerBIQueryPropertiesBuilder();
        PowerBIFiltersPropertiesBuilder builder2 = new PowerBIFiltersPropertiesBuilder();
        PowerBIDataTransformsBuilder builder3 = new PowerBIDataTransformsBuilder();
        String line;
        String configJson;
        String filtersJson;
        String queryJson;
        String displayName;
        String dataTransformsJson;
        JSONObject configJsonObj;
        JSONArray filtersJsonArr;
        JSONObject queryJsonObj;
        StringBuilder statusMessageSb = new StringBuilder();
        JSONObject dataTransformsJsonObj;
        List<Map<String, Map<String, Set<String>>>> eachVisualContainserList = new ArrayList();
        Map<String, Map<String, Set<String>>> visualContainersMap = new LinkedHashMap();
        Map<String, String> mQueryMap = new LinkedHashMap();
        PowerBIConfigPropertiesBuilder.mapOfActualTableAgainstQuery.clear();
        int visibilityVal = 0;
        try {
            line = FileUtils.readFileToString(layoutFilePath, "UTF-8");
            line = line.replaceAll("\\x00", "").replace("\\\"value\\\":\\\"\" \\\"", "\\\"value\\\":\\\"\\\"").replace("\\\"value\\\":\\\"\\\\t\" \\\"", "\\\"value\\\":\\\"\\\"").replace(" d\" ", "").replace("\\\"value\\\":\\\"\" \\\\t\\\",", "\\\"value\\\":\\\"\\\"").replace("\u0013", "").replace("\u0018", "").replace("\u0019", "").replace("\u0019", "").replace("\"\"", "\"");
            JSONObject jsonObj = new JSONObject(line);
            JSONObject colJsonObject = null;
            JSONObject jsonObject;
            JSONArray visualContainersArr;
            JSONArray sectiosJsonArr = (JSONArray) jsonObj.get("sections");
            renamedCol.clear();
            PowerBIConfigPropertiesBuilder.getPowerBIRenamedColumn(sectiosJsonArr, renamedCol);
            Map<String, Set<String>> reportDetailsMap;
            for (int i = 0; i < sectiosJsonArr.length(); i++) {
                jsonObject = (JSONObject) sectiosJsonArr.get(i);
                displayName = jsonObject.getString("displayName");
                logMessage.append("MAP : " + displayName + "\n");
                PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Info::Getting configurtion for Page : " + displayName + "\n");
                PowerBI_LOGGER.overviewLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Info::Getting configurtion for Page : " + displayName + "\n");
                actualTableAgainstEntity.clear();
                Map<String, Set<String>> filterReportDetailsMap = new HashMap();
                if (jsonObject.has("config")) {
                    JSONObject configObject = new JSONObject(jsonObject.get("config").toString());
                    if (configObject.has("visibility")) {
                        visibilityVal = configObject.getInt("visibility");
                        if (visibilityVal == 1 && !pbib.isCreateHiddenPage()) {
                            continue;
                        }
                    }
                }
                if (jsonObject.has("filters")) {
                    filtersJson = jsonObject.get("filters").toString();
                    filtersJsonArr = new JSONArray(filtersJson);
                    for (int filter_i = 0; filter_i < filtersJsonArr.length(); filter_i++) {
                        reportDetailsMap = PowerBIFiltersPropertiesBuilder.getPowerBIReportFiltersInfo(filtersJsonArr.getJSONObject(filter_i)); // DONE
                        if (!reportDetailsMap.isEmpty()) {
                            for (Map.Entry<String, Set<String>> entryreportDetails : reportDetailsMap.entrySet()) {
                                String reportKey = entryreportDetails.getKey();
                                if (filterReportDetailsMap.containsKey(reportKey)) {
                                    Set<String> setValues = filterReportDetailsMap.get(reportKey);
                                    setValues.addAll(entryreportDetails.getValue());
                                } else {
                                    filterReportDetailsMap.put(reportKey, entryreportDetails.getValue());
                                }
                            }
                        }
                    }
                    if (!filterReportDetailsMap.isEmpty()) {
                        visualContainersMap.put(pbixName + "_" + "0" + "_filters_", filterReportDetailsMap);
                    }
                }
                PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Info::Getting visualcontainers" + "\n");
                PowerBI_LOGGER.overviewLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Info::Getting visualcontainers" + "\n");
                visualContainersArr = jsonObject.getJSONArray("visualContainers");
                for (int col_i = 0; col_i < visualContainersArr.length(); col_i++) {
                    colJsonObject = (JSONObject) visualContainersArr.get(col_i);
                    if (colJsonObject.has("config")) {
                        configJson = colJsonObject.get("config").toString();
                        configJsonObj = new JSONObject(configJson);
                        Map<String, Map<String, Set<String>>> configMap = PowerBIConfigPropertiesBuilder.getPowerBIReportConfigInfo(configJsonObj, pbixName, col_i, displayName);
                        if (!configMap.isEmpty()) {
                            eachVisualContainserList.add(configMap);
                        }
                    }
                    if (colJsonObject.has("query")) {
                        queryJson = colJsonObject.get("query").toString();
                        queryJsonObj = new JSONObject(queryJson);
                        JSONArray commandsArr = queryJsonObj.getJSONArray("Commands");
                        for (int command_i = 0; command_i < commandsArr.length(); command_i++) {
                            reportDetailsMap = PowerBIQueryPropertiesBuilder.getPowerBIReportQueryInfo(commandsArr.getJSONObject(command_i), displayName, pbixName);// top level done
                            if (!reportDetailsMap.isEmpty()) {
                                visualContainersMap.put(pbixName + "_" + col_i + "_query_", reportDetailsMap);
                            }
                            reportDetailsMap = null;
                        }
                    }
                    if (colJsonObject.has("dataTransforms")) {
                        dataTransformsJson = colJsonObject.get("dataTransforms").toString();
                        dataTransformsJsonObj = new JSONObject(dataTransformsJson);
                        reportDetailsMap = PowerBIDataTransformsBuilder.getPowerBIReportDataTransformsInfo(dataTransformsJsonObj, powerBIMetadata, displayName, pbixName);
                        if (reportDetailsMap != null) {
                            visualContainersMap.put(pbixName + "_" + col_i + "_dataTransforms_", reportDetailsMap);
                        }
                        reportDetailsMap = null;
                    }
                    if (colJsonObject.has("filters")) {
                        filtersJson = colJsonObject.get("filters").toString();
                        filtersJsonArr = new JSONArray(filtersJson);
                        for (int filter_i = 0; filter_i < filtersJsonArr.length(); filter_i++) {

                            reportDetailsMap = PowerBIFiltersPropertiesBuilder.getPowerBIReportFiltersInfo(filtersJsonArr.getJSONObject(filter_i)); // DONE
                            if (!reportDetailsMap.isEmpty()) {
                                if (visualContainersMap.containsKey(pbixName + "_" + col_i + "_filters_")) {
                                    Map<String, Set<String>> valueofVisualContaier = visualContainersMap.get(pbixName + "_" + col_i + "_filters_");
                                    for (Map.Entry<String, Set<String>> eachreportValue : reportDetailsMap.entrySet()) {
                                        String reportKey = eachreportValue.getKey();
                                        if (valueofVisualContaier.containsKey(reportKey)) {
                                            Set<String> valuesOfVisualContainer = valueofVisualContaier.get(reportKey);
                                            valuesOfVisualContainer.addAll(reportDetailsMap.get(reportKey));
                                        } else {
                                            valueofVisualContaier.put(reportKey, eachreportValue.getValue());
                                        }

                                    }
                                    visualContainersMap.put(pbixName + "_" + col_i + "_filters_", valueofVisualContaier);

                                } else {
                                    visualContainersMap.put(pbixName + "_" + col_i + "_filters_", reportDetailsMap);
                                }
                            }
                        }
                    }
                }
                PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Info::Creating Mappings for Page : " + displayName + "\n");
                PowerBI_LOGGER.overviewLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Info::Creating Mappings for Page : " + displayName + "\n");
                statusMessageSb.append(createMappingWithLayoutJsonInAMM(smutil, eachVisualContainserList, visualContainersMap, pbixName, kvUtil, displayName, queryFromXmlAgainstObjectMap, envMap, optionsProperties)).append("\n");
                PowerBIConfigPropertiesBuilder.mapOfActualTableAgainstQuery.clear();
                visualContainersMap = new LinkedHashMap();
                eachVisualContainserList = new ArrayList();
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
            StringWriter exception = new StringWriter();
            e.printStackTrace(new PrintWriter(exception));
            PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Error::Exception In PowerBIReportParser.getLayoutJsonContent() Method " + exception + "\n");
        }
        return statusMessageSb.toString();
    }

    /**
     * This method helps to unzip a zip file
     *
     * @param zipFilePath
     * @param destDir
     */
    public static void unzip(String zipFilePath, String destDir) {
        FileInputStream fis;
        byte[] buffer = new byte[1024];
        try {
            fis = new FileInputStream(zipFilePath);
            try ( ZipInputStream zis = new ZipInputStream(fis)) {
                ZipEntry ze = zis.getNextEntry();
                while (ze != null) {
                    String fileName = ze.getName();
                    File newFile = new File(destDir + File.separator + fileName);
                    new File(newFile.getParent()).mkdirs();
                    try ( FileOutputStream fos = new FileOutputStream(newFile)) {
                        int len;
                        while ((len = zis.read(buffer)) > 0) {
                            fos.write(buffer, 0, len);
                        }
                    }
                    zis.closeEntry();
                    ze = zis.getNextEntry();
                }
                zis.closeEntry();
            }
            fis.close();
        } catch (IOException e) {
            logger.error(e.getMessage());
            StringWriter exception = new StringWriter();
            e.printStackTrace(new PrintWriter(exception));
            PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Error::Exception In PowerBIReportParser.unzip() Method " + exception + "\n");
        }
    }

    public static ArrayList<MappingSpecificationRow> getExtreamSpecs(HashMap<String, Set<String>> targetSet, String sourceTable, String targetTable) {
        ArrayList<MappingSpecificationRow> specs = new ArrayList<>();
        if (targetSet == null) {
            return specs;
        }
        for (Map.Entry<String, Set<String>> entry : targetSet.entrySet()) {
            String key = entry.getKey();
            Set<String> value = entry.getValue();
            if (value == null) {
                continue;
            }
            for (Iterator<String> iterator = value.iterator(); iterator.hasNext();) {
                String columnName = iterator.next();
                MappingSpecificationRow mapspec = new MappingSpecificationRow();
                String sourceSystemName = pbib.getSQL_SYSTEM_NAME();
                String sourceSystemEnvironmentName = pbib.getSQL_ENVIRONMENT_NAME();
                String targetSystemName = pbib.getSQL_SYSTEM_NAME();
                String targetSystemEnvironmentName = pbib.getSQL_ENVIRONMENT_NAME();
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

    /**
     * This method create mapping for the PBIX file which don't have the
     * connection information.
     *
     * @param smutil
     * @param eachVisualContainserList
     * @param visualContainersMap
     * @param pbixName
     * @param projectId
     * @param kvUtil
     * @param sourceSystemName1
     * @param sourceEnvironmentName1
     * @param targetSystemName
     * @param targetEnvironmentName
     * @param displayName
     * @param queryFromXmlAgainstObjectMap
     * @param envMap
     * @param colJsonObject
     * @param datatypes
     * @param ymlDatatypes
     * @return
     */
    public static String createMappingWithLayoutJsonInAMM(SystemManagerUtil smutil, List<Map<String, Map<String, Set<String>>>> eachVisualContainserList, Map<String, Map<String, Set<String>>> visualContainersMap, String pbixName, KeyValueUtil kvUtil,
            String displayName, Map<String, String> queryFromXmlAgainstObjectMap,
            LinkedHashMap<String, String> envMap, Map<String, String> optionsProperties) {

        Set<String> mapSecdAddStatus = new HashSet<>();
        Mapping map = null;
        String srcTbl = "";
        String srcCol = "";
        StringBuffer status = new StringBuffer();
        MappingSpecificationRow mapSpec = null;
        HashMap<String, String> extendedProperties = new HashMap();
        ArrayList<MappingSpecificationRow> mappingSpecifications = null;
        Set<String> mQuery = null;
        StringBuilder mQuerySb = new StringBuilder();
        String sourceSystemName1 = pbib.getSQL_SYSTEM_NAME();
        String sourceEnvironmentName1 = pbib.getSQL_ENVIRONMENT_NAME();
        String sourceSystemName = new String(sourceSystemName1);
        String sourceEnvironmentName = new String(sourceEnvironmentName1);
        String tempCol_temp = "";
        Set<String> orderBy = new HashSet<>();
        Set<String> filterValue = new HashSet<>();
        String displayName1 = displayName;
        int join_counter = 1;
        int where_counter = 1;
        int orderby_counter = 1;
        int group_count = 1;
        Set<String> extendedStatusCheck = new HashSet<>();
        Set<String> extractedQueries = new HashSet<>();
        Document document = new Document();
        document.setDocumentOwner("ADMINISTRATOR");
        try {

            if (!StringUtils.isAlphanumericSpace(displayName)) {
                displayName = displayName.replaceAll("[^a-zA-Z0-9_\\s]", "").trim();
            }

            int subId = mappingManagerUtil.getSubjectId(parentSubjectId, com.ads.api.beans.common.Node.NodeType.MM_SUBJECT, subjectName);
            if (subId <= 0) {
                Subject subject = new Subject();
                subject.setProjectId(Integer.parseInt(optionsProperties.get(PowerBI_Constanst.OPTION_KEY_PROJECT_ID)));
                subject.setParentSubjectId(parentSubjectId);
                subject.setSubjectName(subjectName);
                mappingManagerUtil.createSubject(subject);
                subId = mappingManagerUtil.getSubjectId(parentSubjectId, com.ads.api.beans.common.Node.NodeType.MM_SUBJECT, subjectName);
            }

            int mapId = mappingManagerUtil.getMappingId(subId, displayName, com.ads.api.beans.common.Node.NodeType.MM_SUBJECT);
            if (mapId > 0) {
                if (mappingReloadtype.equals("Versioning")) {
                    map = creatingMapVersion(Integer.parseInt(optionsProperties.get(PowerBI_Constanst.OPTION_KEY_PROJECT_ID)), displayName, subId, mappingManagerUtil, status);
                } else {
                    if (getMappingVersions(subId, displayName, mappingManagerUtil).size() != 1) {
                        map = deleteMappingsOnFreshLoad(Integer.parseInt(optionsProperties.get(PowerBI_Constanst.OPTION_KEY_PROJECT_ID)), subId, displayName, mappingManagerUtil);
                    } else {
                        mappingManagerUtil.deleteMapping(mapId);
                        map = new Mapping();
                        map.setMappingName(displayName);
                        map.setProjectId(Integer.parseInt(optionsProperties.get(PowerBI_Constanst.OPTION_KEY_PROJECT_ID)));
                        map.setSubjectId(subId);

                    }
                }
            } else {
                map = new Mapping();
                map.setMappingName(displayName);
                map.setProjectId(Integer.parseInt(optionsProperties.get(PowerBI_Constanst.OPTION_KEY_PROJECT_ID)));
                map.setSubjectId(subId);
            }
            mappingSpecifications = new ArrayList();
            if (IS_SYNC_ON) {
                map.setUpdateSourceMetadata(true);
                map.setUpdateTargetMetadata(true);
            }
            int pbixContainer_i = 0;
            String containerSpecificTransforms = "";
            String containerSpecificFilters = "";
            String containerSpecificQuery = "";
            String queries = "";
            String actualtableName = "";
            String tempObjName = "";
            String renamedColumn = "";

            int count = 0;
            int whereCount = 0;
            boolean check = false;
            for (Map<String, Map<String, Set<String>>> containerMap : eachVisualContainserList) {
                for (Map.Entry<String, Map<String, Set<String>>> entry : containerMap.entrySet()) {
                    String containerName = entry.getKey();
                    if (containerName == null && "".equals(containerName)) {
                        continue;
                    }
                    containerSpecificTransforms = pbixName + "_" + pbixContainer_i + "_dataTransforms_";
                    containerSpecificFilters = pbixName + "_" + pbixContainer_i + "_filters_";
                    containerSpecificQuery = pbixName + "_" + pbixContainer_i + "_query_";
                    if (visualContainersMap.get(containerSpecificQuery) != null) {
                        if (visualContainersMap.get(containerSpecificQuery).get("M-Query") != null) {
                            mQuery = visualContainersMap.get(containerSpecificQuery).get("M-Query");
                        }
                    }
                    if (visualContainersMap.get(containerSpecificQuery) != null) {
                        if (visualContainersMap.get(containerSpecificQuery).get("orderBy") != null) {
                            orderBy.addAll(visualContainersMap.get(containerSpecificQuery).get("orderBy"));
                        }
                    }

                    mQuerySb.append(pbixName).append(" : ").append(containerName).append(" : ").append(mQuery).append("\n\n");
                    Set<String> values = getExtendedValueForContainer(visualContainersMap, containerSpecificFilters);
                    if (values != null) {
                        filterValue.addAll(values);
                    }

                    if (visualContainersMap.get(containerSpecificFilters) != null) {
                        if (visualContainersMap.get(containerSpecificFilters).get("Order By") != null) {
                            orderBy.addAll(visualContainersMap.get(containerSpecificFilters).get("Order By"));
                        }
                    }
                    String temp = "";
                    String temp1 = "";
                    String tempBr = "";
                    Map<String, Set<String>> value = entry.getValue();
                    for (Map.Entry<String, Set<String>> entry1 : value.entrySet()) {
                        String colmetadata = "ERDG#ERWIN#0#ERWIN#0#ERWIN#0";
                        String datatype = "";
                        String type = "";
                        String format = "";
                        String key = entry1.getKey();
                        Set<String> value1 = entry1.getValue();

                        for (String columnWithTableAndExp : value1) {
                            if (PowerBIConfigPropertiesBuilder.mapOfActualTableAgainstQuery.containsKey(columnWithTableAndExp)) {
                                tempCol_temp = PowerBIConfigPropertiesBuilder.mapOfActualTableAgainstQuery.get(columnWithTableAndExp);
                            } else {
                                tempCol_temp = columnWithTableAndExp;
                            }
                            for (String tempCol : tempCol_temp.split("#ERWIN#")) {
                                mapSpec = new MappingSpecificationRow();
                                renamedColumn = "";
                                if (tempCol.contains(".")) {
                                    temp = tempCol.substring(0, tempCol.lastIndexOf("."));
                                    temp1 = tempCol.substring(tempCol.lastIndexOf(".") + 1);
                                    try {
                                        if (tempCol.contains("(")) {
                                            try {
                                                if ((!temp.contains(".") && !temp1.contains("("))) {
                                                    tempBr = tempCol;
                                                }
                                                tempCol = tempCol.substring(tempCol.indexOf("(") + 1, tempCol.indexOf(")"));
                                                srcTbl = tempCol.split("\\.")[0];
                                                srcCol = tempCol.split("\\.")[1];

                                            } catch (ArrayIndexOutOfBoundsException ae) {
                                                srcTbl = temp.trim();
                                                srcCol = temp1;
                                            }

                                        } else if (tempCol.contains("(") && (!temp.contains(".") && !temp1.contains("("))) {

                                            srcTbl = tempCol.substring(tempCol.indexOf("(") + 1, tempCol.indexOf("."));
                                            srcCol = tempCol.substring(tempCol.indexOf(".") + 1, tempCol.length() - 1);
                                        } else if (temp1.contains("(")) {
                                            srcTbl = temp;
                                            srcCol = temp1;
                                        } else {
                                            srcTbl = tempCol.substring(0, tempCol.indexOf("."));
                                            srcCol = tempCol.substring(tempCol.indexOf(".") + 1, tempCol.length());
                                        }
                                    } catch (ArrayIndexOutOfBoundsException ae) {
                                        srcTbl = temp.trim();
                                        srcCol = temp1;
                                    }
                                    if (srcTbl.contains("(") && !srcTbl.contains(")")) {
                                        srcTbl = srcTbl.split("\\(")[1];
                                    }
                                    String srcCol1 = "";
                                    srcTbl = srcTbl.replaceAll("[^a-zA-Z0-9_\\s\\-]", "").trim();
                                    String temp_srcCol = srcCol;
                                    if (srcCol.toLowerCase().contains("date hierarchy")) {
                                        srcCol = srcCol.split("\\.")[0];
                                    } else {
                                        srcCol = srcCol.split("\\.")[srcCol.split("\\.").length - 1];
                                    }
                                    if (renamedCol.get(columnWithTableAndExp) != null) {
                                        renamedColumn = renamedCol.get(columnWithTableAndExp);
                                        srcCol1 = renamedColumn;
                                    }
                                    if (revisedxmlFile.exists()) {
                                        if (!ReadXMLFileToObject.setOfLogicalTable.contains(srcTbl)) {
                                            continue;
                                        }
                                    }
                                    if (actualTableAgainstEntity.containsKey((srcTbl + "$" + srcCol).toUpperCase())) {
                                        srcTbl = actualTableAgainstEntity.get((srcTbl + "$" + srcCol).toUpperCase());
                                    }
                                    String ssasSourceTable = srcTbl;
                                    srcTbl = pbixName + "." + srcTbl;
                                    srcTbl = srcTbl.toUpperCase();
                                    if (columnsAgainstLogicalTable.containsKey(srcTbl.toUpperCase())) {
                                        Set<String> columns = columnsAgainstLogicalTable.get(srcTbl.toUpperCase());
                                        columns.add(srcCol.toUpperCase());
                                        columnsAgainstLogicalTable.put(srcTbl, columns);
                                    } else {
                                        Set<String> columns = new HashSet<>();
                                        columns.add(srcCol.toUpperCase());
                                        columnsAgainstLogicalTable.put(srcTbl, columns);
                                    }
                                    if (replaceForTableCheckValue.equalsIgnoreCase("true") && !"".equals(replaceForTableCheckValue) && replaceForTableCheckValue.length() != 0) {
                                        if (ReadXMLFileToObject.itemPathAndxlPath.get(srcTbl) != null) {
                                            try {
                                                String xpathValue = ReadXMLFileToObject.itemPathAndxlPath.get(srcTbl);
                                                String fileName = "";
                                                String filePath = "";
                                                try {
                                                    fileName = xpathValue.split("#erwin@")[0];
                                                    filePath = xpathValue.split("#erwin@")[1];
                                                } catch (Exception ex) {

                                                }
                                                extendedProperties.put(fileName, filePath);

                                            } catch (Exception e) {
                                                e.printStackTrace();
                                            }
                                        }
                                        if (queryFromXmlAgainstObjectMap.get(srcTbl) != null) {
                                            tempObjName = srcTbl;
                                            srcTbl = actualtableName;
                                            if (srcTbl.equals(tempObjName)) {
                                                check = true;
                                            }

                                            String tempcol = "";
                                            if (!check) {

                                                if (!renamedColumn.equals("")) {
                                                    renamedColumn = temp_srcCol;
                                                    tempcol = renamedColumn;
                                                } else {
                                                    tempcol = srcCol;
                                                }
                                                prepareLogicalColumnMap(tempObjName, tempcol);
                                                pbib.setSQL_SYSTEM_NAME(sourceSystemName1);
                                                pbib.setSQL_ENVIRONMENT_NAME(sourceEnvironmentName1);

                                            } else {
                                                tempcol = srcCol;
                                            }
                                            if (!pbib.isSSAS()) {
                                                if (pbib.isLogicalEnvironmentCreate()) {
                                                    metadataMapCreation(pbib.getPOWERBI_SYSTEM(), pbib.getPOWERBI_ENV_LOGICAL(), tempObjName, srcCol);
                                                }
                                                mapSpec = setMappingSpecificationForPBI3(pbib.getPOWERBI_SYSTEM(), pbib.getPOWERBI_ENV_LOGICAL(), tempObjName, tempcol, pbib.getPOWERBI_SYSTEM(), pbib.getPOWERBI_ENV_PAGE(), containerName, temp_srcCol, displayName1);
                                                pbib.setSQL_SYSTEM_NAME(sourceSystemName1);
                                                pbib.setSQL_ENVIRONMENT_NAME(sourceEnvironmentName1);
                                            } else {
                                                mapSpec = setMappingSpecificationForSSAS(pbib.getSSAS_SYSTEM_NAME(), pbib.getSSAS_ENVIRONMENT_NAME(), ssasSourceTable, tempcol, pbib.getPOWERBI_SYSTEM(), pbib.getPOWERBI_ENV_PAGE(), containerName, temp_srcCol, displayName1, tempObjName);
                                            }
                                        } else {
                                            if (check) {
                                                String tempcol = "";
                                                if (!renamedColumn.equals("")) {
                                                    tempcol = renamedColumn;
                                                } else {
                                                    tempcol = srcCol;
                                                }
                                                if (!pbib.isSSAS()) {
                                                    mapSpec = setMappingSpecificationForPBI3(pbib.getPOWERBI_SYSTEM(), pbib.getPOWERBI_ENV_LOGICAL(), srcTbl, tempcol, pbib.getPOWERBI_SYSTEM(), pbib.getPOWERBI_ENV_PAGE(), containerName, temp_srcCol, displayName1);
//                                                mapSpec.setExtendedBusinessRule(rules);
                                                } else {
                                                    mapSpec = setMappingSpecificationForSSAS(pbib.getSSAS_SYSTEM_NAME(), pbib.getSSAS_ENVIRONMENT_NAME(), ssasSourceTable, tempcol, pbib.getPOWERBI_SYSTEM(), pbib.getPOWERBI_ENV_PAGE(), containerName, temp_srcCol, displayName1, srcTbl);
                                                }
                                            } else {
                                                String tempcol = "";
                                                if (!renamedColumn.equals("")) {
                                                    tempcol = renamedColumn;
                                                } else {
                                                    tempcol = srcCol;
                                                }
                                                if (!pbib.isSSAS()) {
                                                    mapSpec = setMappingSpecificationForPBI3(pbib.getPOWERBI_SYSTEM(), pbib.getPOWERBI_ENV_LOGICAL(), srcTbl, tempcol, pbib.getPOWERBI_SYSTEM(), pbib.getPOWERBI_ENV_PAGE(), containerName, temp_srcCol, displayName1);
//                                                mapSpec.setExtendedBusinessRule(rules);
                                                } else {
                                                    mapSpec = setMappingSpecificationForSSAS(pbib.getSSAS_SYSTEM_NAME(), pbib.getSSAS_ENVIRONMENT_NAME(), ssasSourceTable, tempcol, pbib.getPOWERBI_SYSTEM(), pbib.getPOWERBI_ENV_PAGE(), containerName, temp_srcCol, displayName1, srcTbl);
                                                }
                                            }
                                            sourceSystemName = sourceSystemName1;
                                            sourceEnvironmentName = sourceEnvironmentName1;
                                            if (!pbib.isSSAS()) {
                                                if (pbib.isLogicalEnvironmentCreate()) {
                                                    metadataMapCreation(pbib.getPOWERBI_SYSTEM(), pbib.getPOWERBI_ENV_LOGICAL(), srcTbl, srcCol);
                                                }
                                            }
                                        }
                                    } else {
                                        if (!pbib.isSSAS()) {
                                            mapSpec = setMappingSpecificationForPBI3(pbib.getPOWERBI_SYSTEM(), pbib.getPOWERBI_ENV_LOGICAL(), srcTbl, srcCol, pbib.getPOWERBI_SYSTEM(), pbib.getPOWERBI_ENV_PAGE(), containerName, temp_srcCol, displayName1);
                                        } else {
                                            mapSpec = setMappingSpecificationForSSAS(pbib.getSSAS_SYSTEM_NAME(), pbib.getSSAS_ENVIRONMENT_NAME(), ssasSourceTable, srcCol, pbib.getPOWERBI_SYSTEM(), pbib.getPOWERBI_ENV_PAGE(), containerName, temp_srcCol, displayName1, srcTbl);
                                        }
                                    }
                                    mappingSpecifications.add(mapSpec);
                                    mapSpec = setMappingSpecificationForPBI2(pbib.getPOWERBI_SYSTEM(), pbib.getPOWERBI_ENV_PAGE(), containerName, temp_srcCol, pbib.getPOWERBI_SYSTEM(), pbib.getPOWERBI_ENV_PAGE(), pbixName + "." + displayName, srcCol, displayName1);
                                    if (pbib.isPageEnvironmentCreate()) {
                                        metadataMapCreation(pbib.getPOWERBI_SYSTEM(), pbib.getPOWERBI_ENV_PAGE(), pbixName + "." + displayName, srcCol);
                                    }
                                    mappingSpecifications.add(mapSpec);
                                    mapSpec = setMappingSpecificationForPBI2(pbib.getPOWERBI_SYSTEM(), pbib.getPOWERBI_ENV_PAGE(), pbixName + "." + displayName, srcCol, pbib.getPOWERBI_SYSTEM(), pbib.getPOWERBI_ENV_DASHBOARD(), pbixName, srcCol, displayName1);
                                    if (pbib.isDashboardEnvironmentCreate()) {
                                        metadataMapCreation(pbib.getPOWERBI_SYSTEM(), pbib.getPOWERBI_ENV_DASHBOARD(), pbixName, srcCol);
                                    }
                                    finalMappingSpecifications.add(mapSpec);
                                    sourceSystemName = sourceSystemName1;
                                    sourceEnvironmentName = sourceEnvironmentName1;
                                    check = false;
                                }
                            }
                        }
                    }

                }
                pbixContainer_i = pbixContainer_i + 1;
            }
            if (orderBy != null) {
                List<String> orderByList = new ArrayList(orderBy);
                for (int ol = 0; ol < orderByList.size(); ol++) {
                    extendedProperties.put("Order By _" + ol + count, orderByList.get(ol));
                    count++;
                }
                for (Map.Entry<String, Set<String>> entry : PowerBIConfigPropertiesBuilder.extendedPropeties.entrySet()) {
                    String key = entry.getKey();
                    Set<String> value = entry.getValue();
                    ArrayList<String> listValue = new ArrayList<>(value);
                    for (int ol = 0; ol < listValue.size(); ol++) {
                        String next = listValue.get(ol);
                        extendedProperties.put("Order By _" + ol + count, next);
                        count++;
                    }

                }
            }
            for (Map.Entry<String, Map<String, Set<String>>> filterWhere : PowerBIConfigPropertiesBuilder.extendedPropetiesForWhere.entrySet()) {
                Map<String, Set<String>> filterValues = filterWhere.getValue();
                for (Map.Entry<String, Set<String>> eachfilterValue : filterValues.entrySet()) {
                    Set<String> eachsetValue = eachfilterValue.getValue();
                    ArrayList<String> listValue = new ArrayList<>(eachsetValue);
                    for (int sv = 0; sv < listValue.size(); sv++) {
                        String whereValue = listValue.get(sv);
                        extendedProperties.put("filter_Where" + sv + whereCount, whereValue);
                        whereCount++;
                    }
                }
            }
            if (!filterValue.isEmpty()) {
                List<String> filterValueList = new ArrayList(filterValue);
                for (int ol = 0; ol < filterValueList.size(); ol++) {
                    extendedProperties.put("Filter_" + ol, filterValueList.get(ol));

                }
            }
//            ArrayList<MappingSpecificationRow> newspecs = PowerBI_MappingArrangement.removeReportNameAsSchema(mappingSpecifications, pbixName.toUpperCase());
            if (map.getMappingId() > 0) {
                String addSpecStatus = mappingManagerUtil.addMappingSpecifications(map.getMappingId(), mappingSpecifications).getStatusMessage();
                logMessage.append(displayName).append(" : ").append(addSpecStatus).append("\n");
                status.append(displayName).append(" : ").append(addSpecStatus).append("\n");
            } else {
                map.setMappingSpecifications(mappingSpecifications);
            }
            map.setSourceExtractQuery(queries);
            queries = queries.replace("</div><div>", "\n");
            queries = queries.replace("</div>", "");
            queries = queries.replace("<div>", "");
            document.setDocumentName(displayName);
            FileUtils.writeStringToFile(new File((DOCUMENT_OBJECT + displayName + "\\" + pbixName + "_" + displayName + ".sql").replace(" ", "_")), queries);
            document.setDocumentObject((DOCUMENT_OBJECT + displayName + "\\" + pbixName + "_" + displayName + ".sql").replace(" ", "_"));
            document.setFilePathType(APIConstants.FilePathType.ABSOLUTE);
            document.setDocumentLink("");
            document.setDocumentStatus("ACTIVE");
            document.setDocumentIntendedPurpose("PowerBI Source References");
            if (map.getMappingSpecifications().size() > 0) {
                ArrayList<MappingSpecificationRow> updatedMapSpecs = removeDuplicateMappingSpecification(map.getMappingSpecifications(), pbixName.toUpperCase());
                if (updatedMapSpecs != null && updatedMapSpecs.size() > 0) {
                    map.setMappingSpecifications(updatedMapSpecs);
                }
                if (map.getMappingId() <= 0) {
                    String userDefinedValue = "";
                    for (Map.Entry<String, String> entry : extendedProperties.entrySet()) {
                        userDefinedValue = "<div>" + userDefinedValue + entry.getKey() + "=>  " + entry.getValue() + "</div>";
                    }
                    map.setUserDefined1(userDefinedValue);

                    String mappingCreationStatus = mappingManagerUtil.createMapping(map).getStatusMessage();
                    PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Info::" + mappingCreationStatus + "for Page : " + displayName + "\n");
                    PowerBI_LOGGER.overviewLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Info::" + mappingCreationStatus + "for Page : " + displayName + "\n");
                    logMessage.append(displayName + " : " + mappingCreationStatus + "\n");
                    status.append(displayName + " : " + mappingCreationStatus + "\n");

                }
                PowerBIConfigPropertiesBuilder.extendedPropeties.clear();
                PowerBIConfigPropertiesBuilder.businessRuleMap.clear();
                PowerBIConfigPropertiesBuilder.actualSourceMap.clear();
                PowerBIConfigPropertiesBuilder.extendedPropetiesForWhere.clear();
                mapId = mappingManagerUtil.getMappingId(subId, displayName.trim(), com.ads.api.beans.common.Node.NodeType.MM_SUBJECT);
                if (mapId > 0) {
                    mappingManagerUtil.addMappingDocument(mapId, document);
                    Map<String, String> extendedProperties_1 = new HashMap<>();
                    for (Map.Entry<String, String> entry : extendedProperties.entrySet()) {
                        String key = entry.getKey();
                        if (key.trim().equalsIgnoreCase("Manual Table")) {
                            continue;
                        }
                        String value = entry.getValue();
                        extendedProperties_1.put(key, value);
                    }
                    status.append(keyValueUtil.addKeyValueMap(extendedProperties_1, OBJECT_TYPE_ID, "" + mapId).getStatusMessage()).append("\n");
                }
            }
            PowerBIFiltersPropertiesBuilder.filterInfoMap.clear();
        } catch (Exception e) {
            logger.error(e.getMessage());
            StringWriter exception = new StringWriter();
            e.printStackTrace(new PrintWriter(exception));
            PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Error::Exception In PowerBIReportParser.createMappingWithLayoutJsonInAMM() Method " + exception + "\n");
        }
        return status.toString();
    }

    public static void prepareLogicalColumnMap(String tableName, String columnName) {
        if (columnsAgainstLogicalTable.containsKey(tableName)) {
            Set<String> columns = columnsAgainstLogicalTable.get(tableName);
            columns.add(columnName);
            columnsAgainstLogicalTable.put(tableName, columns);
        } else {
            Set<String> columns = new HashSet<>();
            columns.add(columnName);
            columnsAgainstLogicalTable.put(tableName, columns);
        }
    }

    public static void metadataMapCreation(String systemName, String environmentName, String tableName, String columnName) {
        tableName = tableName.replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim();

        try {

            if (metadataMap.containsKey(systemName)) {

                Map<String, Map<String, Set<String>>> envMap = metadataMap.get(systemName);
                if (envMap.containsKey(environmentName)) {
                    Map<String, Set<String>> tableMap = envMap.get(environmentName);
                    if (tableMap.containsKey(tableName)) {
                        Set<String> oldcolList = tableMap.get(tableName);
                        oldcolList.add(columnName);
                        tableMap.put(tableName, oldcolList);
                        envMap.put(environmentName, tableMap);
                        metadataMap.put(systemName, envMap);

                    } else {
                        Set<String> setOfColumns = new HashSet();
                        Map<String, Set<String>> tablecolMap = new HashMap();
                        setOfColumns.add(columnName);
                        tablecolMap.put(tableName, setOfColumns);
                        tableMap.putAll(tablecolMap);
                    }
                } else {
                    Set<String> setOfColumns = new HashSet();
                    Map<String, Map<String, Set<String>>> environmentMap = new HashMap();
                    Map<String, Set<String>> tablesMap = new HashMap();
                    setOfColumns.add(columnName);
                    tablesMap.put(tableName, setOfColumns);
                    environmentMap.put(environmentName, tablesMap);
                    envMap.putAll(environmentMap);
                }

            } else {
                Set<String> setOfColumns = new HashSet();
                Map<String, Map<String, Set<String>>> environmentMap = new HashMap();
                Map<String, Set<String>> tablesMap = new HashMap();
                setOfColumns.add(columnName);
                tablesMap.put(tableName, setOfColumns);
                environmentMap.put(environmentName, tablesMap);
                metadataMap.put(systemName, environmentMap);

            }

        } catch (Exception e) {
            logger.error(e.getMessage());
            StringWriter exception = new StringWriter();
            e.printStackTrace(new PrintWriter(exception));
            PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Error::Exception In PowerBIReportParser.metadataMapCreation() Method " + exception + "\n");
        }

    }

    /**
     * This method help to get the key value for the mapping of the respective
     * container
     *
     * @param visualContainersMap
     * @param key
     * @return String
     */
    public static Set<String> getExtendedValueForContainer(Map<String, Map<String, Set<String>>> visualContainersMap, String key) {

        Set<String> containerSet = new HashSet();
        Map<String, Set<String>> value = visualContainersMap.get(key);
        if (value != null) {
            for (Map.Entry<String, Set<String>> entry1 : value.entrySet()) {
                Set<String> value1 = entry1.getValue();
                List<String> listofValues = new ArrayList(value1);
                for (int lv = 0; lv < listofValues.size(); lv++) {
                    containerSet.add(listofValues.get(lv));
                }

            }
        }
        return containerSet;
    }

    public static void removeDuplicateMappingSpecificationWithBusinessRules(MappingSpecificationRow eachMapSpec, String displayName, Set<String> CountersetofSpecs, ArrayList<MappingSpecificationRow> updatedMapsec, String businessRule) {
        String allsrctgrDetails = "";
        String sourcetableName = eachMapSpec.getSourceTableName().trim();
        String targettableName = eachMapSpec.getTargetTableName().trim();
        String sourceColumnName = eachMapSpec.getSourceColumnName().trim();
        String targetColumnName = eachMapSpec.getTargetColumnName().trim();

        if ((targettableName.contains(".") && targettableName.split("\\.").length == 1 || targettableName.replace(displayName.toUpperCase(), "").trim().equals(".")) && (eachMapSpec.getTargetSystemName().equalsIgnoreCase(pbib.getSQL_SYSTEM_NAME()) && eachMapSpec.getTargetSystemEnvironmentName().equalsIgnoreCase(pbib.getSQL_ENVIRONMENT_NAME()))) {
            return;
        }
        if ((sourcetableName.contains(".") && sourcetableName.split("\\.").length == 1 || sourcetableName.replace(displayName.toUpperCase(), "").trim().equals(".")) && (eachMapSpec.getSourceSystemName().equalsIgnoreCase(pbib.getSQL_SYSTEM_NAME()) && eachMapSpec.getSourceSystemEnvironmentName().equalsIgnoreCase(pbib.getSQL_ENVIRONMENT_NAME()))) {
            return;
        }
        if ((sourcetableName.contains(".") && eachMapSpec.getSourceSystemName().equalsIgnoreCase(pbib.getSQL_SYSTEM_NAME()) && eachMapSpec.getSourceSystemEnvironmentName().equalsIgnoreCase(pbib.getSQL_ENVIRONMENT_NAME())) && !eachMapSpec.getSourceTableName().contains(displayName.toUpperCase())) {
            if (ReadXMLFileToObject.serverDBAgainstTableName.containsKey(sourcetableName.toUpperCase().split("\\.")[1])) {
                String serverName = ReadXMLFileToObject.serverDBAgainstTableName.get(sourcetableName.toUpperCase().split("\\.")[1]).split("\\*ERWIN\\*")[0].replaceAll("[^a-zA-Z0-9_\\s\\-]", "_");
                String databaseName = ReadXMLFileToObject.serverDBAgainstTableName.get(sourcetableName.toUpperCase().split("\\.")[1]).split("\\*ERWIN\\*")[1].replaceAll("[^a-zA-Z0-9_\\s\\-]", "_");
//                String sysEnv = SyncupWithServerDBSchamaSysEnvCPT.newmetasync(setMetadataMap1("", databaseName, serverName, metadataJsonPath, pbib.getSQL_SYSTEM_NAME(), pbib.getSQL_ENVIRONMENT_NAME(), allDBMap, allTablesMap, metadataMap, "", systemManaegerUtil, sourcetableName.toUpperCase().split("\\.")[1]));
//                eachMapSpec.setSourceSystemName(sysEnv.split("@erwin@")[0]);
//                eachMapSpec.setSourceSystemEnvironmentName(sysEnv.split("@erwin@")[1]);
//                eachMapSpec.setSourceTableName(sourcetableName.toUpperCase());
            } else if (!eachMapSpec.getSourceTableName().contains(displayName)) {
//                String intermediateSysEnvInfo1 = SyncMetadataJsonFileDesign.newmetasync(sourcetableName.split("\\.")[1], "", "", PowerBIReportParser.metadataJsonPath, eachMapSpec.getSourceSystemName(), eachMapSpec.getSourceSystemEnvironmentName(), PowerBIReportParser.cacheMap, PowerBIReportParser.allTablesMap, PowerBIReportParser.allDBMap, "", "", "");
//                eachMapSpec.setSourceSystemName(intermediateSysEnvInfo1.split("##")[0]);
//                eachMapSpec.setSourceSystemEnvironmentName(intermediateSysEnvInfo1.split("##")[1]);
//                eachMapSpec.setSourceTableName(sourcetableName.toUpperCase());
            }
        }
        if ((targettableName.contains(".") && eachMapSpec.getTargetSystemName().equalsIgnoreCase(pbib.getSQL_SYSTEM_NAME()) && eachMapSpec.getTargetSystemEnvironmentName().equalsIgnoreCase(pbib.getSQL_ENVIRONMENT_NAME())) && !eachMapSpec.getTargetTableName().contains(displayName.toUpperCase())) {
            if (ReadXMLFileToObject.serverDBAgainstTableName.containsKey(targettableName.toUpperCase().split("\\.")[1])) {
                String serverName = ReadXMLFileToObject.serverDBAgainstTableName.get(targettableName.toUpperCase().split("\\.")[1]).split("\\*ERWIN\\*")[0].replaceAll("[^a-zA-Z0-9_\\s]", "_");
                String databaseName = ReadXMLFileToObject.serverDBAgainstTableName.get(targettableName.toUpperCase().split("\\.")[1]).split("\\*ERWIN\\*")[1].replaceAll("[^a-zA-Z0-9_\\s]", "_");
//                String sysEnv = SyncupWithServerDBSchamaSysEnvCPT.newmetasync(setMetadataMap1("", databaseName, serverName, metadataJsonPath, pbib.getSQL_SYSTEM_NAME(), pbib.getSQL_ENVIRONMENT_NAME(), allDBMap, allTablesMap, metadataMap, "", systemManaegerUtil, targettableName.toUpperCase().split("\\.")[1]));
//                eachMapSpec.setTargetSystemName(sysEnv.split("@erwin@")[0]);
//                eachMapSpec.setTargetSystemEnvironmentName(sysEnv.split("@erwin@")[1]);
//                eachMapSpec.setTargetTableName(targettableName.toUpperCase());
            } else if (!eachMapSpec.getTargetTableName().contains(displayName)) {
//                String intermediateSysEnvInfo2 = SyncMetadataJsonFileDesign.newmetasync(targettableName.split("\\.")[1], "", "", PowerBIReportParser.metadataJsonPath, eachMapSpec.getTargetSystemName(), eachMapSpec.getTargetSystemEnvironmentName(), PowerBIReportParser.cacheMap, PowerBIReportParser.allTablesMap, PowerBIReportParser.allDBMap, "", "", "");
//                eachMapSpec.setTargetSystemName(intermediateSysEnvInfo2.split("##")[0]);
//                eachMapSpec.setTargetSystemEnvironmentName(intermediateSysEnvInfo2.split("##")[1]);
//                eachMapSpec.setTargetTableName(targettableName.toUpperCase());
            }
        }
        if (!sourceColumnName.trim().contains("DUMMY") && !StringUtils.isBlank(sourceColumnName) && !StringUtils.isBlank(targetColumnName)) {
            if (!targetColumnName.trim().contains("DUMMY")) {
                allsrctgrDetails = sourcetableName + "!Erwin!" + sourceColumnName + "!Erwin!" + targettableName + "!Erwin!" + targetColumnName + "!Erwin!" + businessRule;
                if (CountersetofSpecs.contains(allsrctgrDetails) == false) {
                    eachMapSpec.setSourceColumnName(sourceColumnName.replace("\"", ""));
                    eachMapSpec.setTargetColumnName(targetColumnName.replace("\"", ""));
                    updatedMapsec.add(eachMapSpec);
                    CountersetofSpecs.add(allsrctgrDetails);
                }
            }
        }
    }

    public static ArrayList<MappingSpecificationRow> removeDuplicateMappingSpecification(ArrayList<MappingSpecificationRow> mapspecs, String displayName) {
        try {
            String businessRule = "";
            String sourcetableName = "";
            String targettableName = "";
            String sourceColumnName = "";
            String targetColumnName = "";
            String allsrctgrDetails = "";
            Set<String> CountersetofSpecs = new HashSet();
            Set<String> CountersetofSpecswithoutBr = new HashSet();
            ArrayList<MappingSpecificationRow> updatedMapsec = new ArrayList();
            for (MappingSpecificationRow eachMapSpec : mapspecs) {
                sourcetableName = eachMapSpec.getSourceTableName().trim();
                targettableName = eachMapSpec.getTargetTableName().trim();
                sourceColumnName = eachMapSpec.getSourceColumnName().trim();
                targetColumnName = eachMapSpec.getTargetColumnName().trim();
//                if(){
//                    
//                }
                if (sourceColumnName.contains("RowNumber")) {
                    continue;
                }
                if (eachMapSpec.getSourceTableName().replace(displayName, "").trim().equals(".-")) {
                    continue;
                }
                if (eachMapSpec.getSourceTableName().toUpperCase().contains("FOLDER.FILES") || eachMapSpec.getSourceTableName().toUpperCase().contains(".FILESCUSERS")) {
                    continue;
                }
                if (eachMapSpec.getSourceTableName().split("\\.").length == 2 && eachMapSpec.getSourceTableName().split("\\.")[1].indexOf("-") == 0) {
                    String tab_temp = eachMapSpec.getSourceTableName().split("\\.")[0].toUpperCase() + "." + eachMapSpec.getSourceTableName().split("\\.")[1].substring(1);
                    eachMapSpec.setSourceTableName(tab_temp);
                }
                if (!eachMapSpec.getSourceTableName().replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim().toUpperCase().contains("REMOVED DUPLICATES") && !eachMapSpec.getSourceTableName().replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim().toUpperCase().contains("CHANGED TYPE") && !eachMapSpec.getSourceTableName().replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim().toUpperCase().contains("TABLE.SELECTCOLUMNS") && !eachMapSpec.getSourceTableName().replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim().toUpperCase().contains("ODATA.FEED") && !eachMapSpec.getSourceTableName().replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim().toUpperCase().contains("TABLE.SELECTROWS") && !eachMapSpec.getSourceTableName().replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim().toUpperCase().contains("FILTERED ROWS") && !eachMapSpec.getSourceTableName().replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim().toUpperCase().contains("SPLIT COLUMN BY DELIMITER") && !eachMapSpec.getSourceTableName().replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim().toUpperCase().contains("TABLE.TRANSFORMCOLUMNS") && !eachMapSpec.getSourceTableName().replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim().toUpperCase().contains(".EXPANDED ") && !eachMapSpec.getSourceTableName().replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim().toUpperCase().contains("RENAMED COLUMNS") && !eachMapSpec.getSourceTableName().replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim().toUpperCase().contains("ADDED CUSTOM") && !eachMapSpec.getSourceTableName().replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim().toUpperCase().contains("DUPLICATED COLUMN") && !eachMapSpec.getSourceTableName().replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim().toUpperCase().contains("MERGED ") && !eachMapSpec.getSourceTableName().replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim().toUpperCase().contains("REMOVED COLUMNS") && !eachMapSpec.getSourceTableName().replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim().toUpperCase().contains("REORDERED COLUMNS") && !eachMapSpec.getSourceTableName().replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim().toUpperCase().contains("NEWCOLUMN")) {
                    eachMapSpec.setSourceTableName(eachMapSpec.getSourceTableName().replace("\"", ""));
                    if (!targettableName.toUpperCase().trim().equals(displayName.toUpperCase().trim() + "." + displayName.toUpperCase().trim()) && !sourcetableName.toUpperCase().trim().equals(displayName.toUpperCase().trim() + "." + displayName.toUpperCase().trim())) {
                        eachMapSpec.setSourceTableName(eachMapSpec.getSourceTableName().replace(displayName.toUpperCase() + "." + displayName.toUpperCase() + ".", displayName.toUpperCase() + "."));
                    }
                    eachMapSpec.setSourceColumnName(eachMapSpec.getSourceColumnName().replace("\"", "").replace("[", "").replace("]", ""));
                    eachMapSpec.setTargetColumnName(eachMapSpec.getTargetColumnName().replace("\"", "").replace("[", "").replace("]", ""));
                    eachMapSpec.setSourceTableName(eachMapSpec.getSourceTableName().replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim().toUpperCase());
                    eachMapSpec.setTargetTableName(eachMapSpec.getTargetTableName().replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim().toUpperCase());
                    if (eachMapSpec.getSourceSystemName().equals("SYS") || eachMapSpec.getTargetSystemName().equals("SYS")) {
                        continue;
                    }
                    if ((eachMapSpec.getSourceSystemName().equals("PBIX") && !pbib.getPOWERBI_SYSTEM().equals("PBIX")) || (eachMapSpec.getTargetSystemName().equals("PBIX") && !pbib.getPOWERBI_SYSTEM().equals("PBIX"))) {
                        continue;
                    }
                    if (eachMapSpec.getTargetColumnName().trim().equals(" ") || eachMapSpec.getSourceColumnName().trim().equals(" ")) {
                        continue;
                    }
                    businessRule = eachMapSpec.getBusinessRule().trim();
                    if (businessRule != null && !businessRule.isEmpty() && businessRule.length() != 0) {
                        sourcetableName = eachMapSpec.getSourceTableName().trim();
                        targettableName = eachMapSpec.getTargetTableName().trim();
                        sourceColumnName = eachMapSpec.getSourceColumnName().trim();
                        targetColumnName = eachMapSpec.getTargetColumnName().trim();
                        if (!targettableName.toUpperCase().trim().equals(displayName.toUpperCase().trim() + "." + displayName.toUpperCase().trim()) && !sourcetableName.toUpperCase().trim().equals(displayName.toUpperCase().trim() + "." + displayName.toUpperCase().trim())) {
                            if ((targettableName.contains(".") && targettableName.split("\\.").length == 1 || targettableName.replace(displayName.toUpperCase(), "").trim().equals(".")) && (eachMapSpec.getTargetSystemName().equalsIgnoreCase(pbib.getSQL_SYSTEM_NAME()) && eachMapSpec.getTargetSystemEnvironmentName().equalsIgnoreCase(pbib.getSQL_ENVIRONMENT_NAME()))) {
                                continue;
                            }
                            if ((sourcetableName.contains(".") && sourcetableName.split("\\.").length == 1 || sourcetableName.replace(displayName.toUpperCase(), "").trim().equals(".")) && (eachMapSpec.getSourceSystemName().equalsIgnoreCase(pbib.getSQL_SYSTEM_NAME()) && eachMapSpec.getSourceSystemEnvironmentName().equalsIgnoreCase(pbib.getSQL_ENVIRONMENT_NAME()))) {
                                continue;
                            }
                        }
                        if ((sourcetableName.contains(".") && eachMapSpec.getSourceSystemName().equalsIgnoreCase(pbib.getSQL_SYSTEM_NAME()) && eachMapSpec.getSourceSystemEnvironmentName().equalsIgnoreCase(pbib.getSQL_ENVIRONMENT_NAME())) && !eachMapSpec.getSourceTableName().contains(displayName.toUpperCase())) {
                            if (!eachMapSpec.getSourceTableName().contains(displayName)) {

//                                String intermediateSysEnvInfo1 = SyncMetadataJsonFileDesign.newmetasync(sourcetableName.split("\\.")[1], "", "", PowerBIReportParser.metadataJsonPath, eachMapSpec.getSourceSystemName(), eachMapSpec.getSourceSystemEnvironmentName(), PowerBIReportParser.cacheMap, PowerBIReportParser.allTablesMap, PowerBIReportParser.allDBMap, "", "", "");
//                                eachMapSpec.setSourceSystemName(intermediateSysEnvInfo1.split("##")[0]);
//                                eachMapSpec.setSourceSystemEnvironmentName(intermediateSysEnvInfo1.split("##")[1]);
//                                eachMapSpec.setSourceTableName(sourcetableName.toUpperCase());
                            }
                        }

                        if ((targettableName.contains(".") && eachMapSpec.getTargetSystemName().equalsIgnoreCase(pbib.getSQL_SYSTEM_NAME()) && eachMapSpec.getTargetSystemEnvironmentName().equalsIgnoreCase(pbib.getSQL_ENVIRONMENT_NAME())) && !eachMapSpec.getTargetTableName().contains(displayName.toUpperCase())) {
                            if (!eachMapSpec.getTargetTableName().contains(displayName)) {

//                                String intermediateSysEnvInfo2 = SyncMetadataJsonFileDesign.newmetasync(targettableName.split("\\.")[1], "", "", PowerBIReportParser.metadataJsonPath, eachMapSpec.getTargetSystemName(), eachMapSpec.getTargetSystemEnvironmentName(), PowerBIReportParser.cacheMap, PowerBIReportParser.allTablesMap, PowerBIReportParser.allDBMap, "", "", "");
//                                eachMapSpec.setTargetSystemName(intermediateSysEnvInfo2.split("##")[0]);
//                                eachMapSpec.setTargetSystemEnvironmentName(intermediateSysEnvInfo2.split("##")[1]);
//                                eachMapSpec.setTargetTableName(targettableName.toUpperCase());
                            }
                        }

//                        if (!sourceColumnName.trim().contains("DUMMY") && !StringUtils.isBlank(sourceColumnName) && !StringUtils.isBlank(targetColumnName)) {
//                            if (!targetColumnName.trim().contains("DUMMY")) {
                        allsrctgrDetails = sourcetableName + "!Erwin!" + sourceColumnName + "!Erwin!" + targettableName + "!Erwin!" + targetColumnName + "!Erwin!" + businessRule;
                        if (CountersetofSpecs.contains(allsrctgrDetails) == false) {
                            eachMapSpec.setSourceColumnName(sourceColumnName.replace("\"", ""));
                            eachMapSpec.setTargetColumnName(targetColumnName.replace("\"", ""));
                            updatedMapsec.add(eachMapSpec);
                            CountersetofSpecs.add(allsrctgrDetails);
                        }
//                            }
//                        }
                    } else {
                        sourcetableName = eachMapSpec.getSourceTableName().trim();
                        targettableName = eachMapSpec.getTargetTableName().trim();
                        sourceColumnName = eachMapSpec.getSourceColumnName().trim();
                        targetColumnName = eachMapSpec.getTargetColumnName().trim();
                        if (!targettableName.toUpperCase().trim().equals(displayName.toUpperCase().trim() + "." + displayName.toUpperCase().trim()) && !sourcetableName.toUpperCase().trim().equals(displayName.toUpperCase().trim() + "." + displayName.toUpperCase().trim())) {
                            if ((targettableName.contains(".") && targettableName.split("\\.").length == 1 || targettableName.replace(displayName.toUpperCase(), "").trim().equals(".")) && (eachMapSpec.getTargetSystemName().equalsIgnoreCase(pbib.getSQL_SYSTEM_NAME()) && eachMapSpec.getTargetSystemEnvironmentName().equalsIgnoreCase(pbib.getSQL_ENVIRONMENT_NAME()))) {
                                continue;
                            }
                            if ((sourcetableName.contains(".") && sourcetableName.split("\\.").length == 1 || sourcetableName.replace(displayName.toUpperCase(), "").trim().equals(".")) && (eachMapSpec.getSourceSystemName().equalsIgnoreCase(pbib.getSQL_SYSTEM_NAME()) && eachMapSpec.getSourceSystemEnvironmentName().equalsIgnoreCase(pbib.getSQL_ENVIRONMENT_NAME()))) {
                                continue;
                            }
                        }
                        if ((sourcetableName.contains(".") && eachMapSpec.getSourceSystemName().equalsIgnoreCase(pbib.getSQL_SYSTEM_NAME()) && eachMapSpec.getSourceSystemEnvironmentName().equalsIgnoreCase(pbib.getSQL_ENVIRONMENT_NAME())) && !eachMapSpec.getSourceTableName().contains(displayName.toUpperCase())) {
                            if (!eachMapSpec.getSourceTableName().contains(displayName)) {
//                                String intermediateSysEnvInfo1 = SyncMetadataJsonFileDesign.newmetasync(sourcetableName.split("\\.")[1], "", "", PowerBIReportParser.metadataJsonPath, eachMapSpec.getSourceSystemName(), eachMapSpec.getSourceSystemEnvironmentName(), PowerBIReportParser.cacheMap, PowerBIReportParser.allTablesMap, PowerBIReportParser.allDBMap, "", "", "");
//                                eachMapSpec.setSourceSystemName(intermediateSysEnvInfo1.split("##")[0]);
//                                eachMapSpec.setSourceSystemEnvironmentName(intermediateSysEnvInfo1.split("##")[1]);
//                                eachMapSpec.setSourceTableName(sourcetableName.toUpperCase());
                            }
                        }
                        if ((targettableName.contains(".") && eachMapSpec.getTargetSystemName().equalsIgnoreCase(pbib.getSQL_SYSTEM_NAME()) && eachMapSpec.getTargetSystemEnvironmentName().equalsIgnoreCase(pbib.getSQL_ENVIRONMENT_NAME())) && !eachMapSpec.getTargetTableName().contains(displayName.toUpperCase())) {
                            if (!eachMapSpec.getTargetTableName().contains(displayName)) {
//                                String intermediateSysEnvInfo2 = SyncMetadataJsonFileDesign.newmetasync(targettableName.split("\\.")[1], "", "", PowerBIReportParser.metadataJsonPath, eachMapSpec.getTargetSystemName(), eachMapSpec.getTargetSystemEnvironmentName(), PowerBIReportParser.cacheMap, PowerBIReportParser.allTablesMap, PowerBIReportParser.allDBMap, "", "", "");
//                                eachMapSpec.setTargetSystemName(intermediateSysEnvInfo2.split("##")[0]);
//                                eachMapSpec.setTargetSystemEnvironmentName(intermediateSysEnvInfo2.split("##")[1]);
//                                eachMapSpec.setTargetTableName(targettableName.toUpperCase());
                            }
                        }
//                        if (!sourceColumnName.trim().contains("DUMMY") && !StringUtils.isBlank(sourceColumnName) && !StringUtils.isBlank(targettableName)) {
//                            if (!targetColumnName.trim().contains("DUMMY")) {
                        allsrctgrDetails = sourcetableName + "!Erwin!" + sourceColumnName + "!Erwin!" + targettableName + "!Erwin!" + targetColumnName;
                        if (CountersetofSpecswithoutBr.contains(allsrctgrDetails) == false) {
                            eachMapSpec.setSourceColumnName(sourceColumnName.replace("\"", ""));
                            eachMapSpec.setTargetColumnName(targetColumnName.replace("\"", ""));
                            updatedMapsec.add(eachMapSpec);
                            CountersetofSpecswithoutBr.add(allsrctgrDetails);
                        }
//                            }
//                        }
                    }
                }
            }
            ArrayList<MappingSpecificationRow> updatedMapsec1 = removeSelfLineage(updatedMapsec, displayName);
            ArrayList<MappingSpecificationRow> updatedMapsec2 = removeExtraComponent(updatedMapsec1);
            ArrayList<MappingSpecificationRow> updatedMapsec3 = removeMapSpecDuplicatesForBusinessRule1(updatedMapsec2, displayName);
            return updatedMapsec3;
        } catch (Exception e) {
            logger.error(e.getMessage());
            StringWriter exception = new StringWriter();
            e.printStackTrace(new PrintWriter(exception));
            PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Error::Exception In PowerBIReportParser.removeMapSpecDuplicatesForBusinessRule() Method " + exception + "\n");
            return null;
        }

    }

    public static ArrayList<MappingSpecificationRow> removeSelfLineage(ArrayList<MappingSpecificationRow> mapspecs, String reportName) {
        ArrayList<MappingSpecificationRow> updatedMapSpecRow = new ArrayList();
        String sourceSystem = "";
        String targetSystem = "";
        String sourceTableName = "";
        String targetTableName = "";
        String sourceColName = "";
        String targetColName = "";
        String srcEnvName = "";
        String tgtEnvName = "";
        try {
            for (MappingSpecificationRow eachMapSpec : mapspecs) {
                sourceSystem = eachMapSpec.getSourceSystemName();
                targetSystem = eachMapSpec.getTargetSystemName();
                sourceTableName = eachMapSpec.getSourceTableName();
                targetTableName = eachMapSpec.getTargetTableName();
                sourceColName = eachMapSpec.getSourceColumnName();
                targetColName = eachMapSpec.getTargetColumnName();
                srcEnvName = eachMapSpec.getSourceSystemEnvironmentName();
                tgtEnvName = eachMapSpec.getTargetSystemEnvironmentName();
                if (sourceSystem.equalsIgnoreCase(targetSystem) && srcEnvName.equalsIgnoreCase(tgtEnvName) && sourceTableName.equalsIgnoreCase(targetTableName) && sourceColName.equalsIgnoreCase(targetColName)) {
                    continue;
                } else {
                    if (sourceTableName.contains("DBO.") && sourceTableName.contains(reportName.toUpperCase() + ".")) {
                        eachMapSpec.setSourceTableName(sourceTableName.replace("DBO.", ""));
                    }
                    if (eachMapSpec.getSourceTableName().contains(reportName.toUpperCase() + ".")) {
                        if (itemPathAndQueryMap.containsKey(eachMapSpec.getSourceTableName())) {
                            MappingSpecificationRow newSpec = new MappingSpecificationRow();
                            String sourcetableName = itemPathAndQueryMap.get(eachMapSpec.getSourceTableName()).split("#@ERWIN@#")[0];
                            if (sourcetableName != null && !sourcetableName.contains("SELECT") && !sourcetableName.toLowerCase().contains(".xlsx") && !itemPathAndQueryMap.get(eachMapSpec.getSourceTableName()).toUpperCase().contains("PROC")) {

                                String temp_tab = sourcetableName;
                                if (ReadXMLFileToObject.serverDBAgainstTableName.containsKey(temp_tab)) {
                                    String serverName = ReadXMLFileToObject.serverDBAgainstTableName.get(temp_tab).split("\\*ERWIN\\*")[0].replaceAll("[^a-zA-Z0-9_\\s\\-]", "_");
                                    String databaseName = ReadXMLFileToObject.serverDBAgainstTableName.get(temp_tab).split("\\*ERWIN\\*")[1].replaceAll("[^a-zA-Z0-9_\\s\\-]", "_");
//                                    String sysEnv = SyncupWithServerDBSchamaSysEnvCPT.newmetasync(setMetadataMap1("", databaseName, serverName, metadataJsonPath, pbib.getSQL_SYSTEM_NAME(), pbib.getSQL_ENVIRONMENT_NAME(), allDBMap, allTablesMap, metadataMap, "", systemManaegerUtil, temp_tab));
//                                    newSpec.setSourceSystemName(sysEnv.split("@erwin@")[0]);
//                                    newSpec.setSourceSystemEnvironmentName(sysEnv.split("@erwin@")[1]);
//                                    newSpec.setSourceTableName(sourcetableName.toUpperCase().replace(".xlsx", ""));
                                } else {
                                    if (temp_tab.toUpperCase().contains(".CSV") || temp_tab.toUpperCase().contains(".XLSX") || temp_tab.toUpperCase().contains(".JSON") || temp_tab.toUpperCase().contains(".XML") || temp_tab.toUpperCase().contains(".TXT")) {
                                        temp_tab = temp_tab.toUpperCase().replace(".CSV", "").replace(".XLSX", "");
                                    }
                                    if (itemPathAndQueryMap.get(eachMapSpec.getSourceTableName()).contains("ODATA_TAB")) {
//                                        String intermediateSysEnvInfo1 = SyncMetadataJsonFileDesign.newmetasync(temp_tab, "", "", PowerBIReportParser.metadataJsonPath, pbib.getODATA_SYSTEM_NAME(), pbib.getODATA_ENVIRONEMNT_NAME(), PowerBIReportParser.cacheMap, PowerBIReportParser.allTablesMap, PowerBIReportParser.allDBMap, "", "", "");
//                                        newSpec.setSourceSystemName(intermediateSysEnvInfo1.split("##")[0]);
//                                        newSpec.setSourceSystemEnvironmentName(intermediateSysEnvInfo1.split("##")[1]);
//                                        newSpec.setSourceTableName(sourcetableName.toUpperCase().replace(".XLSX", "").replace(".CSV", "").replace(".JSON", "").replace(".XML", ""));
                                    } else {
//                                        String intermediateSysEnvInfo1 = SyncMetadataJsonFileDesign.newmetasync(temp_tab, "", "", PowerBIReportParser.metadataJsonPath, pbib.getSQL_SYSTEM_NAME(), pbib.getSQL_ENVIRONMENT_NAME(), PowerBIReportParser.cacheMap, PowerBIReportParser.allTablesMap, PowerBIReportParser.allDBMap, "", "", "");
//                                        newSpec.setSourceSystemName(intermediateSysEnvInfo1.split("##")[0]);
//                                        newSpec.setSourceSystemEnvironmentName(intermediateSysEnvInfo1.split("##")[1]);
//                                        newSpec.setSourceTableName(sourcetableName.toUpperCase().replace(".XLSX", "").replace(".CSV", "").replace(".JSON", "").replace(".XML", ""));
                                    }
                                }
                                newSpec.setSourceColumnName(sourceColName.split("\\.")[0]);
                                newSpec.setTargetSystemName(sourceSystem);
                                newSpec.setTargetSystemEnvironmentName(srcEnvName);
                                newSpec.setTargetTableName(eachMapSpec.getSourceTableName());
                                newSpec.setTargetColumnName(sourceColName);
//                                updatedMapSpecRow.add(newSpec);
                            }
                        }
                    }
//                    eachMapSpec.setSourceColumnName(sourceColName.replaceAll("[^a-zA-Z0-9_\\s\\.\\-\\#\\(\\)]", ""));
//                    eachMapSpec.setTargetColumnName(targetColName.replaceAll("[^a-zA-Z0-9_\\s\\.\\-\\#\\(\\)]", ""));
                    updatedMapSpecRow.add(eachMapSpec);
                }
            }

        } catch (Exception e) {
            StringWriter exception = new StringWriter();
            e.printStackTrace(new PrintWriter(exception));
            PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Error::Exception In PowerBIReportParser.removeMapSpecDuplicatesForBusinessRule() Method " + exception + "\n");
        }
        return updatedMapSpecRow;
    }

    public static void getExtreamResultSet(ArrayList<MappingSpecificationRow> mapspecs) {
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

    public static ArrayList<MappingSpecificationRow> removeExtraComponent(ArrayList<MappingSpecificationRow> mappingSpecs) {
        ArrayList<MappingSpecificationRow> updatedMappingSpecification = new ArrayList<>();
        try {

            for (MappingSpecificationRow mappingSpec : mappingSpecs) {
                if (mappingSpec.getSourceTableName().contains("EXPANDTABLECOLUMN") || mappingSpec.getSourceTableName().contains("TRANSFORMCOLUMNTYPES") || mappingSpec.getSourceTableName().contains("JSON.DOCUMENT") || (mappingSpec.getSourceTableName().contains("COLUMN") && !mappingSpec.getSourceTableName().contains("DUMMY_ORPHAN_COLUMN"))) {
                    continue;
                } else {
                    if (mappingSpec.getSourceSystemName().equals(pbib.getPOWERBI_SYSTEM()) && mappingSpec.getSourceSystemEnvironmentName().equals(pbib.getPOWERBI_ENV_PAGE())) {
                        mappingSpec.setSourceTableName(mappingSpec.getSourceTableName().replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", ""));
                    } else if (mappingSpec.getTargetSystemName().equals(pbib.getPOWERBI_SYSTEM()) && mappingSpec.getTargetSystemEnvironmentName().equals(pbib.getPOWERBI_ENV_PAGE())) {
                        mappingSpec.setTargetTableName(mappingSpec.getTargetTableName().replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", ""));
                    }
                    updatedMappingSpecification.add(mappingSpec);
                }

            }

        } catch (Exception ex) {
            StringWriter exception = new StringWriter();
            ex.printStackTrace(new PrintWriter(exception));
            PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Error::Exception In PowerBIReportParser.removeMapSpecDuplicatesForBusinessRule() Method " + exception + "\n");
        }
        return updatedMappingSpecification;
    }

    public static ArrayList<MappingSpecificationRow> removeMapSpecDuplicatesForBusinessRule1(ArrayList<MappingSpecificationRow> mapspecs, String displayName) {
        try {
            String businessRule = "";
            String sourcetableName = "";
            String targettableName = "";
            String sourceColumnName = "";
            String targetColumnName = "";
            String allsrctgrDetails = "";
            Set<String> CountersetofSpecs = new HashSet();
            Set<String> CountersetofSpecswithoutBr = new HashSet();
            ArrayList<MappingSpecificationRow> updatedMapsec = new ArrayList();
            for (MappingSpecificationRow eachMapSpec : mapspecs) {
                if (eachMapSpec.getSourceTableName().toUpperCase().contains("SELECT ") || eachMapSpec.getSourceTableName().toUpperCase().contains("FOLDER.FILES") || eachMapSpec.getSourceTableName().toUpperCase().contains(".FILESCUSERS")) {
                    continue;
                }
//                if (eachMapSpec.getSourceSystemName().equals("PBI")) {
//                    eachMapSpec.setSourceSystemName(pbib.getPOWERBI_SYSTEM());
//                } else if (eachMapSpec.getTargetSystemName().equals("PBI")) {
//                    eachMapSpec.setTargetSystemName(pbib.getPOWERBI_SYSTEM());
//                }
                if (!eachMapSpec.getSourceTableName().replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim().toUpperCase().contains("TABLE.SELECTCOLUMNS") && !eachMapSpec.getSourceTableName().replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim().toUpperCase().contains("TABLE.SELECTROWS") && !eachMapSpec.getSourceTableName().replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim().toUpperCase().contains("FILTERED ROWS") && !eachMapSpec.getSourceTableName().replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim().toUpperCase().contains("SPLIT COLUMN BY DELIMITER") && !eachMapSpec.getSourceTableName().replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim().toUpperCase().contains("REPLACED VALUE")) {

                    businessRule = eachMapSpec.getBusinessRule().trim();
                    if (businessRule != null && !businessRule.isEmpty() && businessRule.length() != 0) {
                        sourcetableName = eachMapSpec.getSourceTableName().trim();
                        targettableName = eachMapSpec.getTargetTableName().trim();
                        sourceColumnName = eachMapSpec.getSourceColumnName().trim();
                        targetColumnName = eachMapSpec.getTargetColumnName().trim();

//                        if (!sourceColumnName.trim().contains("DUMMY") && !StringUtils.isBlank(sourceColumnName) && !StringUtils.isBlank(targetColumnName)) {
//                            if (!targetColumnName.trim().contains("DUMMY")) {
                        allsrctgrDetails = sourcetableName + "!Erwin!" + sourceColumnName + "!Erwin!" + targettableName + "!Erwin!" + targetColumnName + "!Erwin!" + businessRule;
                        if (CountersetofSpecs.contains(allsrctgrDetails) == false) {
                            eachMapSpec.setSourceColumnName(sourceColumnName.replace("\"", ""));
                            eachMapSpec.setTargetColumnName(targetColumnName.replace("\"", ""));
                            updatedMapsec.add(eachMapSpec);
                            CountersetofSpecs.add(allsrctgrDetails);
                        }
//                            }
//                        }

                    } else {
                        sourcetableName = eachMapSpec.getSourceTableName().trim();
                        targettableName = eachMapSpec.getTargetTableName().trim();
                        sourceColumnName = eachMapSpec.getSourceColumnName().trim();
                        targetColumnName = eachMapSpec.getTargetColumnName().trim();

//                        if (!sourceColumnName.trim().contains("DUMMY") && !StringUtils.isBlank(sourceColumnName) && !StringUtils.isBlank(targettableName)) {
//                            if (!targetColumnName.trim().contains("DUMMY")) {
                        allsrctgrDetails = sourcetableName + "!Erwin!" + sourceColumnName + "!Erwin!" + targettableName + "!Erwin!" + targetColumnName;
                        if (CountersetofSpecswithoutBr.contains(allsrctgrDetails) == false) {
                            eachMapSpec.setSourceColumnName(sourceColumnName.replace("\"", ""));
                            eachMapSpec.setTargetColumnName(targetColumnName.replace("\"", ""));
                            updatedMapsec.add(eachMapSpec);
                            CountersetofSpecswithoutBr.add(allsrctgrDetails);
                        }
//                            }
//                        }
                    }
                }
            }

            return updatedMapsec;
        } catch (Exception e) {
            logger.error(e.getMessage());
            StringWriter exception = new StringWriter();
            e.printStackTrace(new PrintWriter(exception));
            PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Error::Exception In PowerBIReportParser.removeMapSpecDuplicatesForBusinessRule() Method " + exception + "\n");
            return null;
        }

    }
}
