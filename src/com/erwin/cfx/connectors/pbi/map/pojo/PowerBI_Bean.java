/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.connectors.pbi.map.pojo;

import com.ads.api.util.KeyValueUtil;
import com.ads.api.util.MappingManagerUtil;
import com.ads.api.util.SystemManagerUtil;
import com.erwin.cfx.connectors.pbi.map.sql.util.PowerBI_DataflowUtil;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author SudhansuTarai
 */
public class PowerBI_Bean {

    private String SQL_SYSTEM_NAME;
    private String SQL_ENVIRONMENT_NAME;
    private String SSAS_SYSTEM_NAME;
    private String SSAS_ENVIRONMENT_NAME;
    private String POWERBI_SYSTEM;
    private String POWERBI_ENV_PAGE;
    private String POWERBI_ENV_DASHBOARD;
    private String POWERBI_ENV_LOGICAL;
    private String POWERBI_DATASET_ID;
    private String ODATA_SYSTEM_NAME;
    private String ODATA_ENVIRONEMNT_NAME;
    private String PROJECT_ID;
    private String DEFAULT_SCHEMA;
    private String PHYSICAL_SUBJECT;
    private int PARENT_SUBJECT_ID;
    private String POWERBI_REPORT_NAME;
    private String STORED_PROC_SYSTEM_NAME;
    private String STORED_PROC_ENVIRONMENT_EXTENSION;
    private boolean isCreatePageEnvironment;
    private boolean isCreateLogicalEnvironment;
    private boolean isCreateDashboardEnvironment;
    private boolean hiddenPageInfo;
    private String MAP_RELOAD_TYPE;
    private Map<String, String> datasetNameAgainstID;
    private SystemManagerUtil systemManagerUtil;
    private MappingManagerUtil mappingManagerUtil;
    private KeyValueUtil keyValueUtil;

    private Map<String, String> sqlQuery;
    private Map<String, String> dbTypes;
    private HashMap<String, String> allDBMap;
    private HashMap<String, String> allTablesMap;
    private String metadataJsonPath;
    private Map<String, Map<String, Map<String, Set<String>>>> metadataMap;
    private Map<String, Set<String>> logicalMap;
    private PowerBI_DataflowUtil dataflowUtil;

    private Map<String, Set<String>> powerBIDashboardColumns;
    private Map<String, Map<String, Set<String>>> powerBIPageColumns;
    private Map<String, Map<String, Map<String, Set<String>>>> powerBILogicalTableColumns;
    private Map<String, String> expressionAgainstColumnMap = new HashMap<>();
    private Map<String, String> datatypeAgainstColumnMap = new HashMap<>();
    private Map<String, String> expressionAgainstMeasureMap = new HashMap<>();
    private boolean isSSAS;
    private Map<String, Map<String, String>> REPORT_expressionAgainstColumnMap = new HashMap<>();
    private Map<String, Map<String, String>> REPORT_datatypeAgainstColumnMap = new HashMap<>();
    private Map<String, Map<String, String>> REPORT_expressionAgainstMeasureMap = new HashMap<>();
    private Map<String, Map<String, String>> REPORT_descriptionAgainstColumnMap = new HashMap<>();

    public KeyValueUtil getKeyValueUtil() {
        return keyValueUtil;
    }

    public void setKeyValueUtil(KeyValueUtil keyValueUtil) {
        this.keyValueUtil = keyValueUtil;
    }

    public SystemManagerUtil getSystemManagerUtil() {
        return systemManagerUtil;
    }

    public void setSystemManagerUtil(SystemManagerUtil systemManagerUtil) {
        this.systemManagerUtil = systemManagerUtil;
    }

    public MappingManagerUtil getMappingManagerUtil() {
        return mappingManagerUtil;
    }

    public void setMappingManagerUtil(MappingManagerUtil mappingManagerUtil) {
        this.mappingManagerUtil = mappingManagerUtil;
    }

    public String getPOWERBI_DATASET_ID() {
        return POWERBI_DATASET_ID;
    }

    public void setPOWERBI_DATASET_ID(String POWERBI_DATASET_ID) {
        this.POWERBI_DATASET_ID = POWERBI_DATASET_ID;
    }

    public String getSQL_SYSTEM_NAME() {
        return SQL_SYSTEM_NAME;
    }

    public void setSQL_SYSTEM_NAME(String SQL_SYSTEM_NAME) {
        this.SQL_SYSTEM_NAME = SQL_SYSTEM_NAME;
    }

    public String getSQL_ENVIRONMENT_NAME() {
        return SQL_ENVIRONMENT_NAME;
    }

    public void setSQL_ENVIRONMENT_NAME(String SQL_ENVIRONMENT_NAME) {
        this.SQL_ENVIRONMENT_NAME = SQL_ENVIRONMENT_NAME;
    }

    public String getPOWERBI_SYSTEM() {
        return POWERBI_SYSTEM;
    }

    public void setPOWERBI_SYSTEM(String POWERBI_SYSTEM) {
        this.POWERBI_SYSTEM = POWERBI_SYSTEM;
    }

    public String getPOWERBI_ENV_PAGE() {
        return POWERBI_ENV_PAGE;
    }

    public void setPOWERBI_ENV_PAGE(String POWERBI_ENV_PAGE) {
        this.POWERBI_ENV_PAGE = POWERBI_ENV_PAGE;
    }

    public String getPOWERBI_ENV_DASHBOARD() {
        return POWERBI_ENV_DASHBOARD;
    }

    public void setPOWERBI_ENV_DASHBOARD(String POWERBI_ENV_DASHBOARD) {
        this.POWERBI_ENV_DASHBOARD = POWERBI_ENV_DASHBOARD;
    }

    public Map<String, String> getDatasetNameAgainstID() {
        return datasetNameAgainstID;
    }

    public void setDatasetNameAgainstID(Map<String, String> datasetNameAgainstID) {
        this.datasetNameAgainstID = datasetNameAgainstID;
    }

    public String getODATA_SYSTEM_NAME() {
        return ODATA_SYSTEM_NAME;
    }

    public void setODATA_SYSTEM_NAME(String ODATA_SYSTEM_NAME) {
        this.ODATA_SYSTEM_NAME = ODATA_SYSTEM_NAME;
    }

    public String getODATA_ENVIRONEMNT_NAME() {
        return ODATA_ENVIRONEMNT_NAME;
    }

    public void setODATA_ENVIRONEMNT_NAME(String ODATA_ENVIRONEMNT_NAME) {
        this.ODATA_ENVIRONEMNT_NAME = ODATA_ENVIRONEMNT_NAME;
    }

    public String getPROJECT_ID() {
        return PROJECT_ID;
    }

    public void setPROJECT_ID(String PROJECT_ID) {
        this.PROJECT_ID = PROJECT_ID;
    }

    public void setCheckLogicalEnvironmentCreation(boolean isCreateLogicalEnvironment) {
        this.isCreateLogicalEnvironment = isCreateLogicalEnvironment;
    }

    public void setCheckPageEnvironmentCreation(boolean isCreatePageEnvironment) {
        this.isCreatePageEnvironment = isCreatePageEnvironment;
    }

    public void setCheckDashboardEnvironmentCreation(boolean isCreateDashboardEnvironment) {
        this.isCreateDashboardEnvironment = isCreateDashboardEnvironment;
    }

    public boolean isLogicalEnvironmentCreate() {
        return this.isCreateLogicalEnvironment;
    }

    public boolean isDashboardEnvironmentCreate() {
        return this.isCreateDashboardEnvironment;
    }

    public boolean isPageEnvironmentCreate() {
        return this.isCreatePageEnvironment;
    }

    public Map<String, String> getSqlQuery() {
        return sqlQuery;
    }

    public void setSqlQuery(Map<String, String> sqlQuery) {
        this.sqlQuery = sqlQuery;
    }

    public Map<String, String> getDbTypes() {
        return dbTypes;
    }

    public void setDbTypes(Map<String, String> dbTypes) {
        this.dbTypes = dbTypes;
    }

    public HashMap<String, String> getAllDBMap() {
        return allDBMap;
    }

    public void setAllDBMap(HashMap<String, String> allDBMap) {
        this.allDBMap = allDBMap;
    }

    public HashMap<String, String> getAllTablesMap() {
        return allTablesMap;
    }

    public void setAllTablesMap(HashMap<String, String> allTablesMap) {
        this.allTablesMap = allTablesMap;
    }

    public String getMetadataJsonPath() {
        return metadataJsonPath;
    }

    public void setMetadataJsonPath(String metadataJsonPath) {
        this.metadataJsonPath = metadataJsonPath;
    }

    public Map<String, Map<String, Map<String, Set<String>>>> getMetadataMap() {
        return metadataMap;
    }

    public void setMetadataMap(Map<String, Map<String, Map<String, Set<String>>>> metadataMap) {
        this.metadataMap = metadataMap;
    }

    public String getDEFAULT_SCHEMA() {
        return DEFAULT_SCHEMA;
    }

    public void setDEFAULT_SCHEMA(String DEFAULT_SCHEMA) {
        this.DEFAULT_SCHEMA = DEFAULT_SCHEMA;
    }

    public String getPHYSICAL_SUBJECT() {
        return PHYSICAL_SUBJECT;
    }

    public void setPHYSICAL_SUBJECT(String PHYSICAL_SUBJECT) {
        this.PHYSICAL_SUBJECT = PHYSICAL_SUBJECT;
    }

    public int getPARENT_SUBJECT_ID() {
        return PARENT_SUBJECT_ID;
    }

    public void setPARENT_SUBJECT_ID(int PARENT_SUBJECT_ID) {
        this.PARENT_SUBJECT_ID = PARENT_SUBJECT_ID;
    }

    public String getMAP_RELOAD_TYPE() {
        return MAP_RELOAD_TYPE;
    }

    public void setMAP_RELOAD_TYPE(String MAP_RELOAD_TYPE) {
        this.MAP_RELOAD_TYPE = MAP_RELOAD_TYPE;
    }

    public String getPOWERBI_REPORT_NAME() {
        return POWERBI_REPORT_NAME;
    }

    public void setPOWERBI_REPORT_NAME(String POWERBI_REPORT_NAME) {
        this.POWERBI_REPORT_NAME = POWERBI_REPORT_NAME;
    }

    public String getSTORED_PROC_SYSTEM_NAME() {
        return STORED_PROC_SYSTEM_NAME;
    }

    public void setSTORED_PROC_SYSTEM_NAME(String STORED_PROC_SYSTEM_NAME) {
        this.STORED_PROC_SYSTEM_NAME = STORED_PROC_SYSTEM_NAME;
    }

    public String getSTORED_PROC_ENVIRONMENT_EXTENSION() {
        return STORED_PROC_ENVIRONMENT_EXTENSION;
    }

    public void setSTORED_PROC_ENVIRONMENT_EXTENSION(String STORED_PROC_ENVIRONMENT_EXTENSION) {
        this.STORED_PROC_ENVIRONMENT_EXTENSION = STORED_PROC_ENVIRONMENT_EXTENSION;
    }

    public void setHiddenPageInfo(boolean hiddenPageInfo) {
        this.hiddenPageInfo = hiddenPageInfo;
    }

    public boolean isCreateHiddenPage() {
        return this.hiddenPageInfo;
    }

    public Map<String, Set<String>> getLogicalMap() {
        return logicalMap;
    }

    public void setLogicalMap(Map<String, Set<String>> logicalMap) {
        this.logicalMap = logicalMap;
    }

    public PowerBI_DataflowUtil getDataflowUtil() {
        return dataflowUtil;
    }

    public void setDataflowUtil(PowerBI_DataflowUtil dataflowUtil) {
        this.dataflowUtil = dataflowUtil;
    }

    public Map<String, Set<String>> getPowerBIDashboardColumns() {
        return powerBIDashboardColumns;
    }

    public void setPowerBIDashboardColumns(Map<String, Set<String>> powerBIDashboardColumns) {
        this.powerBIDashboardColumns = powerBIDashboardColumns;
    }

    public Map<String, Map<String, Set<String>>> getPowerBIPageColumns() {
        return powerBIPageColumns;
    }

    public void setPowerBIPageColumns(Map<String, Map<String, Set<String>>> powerBIPageColumns) {
        this.powerBIPageColumns = powerBIPageColumns;
    }

    public Map<String, Map<String, Map<String, Set<String>>>> getPowerBILogicalTableColumns() {
        return powerBILogicalTableColumns;
    }

    public void setPowerBILogicalTableColumns(Map<String, Map<String, Map<String, Set<String>>>> powerBILogicalTableColumns) {
        this.powerBILogicalTableColumns = powerBILogicalTableColumns;
    }

    public String getSSAS_SYSTEM_NAME() {
        return SSAS_SYSTEM_NAME;
    }

    public void setSSAS_SYSTEM_NAME(String SSAS_SYSTEM_NAME) {
        this.SSAS_SYSTEM_NAME = SSAS_SYSTEM_NAME;
    }

    public String getSSAS_ENVIRONMENT_NAME() {
        return SSAS_ENVIRONMENT_NAME;
    }

    public void setSSAS_ENVIRONMENT_NAME(String SSAS_ENVIRONMENT_NAME) {
        this.SSAS_ENVIRONMENT_NAME = SSAS_ENVIRONMENT_NAME;
    }

    public String getPOWERBI_ENV_LOGICAL() {
        return POWERBI_ENV_LOGICAL;
    }

    public void setPOWERBI_ENV_LOGICAL(String POWERBI_ENV_LOGICAL) {
        this.POWERBI_ENV_LOGICAL = POWERBI_ENV_LOGICAL;
    }

    public Map<String, String> getExpressionAgainstColumnMap() {
        return expressionAgainstColumnMap;
    }

    public void setExpressionAgainstColumnMap(Map<String, String> expressionAgainstColumnMap) {
        this.expressionAgainstColumnMap = expressionAgainstColumnMap;
    }

    public Map<String, String> getDatatypeAgainstColumnMap() {
        return datatypeAgainstColumnMap;
    }

    public void setDatatypeAgainstColumnMap(Map<String, String> datatypeAgainstColumnMap) {
        this.datatypeAgainstColumnMap = datatypeAgainstColumnMap;
    }

    public Map<String, String> getExpressionAgainstMeasureMap() {
        return expressionAgainstMeasureMap;
    }

    public void setExpressionAgainstMeasureMap(Map<String, String> expressionAgainstMeasureMap) {
        this.expressionAgainstMeasureMap = expressionAgainstMeasureMap;
    }

    public boolean isSSAS() {
        return isSSAS;
    }

    public void setIsSSAS(boolean isSSAS) {
        this.isSSAS = isSSAS;
    }

    public Map<String, Map<String, String>> getREPORT_expressionAgainstColumnMap() {
        return REPORT_expressionAgainstColumnMap;
    }

    public void setREPORT_expressionAgainstColumnMap(Map<String, Map<String, String>> REPORT_expressionAgainstColumnMap) {
        this.REPORT_expressionAgainstColumnMap = REPORT_expressionAgainstColumnMap;
    }

    public Map<String, Map<String, String>> getREPORT_datatypeAgainstColumnMap() {
        return REPORT_datatypeAgainstColumnMap;
    }

    public void setREPORT_datatypeAgainstColumnMap(Map<String, Map<String, String>> REPORT_datatypeAgainstColumnMap) {
        this.REPORT_datatypeAgainstColumnMap = REPORT_datatypeAgainstColumnMap;
    }

    public Map<String, Map<String, String>> getREPORT_expressionAgainstMeasureMap() {
        return REPORT_expressionAgainstMeasureMap;
    }

    public void setREPORT_expressionAgainstMeasureMap(Map<String, Map<String, String>> REPORT_expressionAgainstMeasureMap) {
        this.REPORT_expressionAgainstMeasureMap = REPORT_expressionAgainstMeasureMap;
    }

    public Map<String, Map<String, String>> getREPORT_descriptionAgainstColumnMap() {
        return REPORT_descriptionAgainstColumnMap;
    }

    public void setREPORT_descriptionAgainstColumnMap(Map<String, Map<String, String>> REPORT_descriptionAgainstColumnMap) {
        this.REPORT_descriptionAgainstColumnMap = REPORT_descriptionAgainstColumnMap;
    }

    
}
