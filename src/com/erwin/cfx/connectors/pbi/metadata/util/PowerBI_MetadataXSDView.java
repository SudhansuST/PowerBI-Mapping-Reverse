package com.erwin.cfx.connectors.pbi.metadata.util;
//
//import com.ads.api.beans.common.Node;
//import com.ads.api.beans.mm.Mapping;
//import com.ads.api.beans.mm.Subject;
//import com.ads.api.beans.sm.SMColumn;
//import com.ads.api.beans.sm.SMEnvironment;
//import com.ads.api.beans.sm.SMSystem;
//import com.ads.api.beans.sm.SMTable;
//import com.erwin.cfx.connectors.pbi.map.pojo.PowerBI_Bean;
//import java.io.PrintWriter;
//import java.io.StringWriter;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//import org.apache.log4j.Logger;
//
///**
// * This class will generate metadata according to the design approved on
// * 8thJUN2022 This will generate hierarchical tables Design
// * ----------------------------------------------- -- Dashboard Name (Main
// * Table) ---- Columns for Dashboard ---- Page Name (Table created under
// * dashboard) --------- Columns for Page --------- Logical Table Name (Table
// * created under page) --------------- Columns for Logical Tables
// *
// * Date - 12thAUG2022
// *
// * @author SudhansuSekharTarai Erwin DI Suit v12.0.4 Java v8
// *
// */
public class PowerBI_MetadataXSDView {
//
//    private PowerBI_Bean bean = new PowerBI_Bean();
//    private StringBuilder status = new StringBuilder();
//    private int powerBI_SystemID = -1;
//    private int powerBI_EnvironmentID = -1;
//    Logger logger = Logger.getLogger(PowerBI_MetadataXSDView.class);
//
//    public void createMetadata(PowerBI_Bean bean) {
//        this.bean = bean;
//        getPowerBISystem();
//        getPowerBIXSDEnvironment();
//        Map<String, Set<String>> powerBIDashboardColumns = bean.getPowerBIDashboardColumns();
//        Map<String, Map<String, Set<String>>> powerBIPageColumns = bean.getPowerBIPageColumns();
//        Map<String, Map<String, Map<String, Set<String>>>> powerBILogicalTableColumns = bean.getPowerBILogicalTableColumns();
//        for (Map.Entry<String, Set<String>> entry : powerBIDashboardColumns.entrySet()) {
//            String dashboardName = entry.getKey();
//            Set<String> dashboardColumns = entry.getValue();
//            int dashBoardTableID = createXSDTable(dashboardName, 0, true);
//            addColumns(dashboardColumns, dashBoardTableID);
//            Map<String, Set<String>> pagesColumns = powerBIPageColumns.get(dashboardName);
//            for (Map.Entry<String, Set<String>> entry1 : pagesColumns.entrySet()) {
//                String pageName = entry1.getKey();
//                Set<String> pageColumns = entry1.getValue();
//                int pageTableId = createXSDTable(pageName, dashBoardTableID, false);
//                addColumns(pageColumns, pageTableId);
//                Map<String, Set<String>> logicalTablesColumns = powerBILogicalTableColumns.get(dashboardName).get(pageName);
//                for (Map.Entry<String, Set<String>> entry2 : logicalTablesColumns.entrySet()) {
//                    String logicalTableName = entry2.getKey();
//                    Set<String> logicalColumns = entry2.getValue();
//                    createXSDTable(logicalTableName, pageTableId, false);
//                    addColumns(logicalColumns, pageTableId);
//                }
//            }
//        }
//
//    }
//
//    private void getPowerBISystem() {
//        try {
//            powerBI_SystemID = bean.getSystemManagerUtil().getSystemId(bean.getPOWERBI_SYSTEM());
//            if (powerBI_SystemID < 0) {
//                SMSystem system = new SMSystem();
//                system.setSystemName(bean.getPOWERBI_SYSTEM());
//                String systemCreattionStatus = bean.getSystemManagerUtil().createSystem(system).getStatusMessage();
//                status.append(systemCreattionStatus + "\n");
//                powerBI_SystemID = bean.getSystemManagerUtil().getSystemId(bean.getPOWERBI_SYSTEM());
//            }
//        } catch (Exception ex) {
//            StringWriter exception = new StringWriter();
//            ex.printStackTrace(new PrintWriter(exception));
//            logger.info("");
//        }
//    }
//
//    private void getPowerBIXSDEnvironment() {
//        try {
//            powerBI_EnvironmentID = bean.getSystemManagerUtil().getEnvironmentId(powerBI_SystemID, bean.getPOWERBI_ENV_PAGE());
//        } catch (Exception e) {
//
//        }
//        if (powerBI_EnvironmentID < 1) {
//            try {
//                SMEnvironment environment = new SMEnvironment();
//                environment.setName(bean.getPOWERBI_ENVIRONMENT_NAME());
//                environment.setDatabaseType("XSD");
//                environment.setEnvironmentType(SMEnvironment.DatabaseType.XSD);
//                environment.setSystemId(powerBI_SystemID);
//                String environmentCreationStatus = bean.getSystemManagerUtil().createEnvironment(environment).getStatusMessage();
//                status.append(environmentCreationStatus + "\n");
//                powerBI_EnvironmentID = bean.getSystemManagerUtil().getEnvironmentId(powerBI_SystemID, bean.getPOWERBI_ENVIRONMENT_NAME());
//            } catch (Exception e) {
//            }
//        }
//    }
//
//    private int createXSDTable(String tableName, int parentTableID, boolean isRootTable) {
//        int tableID = -1;
//        try {
//            SMTable smTable = new SMTable();
//            smTable.setEnvironmentId(powerBI_EnvironmentID);
//            smTable.setTableName(tableName);
//            smTable.setTableType(SMTable.SMTableType.TABLE);
//            smTable.setRootTable(true);
//            if (parentTableID > 0) {
//                SMTable parentTable = bean.getSystemManagerUtil().getTable(parentTableID);
//                smTable.setParentTable(parentTable);
//            }
//            smTable.setParentTable(smTable);
//            String powerBI_TableCreationStatus = bean.getSystemManagerUtil().createTable(smTable).getStatusMessage();
//            tableID = bean.getSystemManagerUtil().getTableId(powerBI_EnvironmentID, tableName);
//            status.append(powerBI_TableCreationStatus + "\n");
//        } catch (Exception ex) {
//
//        }
//
//        return tableID;
//
//    }
//
//    private void addColumns(Set<String> columns, int tableId) {
//        for (String columnName : columns) {
//            SMColumn column = new SMColumn();
//            column.setColumnName(columnName);
//            column.setTableId(tableId);
//            column.setColumnDatatype("");
//            bean.getSystemManagerUtil().createColumn(column);
//        }
//
//    }
//
//    public void deleteEmptySubject(int projectId) {
//        try {
//            List<Subject> subjects = bean.getMappingManagerUtil().getSubjects(projectId);
//            for (Subject subject : subjects) {
//                List<Mapping> mappings = bean.getMappingManagerUtil().getMappings(subject.getSubjectId(), Node.NodeType.MM_SUBJECT);
//                if (mappings.size() == 0) {
//                    bean.getMappingManagerUtil().deleteSubject(subject.getSubjectId());
//                }
//            }
//
//        } catch (Exception ex) {
//
//        }
//    }
//
}
