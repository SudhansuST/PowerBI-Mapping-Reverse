package com.erwin.cfx.connectors.pbi.map.util;

import com.ads.api.beans.mm.MappingSpecificationRow;
import com.erwin.cfx.connectors.pbi.map.pojo.PowerBI_Bean;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author SudhansuTarai
 * @Date 30thAug2022
 */
public class PowerBI_MappingArrangement {

    public static ArrayList<MappingSpecificationRow> SQLArrangement(ArrayList<MappingSpecificationRow> specifications, String logicalTableName, PowerBI_Bean bean) {
        ArrayList<MappingSpecificationRow> updatedSpecifications = new ArrayList<>();
        for (MappingSpecificationRow specification : specifications) {
            String sourceTable = specification.getSourceTableName();
            String targetTable = specification.getTargetTableName();
            if (sourceTable.toUpperCase().contains(bean.getPOWERBI_REPORT_NAME().toUpperCase() + ".")) {
                specification.setSourceSystemName(bean.getPOWERBI_SYSTEM());
                specification.setSourceSystemEnvironmentName(bean.getPOWERBI_ENV_LOGICAL());
            }
            if (targetTable.toUpperCase().contains(bean.getPOWERBI_REPORT_NAME().toUpperCase() + ".")) {
                specification.setTargetSystemName(bean.getPOWERBI_SYSTEM());
                specification.setTargetSystemEnvironmentName(bean.getPOWERBI_ENV_LOGICAL());
            }
            updatedSpecifications.add(specification);
        }
        return updatedSpecifications;
    }

    public static ArrayList<MappingSpecificationRow> removeReportNameAsSchema(ArrayList<MappingSpecificationRow> specifications, String reportName) {

        ArrayList<MappingSpecificationRow> updatedSpecifications = new ArrayList<>();
        for (MappingSpecificationRow specification : specifications) {
            MappingSpecificationRow row = new MappingSpecificationRow();
            String replaceString = reportName + ".";
            String sourceTable = specification.getSourceTableName().toUpperCase().replace(replaceString, "");
            String targetTable = specification.getTargetTableName().toUpperCase().replace(replaceString, "");

            row.setSourceSystemName(specification.getSourceSystemName());
            row.setSourceSystemEnvironmentName(specification.getSourceSystemEnvironmentName());
            row.setSourceTableName(sourceTable);
            row.setSourceColumnName(specification.getSourceColumnName());
            row.setTargetSystemName(specification.getTargetSystemName());
            row.setTargetSystemEnvironmentName(specification.getTargetSystemEnvironmentName());
            row.setTargetTableName(targetTable);
            row.setTargetColumnName(specification.getTargetColumnName());

            row.setBusinessRule(specification.getBusinessRule());
            row.setExtendedBusinessRule(specification.getExtendedBusinessRule());
            updatedSpecifications.add(row);

        }
        return updatedSpecifications;
    }

    public static Map<String, ArrayList<MappingSpecificationRow>> removeDuplicates(Map<String, ArrayList<MappingSpecificationRow>> measureSpecsAgainstLogicalTable, String reportName, boolean isSSAS) {
        Map<String, ArrayList<MappingSpecificationRow>> updatedMap = new HashMap<>();
        for (Map.Entry<String, ArrayList<MappingSpecificationRow>> entry : measureSpecsAgainstLogicalTable.entrySet()) {
            String logicalTableName = entry.getKey();
            ArrayList<MappingSpecificationRow> specifications = entry.getValue();
            ArrayList<MappingSpecificationRow> updatedSpecifications = new ArrayList<>();
            Set<String> uniqueCheck = new HashSet<>();
            for (MappingSpecificationRow specification : specifications) {
                String sourceSystemName = specification.getSourceSystemName().toUpperCase().trim();
                String sourceEnvironmentName = specification.getSourceSystemEnvironmentName().toUpperCase().trim();
                String sourceTableName = specification.getSourceTableName().toUpperCase().trim();
                String sourceColumnName = specification.getSourceColumnName().toUpperCase().trim();
                String targetSystemName = specification.getTargetSystemName().toUpperCase().trim();
                String targetEnvironmentName = specification.getTargetSystemEnvironmentName().toUpperCase().trim();
                String targetTableName = specification.getTargetTableName().toUpperCase().trim();
                String targetColumnName = specification.getTargetColumnName().toUpperCase().trim();

                String businessRule = specification.getBusinessRule().trim();

                String content = sourceSystemName + "@QUEST@" + sourceEnvironmentName + "@QUEST@" + sourceTableName + "@QUEST@" + sourceColumnName + "@QUEST@" + targetSystemName + "@QUEST@" + targetEnvironmentName + "@QUEST@" + targetTableName + "@QUEST@" + targetColumnName + "@QUEST@" + businessRule;
                if (!uniqueCheck.contains(content)) {
                    updatedSpecifications.add(specification);
                    uniqueCheck.add(content);
                }
            }
            updatedMap.put(logicalTableName, updatedSpecifications);
        }
        return updatedMap;
    }

    public static Map<String, ArrayList<MappingSpecificationRow>> removeDuplicates1(Map<String, ArrayList<MappingSpecificationRow>> measureSpecsAgainstLogicalTable, String reportName, boolean isSSAS) {
        Map<String, ArrayList<MappingSpecificationRow>> updatedMap = new HashMap<>();
        for (Map.Entry<String, ArrayList<MappingSpecificationRow>> entry : measureSpecsAgainstLogicalTable.entrySet()) {
            String logicalTableName = entry.getKey();
            ArrayList<MappingSpecificationRow> specifications = entry.getValue();
            ArrayList<MappingSpecificationRow> updatedSpecifications = new ArrayList<>();
            Set<String> uniqueCheck = new HashSet<>();
            for (MappingSpecificationRow specification : specifications) {
                String sourceSystemName = specification.getSourceSystemName().toUpperCase().trim();
                String sourceEnvironmentName = specification.getSourceSystemEnvironmentName().toUpperCase().trim();
                String sourceTableName = specification.getSourceTableName().toUpperCase().trim();
                String sourceColumnName = specification.getSourceColumnName().toUpperCase().trim();
                String targetSystemName = specification.getTargetSystemName().toUpperCase().trim();
                String targetEnvironmentName = specification.getTargetSystemEnvironmentName().toUpperCase().trim();
                String targetTableName = specification.getTargetTableName().toUpperCase().trim();
                String targetColumnName = specification.getTargetColumnName().toUpperCase().trim();

                String businessRule = specification.getBusinessRule().trim();

                String content = sourceSystemName + "@QUEST@" + sourceEnvironmentName + "@QUEST@" + sourceTableName + "@QUEST@" + sourceColumnName + "@QUEST@" + targetSystemName + "@QUEST@" + targetEnvironmentName + "@QUEST@" + targetTableName + "@QUEST@" + targetColumnName + "@QUEST@" + businessRule;
                if (!uniqueCheck.contains(content)) {
                    updatedSpecifications.add(specification);
                    uniqueCheck.add(content);
                }
            }
            updatedMap.put(logicalTableName, updatedSpecifications);
        }
        return updatedMap;
    }

    public static ArrayList<MappingSpecificationRow> removeDuplicateSpecification(ArrayList<MappingSpecificationRow> specifications, String reportName) {
        ArrayList<MappingSpecificationRow> updatedSpecifications = new ArrayList<>();
        for (MappingSpecificationRow specification : specifications) {

            String sourceTableName = specification.getSourceTableName().trim();
            String targetTableName = specification.getTargetTableName().trim();
            String sourceColumnName = specification.getSourceColumnName().trim();
            String targetColumnName = specification.getTargetColumnName().trim();

            if (sourceColumnName.contains("RowNumber")) {
                continue;
            }

            if (targetColumnName.contains("RowNumber")) {
                continue;
            }

            if (specification.getSourceTableName().replace(reportName, "").trim().equals(".-")) {
                continue;
            }

            if (specification.getSourceTableName().toUpperCase().contains("FOLDER.FILES") || specification.getSourceTableName().toUpperCase().contains(".FILESCUSERS")) {
                continue;
            }

            if (specification.getSourceTableName().split("\\.").length == 2 && specification.getSourceTableName().split("\\.")[1].indexOf("-") == 0) {
                String tab_temp = specification.getSourceTableName().split("\\.")[0].toUpperCase() + "." + specification.getSourceTableName().split("\\.")[1].substring(1);
                specification.setSourceTableName(tab_temp);
            }

            if (!specification.getSourceTableName().replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim().toUpperCase().contains("REMOVED DUPLICATES") && !specification.getSourceTableName().replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim().toUpperCase().contains("CHANGED TYPE") && !specification.getSourceTableName().replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim().toUpperCase().contains("TABLE.SELECTCOLUMNS") && !specification.getSourceTableName().replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim().toUpperCase().contains("ODATA.FEED") && !specification.getSourceTableName().replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim().toUpperCase().contains("TABLE.SELECTROWS") && !specification.getSourceTableName().replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim().toUpperCase().contains("FILTERED ROWS") && !specification.getSourceTableName().replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim().toUpperCase().contains("SPLIT COLUMN BY DELIMITER") && !specification.getSourceTableName().replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim().toUpperCase().contains("TABLE.TRANSFORMCOLUMNS") && !specification.getSourceTableName().replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim().toUpperCase().contains(".EXPANDED ") && !specification.getSourceTableName().replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim().toUpperCase().contains("RENAMED COLUMNS") && !specification.getSourceTableName().replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim().toUpperCase().contains("ADDED CUSTOM") && !specification.getSourceTableName().replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim().toUpperCase().contains("DUPLICATED COLUMN") && !specification.getSourceTableName().replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim().toUpperCase().contains("MERGED ") && !specification.getSourceTableName().replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim().toUpperCase().contains("REMOVED COLUMNS") && !specification.getSourceTableName().replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim().toUpperCase().contains("REORDERED COLUMNS") && !specification.getSourceTableName().replaceAll("[^a-zA-Z0-9_\\s\\.\\-]", "").trim().toUpperCase().contains("NEWCOLUMN")) {

                specification.setSourceTableName(specification.getSourceTableName().replace("\"", ""));
                if (!targetTableName.toUpperCase().trim().equals(reportName.toUpperCase().trim() + "." + reportName.toUpperCase().trim()) && !sourceTableName.toUpperCase().trim().equals(reportName.toUpperCase().trim() + "." + reportName.toUpperCase().trim())) {
                    specification.setSourceTableName(specification.getSourceTableName().replace(reportName.toUpperCase() + "." + reportName.toUpperCase() + ".", reportName.toUpperCase() + "."));
                }
                
                

            }
        }

        return updatedSpecifications;
    }
}
