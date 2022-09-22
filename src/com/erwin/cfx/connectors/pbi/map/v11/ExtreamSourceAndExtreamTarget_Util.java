/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.connectors.pbi.map.v11;

import com.ads.api.beans.mm.MappingSpecificationRow;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.commons.lang.StringUtils;

/**
 *
 * @author Inkollu Reddy
 */
public class ExtreamSourceAndExtreamTarget_Util {

    public static HashMap<String, Set<String>> getExtreamTargetTablesSet(ArrayList<MappingSpecificationRow> specificationList) {

        Set<String> sourceTableSet = new HashSet<>();
        Set<String> targetTableSet = new HashSet<>();
        HashMap<String, Set<String>> sourcetableColumnMap = new HashMap<>();
        HashMap<String, Set<String>> targetTableColumnMap = new HashMap<>();
        HashMap<String, Set<String>> ExtreameSourceTableColumnMap = new HashMap<>();
        HashMap<String, Set<String>> ExtreameTargetTableColumnMap = new HashMap<>();
        try {
            for (MappingSpecificationRow mappingSpecificationRow : specificationList) {
                String sourceTableName = mappingSpecificationRow.getSourceTableName();
                String targetTableName = mappingSpecificationRow.getTargetTableName();

                String sourceTableArray[] = sourceTableName.split("\n");
                for (String sourceTable : sourceTableArray) {
                    if (!StringUtils.isBlank(sourceTable)) {
                        sourceTableSet.add(sourceTable.toUpperCase());
                        if (sourcetableColumnMap.containsKey(sourceTable)) {
                            Set<String> column = sourcetableColumnMap.get(sourceTable);
                            column.add(mappingSpecificationRow.getSourceColumnName());
                            sourcetableColumnMap.put(sourceTable, column);
                        } else {
                            Set<String> column = new HashSet<>();
                            column.add(mappingSpecificationRow.getSourceColumnName());
                            sourcetableColumnMap.put(sourceTable, column);
                        }
                    }

                }
                String targetTableArray[] = targetTableName.split("\n");
                for (String targetTable : targetTableArray) {
                    if (!StringUtils.isBlank(targetTable)) {
                        targetTableSet.add(targetTable.toUpperCase());
                        if (targetTableColumnMap.containsKey(targetTable)) {
                            Set<String> column = targetTableColumnMap.get(targetTable);
                            column.add(mappingSpecificationRow.getTargetColumnName());
                            targetTableColumnMap.put(targetTable, column);
                        } else {
                            Set<String> column = new HashSet<>();
                            column.add(mappingSpecificationRow.getTargetColumnName());
                            targetTableColumnMap.put(targetTable, column);
                        }
                    }

                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        Set<String> extremeSourceSet = getExtremeSource(sourceTableSet, targetTableSet);
        for (Iterator<String> iterator = extremeSourceSet.iterator(); iterator.hasNext();) {
            String next = iterator.next();
            ExtreameSourceTableColumnMap.put(next, sourcetableColumnMap.get(next));
        }
        
        Set<String> extremeTargetSet = getExtremeTarget(sourceTableSet, targetTableSet);
        for (Iterator<String> iterator = extremeSourceSet.iterator(); iterator.hasNext();) {
            String next = iterator.next();
            ExtreameTargetTableColumnMap.put(next, targetTableColumnMap.get(next));
        }
        
        return ExtreameTargetTableColumnMap;
    }

    public static Set<String> getExtremeSource(Set<String> sourceTableSet, Set<String> targetTableSet) {
        Set<String> extremeSourceSet = new HashSet<>();
        try {
            Iterator sourceSetIterator = sourceTableSet.iterator();
            while (sourceSetIterator.hasNext()) {
                String sourceTable = sourceSetIterator.next().toString();
                if (!targetTableSet.contains(sourceTable)) {
                    extremeSourceSet.add(sourceTable);
                }
            }

        } catch (Exception e) {
        }
        return extremeSourceSet;
    }
    
    public static Set<String> getExtremeTarget(Set<String> sourceTableSet, Set<String> targetTableSet) {
        Set<String> extremeSourceSet = new HashSet<>();
        try {
            Iterator targetSetIterator = targetTableSet.iterator();
            while (targetSetIterator.hasNext()) {
                String targetTable = targetSetIterator.next().toString();
                if (!sourceTableSet.contains(targetTable)) {
                    extremeSourceSet.add(targetTable);
                }
            }

        } catch (Exception e) {
        }
        return extremeSourceSet;
    }

    public static Set<String> getExtreamTargetTablesSet1(ArrayList<MappingSpecificationRow> mapSpecs) {
        String srcTbl = "";
        String trtTbl = "";
        Set<String> allSrcTblSet = new HashSet();
        Set<String> allTrtTblSet = new HashSet();
        Set<String> extreamTargetTableSet = new HashSet();
        HashMap<String, Set<String>> targetTableColumnMap = new HashMap<>();
        try {
            for (MappingSpecificationRow mapspec : mapSpecs) {
                srcTbl = mapspec.getSourceTableName();
                trtTbl = mapspec.getTargetTableName();

                String srcTab[] = srcTbl.split("\n");
                for (String sourceTable : srcTab) {
                    allSrcTblSet.add(sourceTable);
                }
                String trtTab[] = trtTbl.split("\n");
                for (String targetTable : trtTab) {
                    
                    if (!StringUtils.isBlank(targetTable)) {
                        extreamTargetTableSet.add(targetTable.toUpperCase());
                        if (targetTableColumnMap.containsKey(targetTable)) {
                            Set<String> column = targetTableColumnMap.get(targetTable);
                            column.add(mapspec.getTargetColumnName());
                            targetTableColumnMap.put(targetTable, column);
                        } else {
                            Set<String> column = new HashSet<>();
                            column.add(mapspec.getTargetColumnName());
                            targetTableColumnMap.put(targetTable, column);
                        }
                    }
                }
            }
            allTrtTblSet.stream().filter((trttbl) -> (!(allSrcTblSet.contains(trttbl)))).forEachOrdered((trttbl) -> {
                extreamTargetTableSet.add(trttbl.toUpperCase());
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
        
//        Set<String> extremeTargetSet = getExtremeTarget(sourceTableSet, targetTableSet);
//        for (Iterator<String> iterator = extremeSourceSet.iterator(); iterator.hasNext();) {
//            String next = iterator.next();
//            ExtreameTargetTableColumnMap.put(next, targetTableColumnMap.get(next));
//        }
        return extreamTargetTableSet;

    }

}
