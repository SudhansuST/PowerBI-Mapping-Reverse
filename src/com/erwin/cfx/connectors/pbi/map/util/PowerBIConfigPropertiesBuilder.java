/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.connectors.pbi.map.util;

import static com.erwin.cfx.connectors.pbi.map.v11.PowerBIReportParser.exceptionLog;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 *
 * @author Toshiba
 */
public class PowerBIConfigPropertiesBuilder {

    public static Map<String, String> mapOfActualTableAgainstQuery = new HashMap<>();
    public static String transforms = "";
    public static Map<String, Set<String>> extendedPropeties = new HashMap<>();
    public static Map<String, Map<String, Set<String>>> extendedPropetiesForWhere = new HashMap<>();
    public static Logger logger = Logger.getLogger(PowerBIConfigPropertiesBuilder.class);
    public static Map<String, String> businessRuleMap = new HashMap<>();
    public static Map<String, String> actualSourceMap = new HashMap<>();
    public static Map<String, String> fromSourceMap = new HashMap<String, String>();
    public static Map<String, String> actualTableAgainstEntity = new HashMap<>();

    /**
     * This method extract the Table and column participated in PowerBI
     * configuration
     *
     * @param configJsonObj
     * @param pbixName
     * @param visualContainer_i
     * @param displayName
     * @return returns map of tables and columns which participated in PowerBI
     *
     *
     *
     */
    public static Map<String, Map<String, Set<String>>> getPowerBIReportConfigInfo(JSONObject configJsonObj, String pbixName, int visualContainer_i, String displayName) {

        Map<String, Map<String, Set<String>>> eachVisualContainser = new HashMap();
        StringBuffer sourceTableSb = null;
        StringBuffer sourceTableColSb = null;
        JSONObject orderByJsonObject = null;
        JSONObject prototypeQueryJson = null;
        JSONObject objectfilterJson = null;
        JSONArray orderByJsonObjectArr = null;
        JSONArray selectJsonArr = null;
        String report = pbixName;
        String fromJson = "";
        String propertyName = "";
        String sourceJSONObject = "";
        String entityName = "";
        String aliasName = "";
        String reportName = "";
        String orderByClause = "";
        String selectJson = "";
        String generalJson = "";

        try {
            //"singleVisual" HAS Complete information about particular Report
            if (configJsonObj.has("singleVisual")) {
                JSONObject singleVisualJson = (JSONObject) configJsonObj.get("singleVisual");
                Map<String, Set<String>> tableLevelMap = new HashMap();
                Set<String> colLevel = new HashSet<>();

                // "title" gives Report Name if its a Report Type.
                if (singleVisualJson.has("title")) {
                    JSONObject titleJson = (JSONObject) singleVisualJson.get("title");
                    if (titleJson.has("text")) {
                        reportName = titleJson.get("text").toString();
                        pbixName = pbixName + "." + reportName;
                    }

                } else {
                    if (singleVisualJson.has("vcObjects")) {
                        JSONObject vcObjectsJson = (JSONObject) singleVisualJson.get("vcObjects");
                        if (vcObjectsJson.has("title")) {
                            JSONArray titleJsonArray = (JSONArray) vcObjectsJson.get("title");

                            for (int i = 0; i < titleJsonArray.length(); i++) {
                                JSONObject titleJSON = titleJsonArray.getJSONObject(i);
                                if (titleJSON.has("properties")) {
                                    if (titleJSON.getJSONObject("properties").has("text")) {
                                        if (titleJSON.getJSONObject("properties").getJSONObject("text").has("expr")) {
                                            if (titleJSON.getJSONObject("properties").getJSONObject("text").getJSONObject("expr").has("Literal")) {
                                                JSONObject reportNameJSON = titleJSON.getJSONObject("properties").getJSONObject("text").getJSONObject("expr").getJSONObject("Literal");
                                                if (reportNameJSON.getString("Value").equals("") || reportNameJSON.getString("Value").equals("''")) {
                                                    reportName = pbixName + "." + displayName + "_" + visualContainer_i;
                                                } else {
                                                    reportName = pbixName + "." + reportNameJSON.getString("Value");
                                                }
                                            } else {
                                                reportName = pbixName + "." + displayName + "_" + visualContainer_i;
                                            }
                                        } else {
                                            reportName = pbixName + "." + displayName + "_" + visualContainer_i;
                                        }
                                    } else {
                                        reportName = pbixName + "." + displayName + "_" + visualContainer_i;
                                    }
                                } else {
                                    reportName = pbixName + "." + displayName + "_" + visualContainer_i;
                                }

                            }
//                    reportName = titleJson.get("title").toString();
//                    reportName = titleJson.get("text").toString();
                            pbixName = reportName.replaceAll("[^a-zA-z0-9\\s!@#$%&()_.+={}~-]", "");
                        } else {
                            pbixName = pbixName + "." + displayName + "_" + visualContainer_i;
                        }

                    } else {
                        pbixName = pbixName + "." + displayName + "_" + visualContainer_i;
                    }
                }
                // newly added chirishma 101120
                if (singleVisualJson.has("objects")) {
                    objectfilterJson = (JSONObject) singleVisualJson.get("objects");
                    // using to get filter and where conditions
                    if (objectfilterJson.has("general")) {
                        generalJson = objectfilterJson.get("general").toString();
                        JSONArray generalJsonArr = new JSONArray(generalJson);
                        for (int gj = 0; gj < generalJsonArr.length(); gj++) {
                            JSONObject propJsonObject = (JSONObject) generalJsonArr.get(gj);
                            if (propJsonObject.has("properties")) {
                                JSONObject proJsonObject = (JSONObject) propJsonObject.get("properties");
                                if (proJsonObject.has("filter")) {
                                    if (proJsonObject.getJSONObject("filter").has("filter")) {
                                        if (proJsonObject.getJSONObject("filter").getJSONObject("filter").has("Where")) {
                                            Map<String, Set<String>> reportDEtailsMaap = PowerBIFiltersPropertiesBuilder.parseWhereCondition(proJsonObject.getJSONObject("filter").getJSONObject("filter"));
                                            if (reportDEtailsMaap != null) {
                                                manageWhereCondition(reportDEtailsMaap);
                                            }
                                        }
                                    }
                                }
                            }
                        }

                    }

                }
                // "Projections" has the projective information about the Report.

                if (singleVisualJson.has("prototypeQuery")) {
                    prototypeQueryJson = (JSONObject) singleVisualJson.get("prototypeQuery");

                    // OrderBy Clause to GET the Columns and BusinessRule Details
                    if (prototypeQueryJson.has("From")) {
                        sourceTableSb = new StringBuffer();
                        fromJson = prototypeQueryJson.get("From").toString();
                        JSONArray fromJsonArr = new JSONArray(fromJson);
                        for (int table_i = 0; table_i < fromJsonArr.length(); table_i++) {
                            JSONObject tableJsonObject = (JSONObject) fromJsonArr.get(table_i);
                            entityName = tableJsonObject.get("Entity").toString().replaceAll("\\(\\d+\\)", "").trim();
                            aliasName = tableJsonObject.getString("Name");
                            fromSourceMap.put(aliasName, entityName);
                            sourceTableSb.append(entityName).append(",");
                        }
                    }

                    // From Clause to GET the Entity Details
                    // Select Clause to GET the Columns and BusinessRule Details
                    if (prototypeQueryJson.has("Select")) {
                        selectJson = prototypeQueryJson.get("Select").toString();
                        selectJsonArr = new JSONArray(selectJson);
                        sourceTableColSb = new StringBuffer();
                        for (int select_i = 0; select_i < selectJsonArr.length(); select_i++) {
                            JSONObject selectJsonObject = (JSONObject) selectJsonArr.get(select_i);
                            String selectExpProperty = "";
                            String selectColumnName = "";
                            String aggregationType = "";
                            propertyName = "";
                            
                            if (selectJsonObject.has("Arithmetic")) {
                                JSONObject arithmeticJsonObject = selectJsonObject.getJSONObject("Arithmetic");
                                if(arithmeticJsonObject.has("Left")){
                                    arithmeticJsonObject.getJSONObject("Left").has("Aggregation");
                                }
                                
                            }
                            if (selectJsonObject.has("Aggregation")) {
                                JSONObject aggregationJsonObject = selectJsonObject.getJSONObject("Aggregation");
                                if (aggregationJsonObject.has("Name")) {
                                    aggregationType = aggregationJsonObject.get("Name").toString();
                                }
                                if (aggregationJsonObject.has("Expression")) {
                                    aggregationJsonObject = aggregationJsonObject.getJSONObject("Expression");
                                    if (aggregationJsonObject.has("Column")) {
                                        aggregationJsonObject = aggregationJsonObject.getJSONObject("Column");
                                        if (aggregationJsonObject.has("Expression")) {
                                            if (aggregationJsonObject.getJSONObject("Expression").has("SourceRef")) {
                                                if (aggregationJsonObject.getJSONObject("Expression").getJSONObject("SourceRef").has("Source")) {
                                                    sourceJSONObject = aggregationJsonObject.getJSONObject("Expression").getJSONObject("SourceRef").getString("Source");
                                                }
                                            }
                                        }
                                        if (aggregationJsonObject.has("Property")) {
                                            propertyName = aggregationJsonObject.getString("Property");
                                        }
                                        selectExpProperty = aggregationJsonObject.get("Property").toString();
                                        if (!"".equals(aggregationType)) {
                                            selectExpProperty = aggregationType + ":" + selectExpProperty;
                                        }
                                    }
                                }
                            }
                            if (selectJsonObject.has("Column")) {
                                JSONObject selectColumnJsonObject = selectJsonObject.getJSONObject("Column");
                                if (selectColumnJsonObject.has("Expression")) {
                                    if (selectColumnJsonObject.getJSONObject("Expression").has("SourceRef")) {
                                        if (selectColumnJsonObject.getJSONObject("Expression").getJSONObject("SourceRef").has("Source")) {
                                            sourceJSONObject = selectColumnJsonObject.getJSONObject("Expression").getJSONObject("SourceRef").getString("Source");
                                            sourceJSONObject = sourceJSONObject.replaceAll("\\(\\d+\\)", "").trim();
                                        }
                                    }
                                }
                                if (selectColumnJsonObject.has("Property")) {
                                    propertyName = selectColumnJsonObject.getString("Property");
                                }
//                            sourceTableColSb.append(selectColumnName).append(",");
                            } else if (selectJsonObject.has("Measure")) {
                                JSONObject selectColumnJsonObject = selectJsonObject.getJSONObject("Measure");
                                if (selectColumnJsonObject.has("Expression")) {
                                    if (selectColumnJsonObject.getJSONObject("Expression").has("SourceRef")) {
                                        if (selectColumnJsonObject.getJSONObject("Expression").getJSONObject("SourceRef").has("Source")) {
                                            sourceJSONObject = selectColumnJsonObject.getJSONObject("Expression").getJSONObject("SourceRef").getString("Source");
                                            sourceJSONObject = sourceJSONObject.replaceAll("\\(\\d+\\)", "").trim();
                                        }
                                    }
                                }
                                if (selectColumnJsonObject.has("Property")) {
                                    propertyName = selectColumnJsonObject.getString("Property");
                                }
//                            sourceTableColSb.append(selectColumnName).append(",");
                            } else if (selectJsonObject.has("HierarchyLevel")) {

                                if (selectJsonObject.getJSONObject("HierarchyLevel").has("Expression")) {
                                    if (selectJsonObject.getJSONObject("HierarchyLevel").getJSONObject("Expression").has("Hierarchy")) {
                                        if (selectJsonObject.getJSONObject("HierarchyLevel").getJSONObject("Expression").getJSONObject("Hierarchy").has("Expression")) {
                                            if (selectJsonObject.getJSONObject("HierarchyLevel").getJSONObject("Expression").getJSONObject("Hierarchy").has("Expression")) {
                                                if (selectJsonObject.getJSONObject("HierarchyLevel").getJSONObject("Expression").getJSONObject("Hierarchy").getJSONObject("Expression").has("PropertyVariationSource")) {
                                                    if (selectJsonObject.getJSONObject("HierarchyLevel").getJSONObject("Expression").getJSONObject("Hierarchy").getJSONObject("Expression").getJSONObject("PropertyVariationSource").has("Expression")) {
                                                        if (selectJsonObject.getJSONObject("HierarchyLevel").getJSONObject("Expression").getJSONObject("Hierarchy").getJSONObject("Expression").getJSONObject("PropertyVariationSource").getJSONObject("Expression").has("SourceRef")) {
                                                            if (selectJsonObject.getJSONObject("HierarchyLevel").getJSONObject("Expression").getJSONObject("Hierarchy").getJSONObject("Expression").getJSONObject("PropertyVariationSource").getJSONObject("Expression").getJSONObject("SourceRef").has("Source")) {
                                                                sourceJSONObject = selectJsonObject.getJSONObject("HierarchyLevel").getJSONObject("Expression").getJSONObject("Hierarchy").getJSONObject("Expression").getJSONObject("PropertyVariationSource").getJSONObject("Expression").getJSONObject("SourceRef").getString("Source");
                                                                sourceJSONObject = sourceJSONObject.replaceAll("\\(\\d+\\)", "").trim();
                                                            }
                                                        }
                                                    }
                                                    if (selectJsonObject.getJSONObject("HierarchyLevel").getJSONObject("Expression").getJSONObject("Hierarchy").getJSONObject("Expression").getJSONObject("PropertyVariationSource").has("Property")) {
                                                        propertyName = selectJsonObject.getJSONObject("HierarchyLevel").getJSONObject("Expression").getJSONObject("Hierarchy").getJSONObject("Expression").getJSONObject("PropertyVariationSource").getString("Property");
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }

                            }
                            if (selectJsonObject.has("Name")) {
                                String name = selectJsonObject.get("Name").toString();
                                name = name.replaceAll("\\(\\d+\\)", "");
                                if (name.contains("(") && name.contains(".") && name.contains(")")) {
                                    String tabName = fromSourceMap.get(sourceJSONObject);
                                    getTableAndColumnFromBusinessRule(name, displayName, tabName, report);
                                }
                                if (fromSourceMap.get(sourceJSONObject) != null) {
                                    String tabName = fromSourceMap.get(sourceJSONObject);
                                    manageActualTable(name, propertyName, tabName);
                                    if (name.contains("(") && !name.substring(0, name.indexOf("(") + 1).contains(".")) {
                                        transforms = ", " + name;
                                        try {
                                            String tempTable = name.substring(name.indexOf("(") + 1, name.lastIndexOf(")")).split("\\.")[1];
                                        } catch (Exception ex) {

                                        }
                                        if (name.split("\\.")[1].contains(")")) {
                                            if (mapOfActualTableAgainstQuery.get((name + "$" + propertyName).toUpperCase()) == null && !propertyName.equals("")) {
                                                mapOfActualTableAgainstQuery.put((name + "$" + propertyName).toUpperCase(), name.substring(0, name.indexOf("(") + 1) + tabName + "." + propertyName + ")");
                                            } else if (!propertyName.equals("") && !mapOfActualTableAgainstQuery.get((name + "$" + propertyName).toUpperCase()).contains(name.substring(0, name.indexOf("(") + 1) + tabName + "." + propertyName + ")")) {

                                                mapOfActualTableAgainstQuery.put((name + "$" + propertyName).toUpperCase(), mapOfActualTableAgainstQuery.get((name + "$" + propertyName).toUpperCase()) + "#ERWIN#" + name.substring(0, name.indexOf("(") + 1) + tabName + "." + propertyName + ")");
                                            } else {
                                                mapOfActualTableAgainstQuery.put((name + "$" + propertyName).toUpperCase(), name);
                                            }
                                        }
                                    } else {
                                        if (mapOfActualTableAgainstQuery.get((name + "$" + propertyName).toUpperCase()) == null && !propertyName.equals("")) {
                                            mapOfActualTableAgainstQuery.put((name + "$" + propertyName).toUpperCase(), tabName + "." + propertyName);
                                        } else if (!propertyName.equals("") && !mapOfActualTableAgainstQuery.get((name + "$" + propertyName).toUpperCase()).contains(tabName + "." + propertyName)) {
                                            mapOfActualTableAgainstQuery.put((name + "$" + propertyName).toUpperCase(), mapOfActualTableAgainstQuery.get((name + "$" + propertyName).toUpperCase()) + "#ERWIN#" + tabName + "." + propertyName);
                                        } else {
                                            mapOfActualTableAgainstQuery.put((name + "$" + propertyName).toUpperCase(), name);
                                        }
                                    }
                                }
                                colLevel.add(name.trim());
                            }
                        }
                        if (prototypeQueryJson.has("OrderBy")) {

                            orderByJsonObjectArr = new JSONArray(prototypeQueryJson.get("OrderBy").toString());
                            for (int orderBy_i = 0; orderBy_i < orderByJsonObjectArr.length(); orderBy_i++) {
                                orderByJsonObject = orderByJsonObjectArr.getJSONObject(orderBy_i);
                                if (orderByJsonObject.has("Expression")) {
                                    orderByClause = orderByJsonObject.get("Expression").toString();
                                    orderByJsonObject = new JSONObject(orderByClause);
                                    if (orderByJsonObject.has("Aggregation")) {
                                        orderByClause = orderByJsonObject.get("Aggregation").toString();
                                        orderByJsonObject = new JSONObject(orderByClause);
                                        if (orderByJsonObject.has("Expression")) {
                                            orderByClause = orderByJsonObject.get("Expression").toString();
                                            orderByJsonObject = new JSONObject(orderByClause);
                                            if (orderByJsonObject.has("Column")) {
                                                orderByClause = orderByJsonObject.get("Column").toString();
                                                orderByJsonObject = new JSONObject(orderByClause);
                                            }
                                            if (orderByJsonObject.has("Property")) {
                                                //orderByJsonObject.get("Property").toString();
                                                if (extendedPropeties.containsKey("orderBy")) {
                                                    Set<String> conditions = extendedPropeties.get("orderBy");
                                                    conditions.add(orderByJsonObject.get("Property").toString());
                                                    extendedPropeties.put("orderBy", conditions);
                                                } else {
                                                    Set<String> condition = new HashSet();
                                                    condition.add(orderByJsonObject.get("Property").toString());
                                                    extendedPropeties.put("orderBy", condition);
                                                }
                                            }
                                        }
                                    }

                                    //Added by SudhansuT for getting order by clause from HierarchyLevel
                                    if (orderByJsonObject.has("HierarchyLevel")) {
                                        JSONObject hierarchyLevel = orderByJsonObject.getJSONObject("HierarchyLevel");
                                        if (hierarchyLevel.has("Expression")) {
                                            JSONObject expression_1 = hierarchyLevel.getJSONObject("Expression");
                                            if (expression_1.has("Hierarchy")) {
                                                JSONObject hierarchy_1 = expression_1.getJSONObject("Hierarchy");
                                                if (hierarchy_1.has("Expression")) {
                                                    JSONObject expression_2 = hierarchy_1.getJSONObject("Expression");
                                                    if (expression_2.has("PropertyVariationSource")) {
                                                        JSONObject propertyVariationSource = expression_2.getJSONObject("PropertyVariationSource");
                                                        if (propertyVariationSource.has("Property")) {
                                                            if (extendedPropeties.containsKey("orderBy")) {
                                                                Set<String> conditions = extendedPropeties.get("orderBy");
                                                                conditions.add(propertyVariationSource.get("Property").toString());
                                                                extendedPropeties.put("orderBy", conditions);
                                                            } else {
                                                                Set<String> condition = new HashSet();
                                                                condition.add(propertyVariationSource.get("Property").toString());
                                                                extendedPropeties.put("orderBy", condition);
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    // added chirishma for getting measure info 
                                    if (orderByJsonObject.has("Measure")) {
                                        orderByClause = orderByJsonObject.get("Measure").toString();
                                        orderByJsonObject = new JSONObject(orderByClause);
                                        if (orderByJsonObject.has("Property")) {
                                            orderByJsonObject.get("Property").toString();
                                            if (extendedPropeties.containsKey("orderBy")) {
                                                Set<String> conditions = extendedPropeties.get("orderBy");
                                                conditions.add(orderByJsonObject.get("Property").toString());
                                                extendedPropeties.put("orderBy", conditions);
                                            } else {
                                                Set<String> condition = new HashSet();
                                                condition.add(orderByJsonObject.get("Property").toString());
                                                extendedPropeties.put("orderBy", condition);
                                            }

                                        }

                                    }
                                    if (orderByJsonObject.has("Column")) {
                                        orderByClause = orderByJsonObject.get("Column").toString();
                                        orderByJsonObject = new JSONObject(orderByClause);
                                        if (orderByJsonObject.has("Property")) {
                                            orderByJsonObject.get("Property").toString();
                                            if (extendedPropeties.containsKey("orderBy")) {
                                                Set<String> conditions = extendedPropeties.get("orderBy");
                                                conditions.add(orderByJsonObject.get("Property").toString());
                                                extendedPropeties.put("orderBy", conditions);
                                            } else {
                                                Set<String> condition = new HashSet();
                                                condition.add(orderByJsonObject.get("Property").toString());
                                                extendedPropeties.put("orderBy", condition);
                                            }

                                        }

                                    }

                                }
                            }
                        }

                    }
                }
                if (singleVisualJson.has("projections")) {
                    String projectionsJson = singleVisualJson.get("projections").toString();
                    JSONObject projectionsJsonJson = new JSONObject(projectionsJson);
                    if (projectionsJsonJson.has("queryRef")) {
                        String queryRef = projectionsJsonJson.get("queryRef").toString();
                    }
                    if (projectionsJsonJson.has("Rows")) {
                        JSONArray projectionsRowsJson = projectionsJsonJson.getJSONArray("Rows");
                        for (int i = 0; i < projectionsRowsJson.length(); i++) {
                            JSONObject rowsJSON = projectionsRowsJson.getJSONObject(i);
                            String columnName = rowsJSON.get("queryRef").toString();
                            columnName = columnName.replaceAll("\\(\\d+\\)", "");
                            if (columnName.contains("(") && columnName.contains("(") && columnName.contains(".")) {
                                getTableAndColumnFromBusinessRule(columnName, displayName, "", report);
                            }
                            colLevel.add(columnName.trim());

                        }
                    }
                    if (projectionsJsonJson.has("Values")) {
                        JSONArray projectionsValuesJson = projectionsJsonJson.getJSONArray("Values");
                        for (int i = 0; i < projectionsValuesJson.length(); i++) {
                            JSONObject valuesJSON = projectionsValuesJson.getJSONObject(i);
                            String columnName = valuesJSON.get("queryRef").toString();
                            columnName = columnName.replaceAll("\\(\\d+\\)", "");
                            if (columnName.contains("(") && columnName.contains("(") && columnName.contains(".")) {
                                getTableAndColumnFromBusinessRule(columnName, displayName, "", report);
                            }
                            colLevel.add(columnName.trim());
                        }
                    }
                    if (projectionsJsonJson.has("Category")) {
                        JSONArray projectionsCategoryJson = projectionsJsonJson.getJSONArray("Category");
                        for (int i = 0; i < projectionsCategoryJson.length(); i++) {
                            JSONObject categoryJSON = projectionsCategoryJson.getJSONObject(i);
                            String columnName = categoryJSON.get("queryRef").toString();
                            columnName = columnName.replaceAll("\\(\\d+\\)", "");
                            if (columnName.contains("(") && columnName.contains("(") && columnName.contains(".")) {
                                getTableAndColumnFromBusinessRule(columnName, displayName, "", report);
                            }
                            colLevel.add(columnName.trim());
                        }
                    }
                    if (projectionsJsonJson.has("Targets")) {
                        JSONArray projectionsTargetJson = projectionsJsonJson.getJSONArray("Targets");
                        for (int i = 0; i < projectionsTargetJson.length(); i++) {
                            JSONObject targetJSON = projectionsTargetJson.getJSONObject(i);
                            String columnName = targetJSON.get("queryRef").toString();
                            columnName = columnName.replaceAll("\\(\\d+\\)", "");
                            if (columnName.contains("(") && columnName.contains("(") && columnName.contains(".")) {
                                getTableAndColumnFromBusinessRule(columnName, displayName, "", report);
                            }
                            colLevel.add(columnName.trim());
                        }
                    }

                }
                colLevel.addAll(PowerBIDataTransformsBuilder.colLevel);
                PowerBIDataTransformsBuilder.colLevel.clear();
                if (tableLevelMap.get("EntitysWithColumns") != null) {
                    Set<String> temp = tableLevelMap.get("EntitysWithColumns");
                    temp.addAll(colLevel);
                    tableLevelMap.put("EntitysWithColumns", temp);
                } else {
                    tableLevelMap.put("EntitysWithColumns", colLevel);
                }
                if (!tableLevelMap.isEmpty()) {
                    eachVisualContainser.put(pbixName, tableLevelMap);
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
            StringWriter exception = new StringWriter();
            e.printStackTrace(new PrintWriter(exception));
//            PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Error::Exception In PowerBIFiltersPropertiesBuilder.parseWhereCondition() Method " + exception + "\n");
        }
        return eachVisualContainser;
    }

    /**
     * this method extract the columns which had renamed in PowerBI table
     *
     * @param sectiosJsonArr
     * @param renamedColumns
     *
     *
     */
    public static void getPowerBIRenamedColumn(JSONArray sectiosJsonArr, Map<String, String> renamedColumns) {
        try {
            JSONArray visualContainersArr;
            JSONObject jsonObject;
            JSONObject configJsonObj;
            JSONObject colJsonObject = null;
            String configJson;
            for (int i = 0; i < sectiosJsonArr.length(); i++) {
                jsonObject = (JSONObject) sectiosJsonArr.get(i);
                String displayName = jsonObject.getString("displayName");
                visualContainersArr = jsonObject.getJSONArray("visualContainers");
                for (int col_i = 0; col_i < visualContainersArr.length(); col_i++) {
                    colJsonObject = (JSONObject) visualContainersArr.get(col_i);
                    if (colJsonObject.has("config")) {
                        configJson = colJsonObject.get("config").toString();
                        configJsonObj = new JSONObject(configJson);
                        if (configJsonObj.has("singleVisual")) {
                            JSONObject singleVisualJson = (JSONObject) configJsonObj.get("singleVisual");
                            if (singleVisualJson.has("columnProperties")) {
                                ObjectMapper mapper = new ObjectMapper();
                                JSONObject columnPropertiesJson = (JSONObject) singleVisualJson.get("columnProperties");
                                Map<String, Object> columnMap = mapper.readValue(columnPropertiesJson.toString(), Map.class);
                                for (Map.Entry<String, Object> entry : columnMap.entrySet()) {
                                    String columnName = entry.getKey();
                                    Map<String, String> displayJson = (Map) entry.getValue();
                                    for (Map.Entry<String, String> entry1 : displayJson.entrySet()) {
                                        String value = entry1.getValue();
                                        renamedColumns.put(columnName, value);
                                    }
                                }
                            }
                        }
                    }
                }

            }
        } catch (Exception ex) {
            logger.error(ex.getMessage());
            StringWriter exception = new StringWriter();
            ex.printStackTrace(new PrintWriter(exception));
//            exceptionLog.append("Exception In getPowerBIRenamedColumn() \n" + exception.toString());
//            exceptionLog.append("\n ================================");
        }

    }

    public static void getTableAndColumnFromBusinessRule(String businessRule, String displayName, String actualSource, String reportName) {

        Pattern pattern = Pattern.compile("[\\'a-zA-Z0-9\\s\\_\\-']*[.][\\'a-zA-Z0-9\\s\\_\\/\\-\\(\\)']*");
        Matcher matcher = pattern.matcher(businessRule);
        while (matcher.find()) {
//            System.out.println(matcher.group());
            try {
                String tableName = matcher.group().split("\\.")[0];
                if (actualSource == null || actualSource.equals("")) {
                    if (actualSourceMap.get(tableName) == null) {
                        actualSource = tableName.trim();
                    } else {
                        actualSource = actualSourceMap.get(tableName);

                    }
                } else {
                    actualSourceMap.put(tableName, actualSource.trim());
                }
                String columnName = matcher.group().split("\\.")[1].replace("(", "").replace(")", "");
                tableName = reportName.toLowerCase() + "." + tableName.toLowerCase();
                columnName = columnName.toLowerCase();
                displayName = displayName.toLowerCase();
                actualSource = reportName.toLowerCase() + "." + actualSource.toLowerCase();
                if (businessRuleMap.get(actualSource.trim() + "#ERWIN#" + columnName.trim() + "#ERWIN#" + displayName.trim()) != null) {
                    if (!businessRuleMap.get(actualSource.trim() + "#ERWIN#" + columnName.trim() + "#ERWIN#" + displayName.trim()).contains(businessRule)) {
                        businessRuleMap.put(actualSource.trim() + "#ERWIN#" + columnName.trim() + "#ERWIN#" + displayName.trim(), businessRuleMap.get(actualSource.trim() + "#ERWIN#" + columnName.trim() + "#ERWIN#" + displayName.trim()) + ", " + businessRule);
                    }
                } else {
                    if (businessRule != null) {
                        businessRuleMap.put(actualSource.trim() + "#ERWIN#" + columnName.trim() + "#ERWIN#" + displayName.trim(), businessRule);
                    }
                }

                if (businessRuleMap.get(tableName.trim() + "#ERWIN#" + columnName.trim() + "#ERWIN#" + displayName.trim()) != null) {
                    if (!businessRuleMap.get(tableName.trim() + "#ERWIN#" + columnName.trim() + "#ERWIN#" + displayName.trim()).contains(businessRule)) {
                        businessRuleMap.put(tableName.trim() + "#ERWIN#" + columnName.trim() + "#ERWIN#" + displayName.trim(), businessRuleMap.get(tableName.trim() + "#ERWIN#" + columnName.trim() + "#ERWIN#" + displayName.trim()) + ", " + businessRule);
                    }
                } else {
                    if (businessRule != null) {
                        businessRuleMap.put(tableName.trim() + "#ERWIN#" + columnName.trim() + "#ERWIN#" + displayName.trim(), businessRule);
                    }
                }
            } catch (Exception ex) {

            }
        }
    }

    public static void manageWhereCondition(Map<String, Set<String>> reportDEtailsMaap) {
        if (extendedPropetiesForWhere.get("where") != null) {
            Map<String, Set<String>> conditions = extendedPropetiesForWhere.get("where");
            for (Map.Entry<String, Set<String>> entry : reportDEtailsMaap.entrySet()) {
                String key = entry.getKey();
                Set<String> value = entry.getValue();
                if (conditions.containsKey(key)) {
                    conditions.get(key).addAll(value);
                } else {
                    conditions.put(key, value);
                }

            }
        } else {
            extendedPropetiesForWhere.put("where", reportDEtailsMaap);
        }
    }

    public static void manageActualTable(String actualTableName, String columnName, String jsonInfo) {

        Pattern pattern = Pattern.compile("[\\'a-zA-Z0-9\\s\\_\\-']*[.][\\'a-zA-Z0-9\\s\\_\\/\\-\\(\\)']*");
        Matcher matcher = pattern.matcher(actualTableName);
        while (matcher.find()) {
            try {
                actualTableName = matcher.group().split("\\.")[0];
                //f//  actualTableAgainstEntity.put(tableName.toUpperCase(),actualTableName.toUpperCase());
                actualTableAgainstEntity.put(actualTableName.toUpperCase() + "$" + columnName.toUpperCase(), jsonInfo.toUpperCase());
            } catch (Exception ex) {

            }
        }

    }
}
