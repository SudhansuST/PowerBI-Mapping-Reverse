/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.connectors.pbi.map.util;

import static com.erwin.cfx.connectors.pbi.map.util.PowerBIConfigPropertiesBuilder.getTableAndColumnFromBusinessRule;
import static com.erwin.cfx.connectors.pbi.map.v11.PowerBIReportParser.exceptionLog;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 *
 * @author sudhansu/chirishma
 */
public class PowerBIQueryPropertiesBuilder {

    public static Logger logger = Logger.getLogger(PowerBIQueryPropertiesBuilder.class);

    public static Map<String, Set<String>> getPowerBIReportQueryInfo(JSONObject commandJsonObj, String displayName, String report) {

        Map<String, Set<String>> reportQueryInfoMap = new HashMap();
        JSONObject selectJsonObject = null;
        JSONArray selectJsonArr = null;
        JSONArray orderByJsonArr = null;
        JSONArray fromJsonArr = null;
        JSONArray whereJsonArr = null;
        JSONObject fromJsonObject = null;
        JSONObject whereJsonObject = null;
        JSONObject orderByJsonObject = null;
        JSONObject semanticQueyObj = null;
        JSONObject queyObj = null;
        JSONObject tempObj = null;
        String str = "";
        String aliasName = "";
        String entityName = "";
        String mQuery = "";
        String propertyName = "";
        String sourceJSONObject = "";

        try {
            if (commandJsonObj.has("SemanticQueryDataShapeCommand")) {
                semanticQueyObj = commandJsonObj.getJSONObject("SemanticQueryDataShapeCommand");
                if (semanticQueyObj.has("Query")) {
                    if (semanticQueyObj.has("Binding")) {
                        semanticQueyObj.remove("Binding");
                        mQuery = semanticQueyObj.toString();
                    } else {
                        mQuery = semanticQueyObj.toString();
                    }

                    if (reportQueryInfoMap.containsKey("M-Query")) {
                        Set<String> mQueries = reportQueryInfoMap.get("M-Query");
                        mQueries.add(mQuery);
                        reportQueryInfoMap.put("M-Query", mQueries);
                    } else {
                        Set<String> mQueries = new HashSet<>();
                        mQueries.add(mQuery);
                        reportQueryInfoMap.put("M-Query", mQueries);
                    }
                    queyObj = semanticQueyObj.getJSONObject("Query");
                    if (queyObj.has("From")) {
                        fromJsonArr = queyObj.getJSONArray("From");
                        for (int from_i = 0; from_i < fromJsonArr.length(); from_i++) {
                            fromJsonObject = fromJsonArr.getJSONObject(from_i);
                            if (fromJsonObject.has("Entity")) {
                                entityName = fromJsonObject.getString("Entity").replaceAll("\\(\\d+\\)", "").trim();

                            }
                            if (fromJsonObject.has("Name")) {
                                aliasName = fromJsonObject.getString("Name");
                            }
                            if (reportQueryInfoMap.containsKey("From")) {
                                Set<String> fromcond = reportQueryInfoMap.get("From");
                                fromcond.add(fromJsonObject.getString("Name"));
                                reportQueryInfoMap.put("From", fromcond);
                            } else {
                                Set<String> fromcond = new HashSet<>();
                                fromcond.add(fromJsonObject.getString("Name"));
                                reportQueryInfoMap.put("From", fromcond);
                            }
                        }
                    }

                    if (queyObj.has("OrderBy")) {
                        orderByJsonArr = queyObj.getJSONArray("OrderBy");
                        for (int orderBy_i = 0; orderBy_i < orderByJsonArr.length(); orderBy_i++) {
                            orderByJsonObject = orderByJsonArr.getJSONObject(orderBy_i);
                            if (orderByJsonObject.has("Expression")) {
                                orderByJsonObject = orderByJsonObject.getJSONObject("Expression");
                                if (orderByJsonObject.has("Column")) {
                                    orderByJsonObject = orderByJsonObject.getJSONObject("Column");
                                    if (orderByJsonObject.has("Property")) {
                                        if (PowerBIConfigPropertiesBuilder.extendedPropeties.containsKey("orderBy")) {

                                            Set<String> orderByCondition = PowerBIConfigPropertiesBuilder.extendedPropeties.get("orderBy");
                                            orderByCondition.add(orderByJsonObject.getString("Property"));
                                            PowerBIConfigPropertiesBuilder.extendedPropeties.put("orderBy", orderByCondition);
                                        } else {
                                            Set<String> orderByCondition = new HashSet<>();
                                            orderByCondition.add(orderByJsonObject.getString("Property"));
                                            PowerBIConfigPropertiesBuilder.extendedPropeties.put("orderBy", orderByCondition);
                                        }
                                    }
                                }
                                if (orderByJsonObject.has("Aggregation")) {
                                    orderByJsonObject = orderByJsonObject.getJSONObject("Aggregation");
                                    if (orderByJsonObject.has("Expression")) {
                                        orderByJsonObject = orderByJsonObject.getJSONObject("Expression");
                                        if (orderByJsonObject.has("Column")) {
                                            orderByJsonObject = orderByJsonObject.getJSONObject("Column");
                                            if (orderByJsonObject.has("Property")) {
                                                //if (reportQueryInfoMap.containsKey("orderBy")) {
                                                if (PowerBIConfigPropertiesBuilder.extendedPropeties.containsKey("orderBy")) {
                                                    Set<String> orderByCondition = PowerBIConfigPropertiesBuilder.extendedPropeties.get("orderBy");
                                                    orderByCondition.add(orderByJsonObject.getString("Property"));
                                                    PowerBIConfigPropertiesBuilder.extendedPropeties.put("orderBy", orderByCondition);
                                                } else {
                                                    Set<String> orderByCondition = new HashSet<>();
                                                    orderByCondition.add(orderByJsonObject.getString("Property"));
                                                    PowerBIConfigPropertiesBuilder.extendedPropeties.put("orderBy", orderByCondition);
                                                }
                                                //reportQueryInfoMap.put("orderBy", orderByJsonObject.getString("Property"));
                                                propertyName = orderByJsonObject.getString("Property");

                                            }
                                            if (orderByJsonObject.has("Expression")) {
                                                if (orderByJsonObject.getJSONObject("Expression").has("SourceRef")) {
                                                    if (orderByJsonObject.getJSONObject("Expression").getJSONObject("SourceRef").has("Source")) {
                                                        sourceJSONObject = orderByJsonObject.getJSONObject("Expression").getJSONObject("SourceRef").getString("Source");
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                if (orderByJsonObject.has("HierarchyLevel")) {
                                    orderByJsonObject = orderByJsonObject.getJSONObject("HierarchyLevel");
                                    if (orderByJsonObject.has("Level")) {
                                        if (PowerBIConfigPropertiesBuilder.extendedPropeties.containsKey("orderBy")) {
                                            Set<String> orderByCondition = PowerBIConfigPropertiesBuilder.extendedPropeties.get("orderBy");
                                            orderByCondition.add(orderByJsonObject.getString("Level"));
                                            PowerBIConfigPropertiesBuilder.extendedPropeties.put("orderBy", orderByCondition);
                                        } else {
                                            Set<String> orderByCondition = new HashSet<>();
                                            orderByCondition.add(orderByJsonObject.getString("Level"));
                                            PowerBIConfigPropertiesBuilder.extendedPropeties.put("orderBy", orderByCondition);
                                        }
                                    }
                                }

                            }
                        }
                    }

                    if (queyObj.has("Select")) {
                        selectJsonArr = queyObj.getJSONArray("Select");
                        for (int select_i = 0; select_i < selectJsonArr.length(); select_i++) {
                            selectJsonObject = selectJsonArr.getJSONObject(select_i);

                            if (selectJsonObject.has("Expression")) {
                                JSONObject propertyObj = selectJsonObject.getJSONObject("Expression");

                                //reportQueryInfoMap.put("Property", propertyObj.getString("Property"));
                                if (reportQueryInfoMap.containsKey("Property")) {
                                    Set<String> propertyCondition = reportQueryInfoMap.get("Property");
                                    propertyCondition.add(propertyObj.getString("Property"));
                                    reportQueryInfoMap.put("Property", propertyCondition);
                                } else {
                                    Set<String> propertyCondition = new HashSet<>();
                                    propertyCondition.add(orderByJsonObject.getString("Property"));
                                    reportQueryInfoMap.put("Property", propertyCondition);
                                }
                            }
                            if (selectJsonObject.has("Aggregation")) {
                                JSONObject propertyObj = selectJsonObject.getJSONObject("Aggregation");
                                if (propertyObj.has("Expression")) {
                                    if (propertyObj.getJSONObject("Expression").has("Column")) {
                                        if (propertyObj.getJSONObject("Expression").getJSONObject("Column").has("Expression")) {
                                            if (propertyObj.getJSONObject("Expression").getJSONObject("Column").getJSONObject("Expression").has("SourceRef")) {
                                                if (propertyObj.getJSONObject("Expression").getJSONObject("Column").getJSONObject("Expression").getJSONObject("SourceRef").has("Source")) {
                                                    sourceJSONObject = propertyObj.getJSONObject("Expression").getJSONObject("Column").getJSONObject("Expression").getJSONObject("SourceRef").getString("Source");
                                                    sourceJSONObject = sourceJSONObject.replaceAll("\\(\\d+\\)", "").trim();
                                                }
                                            }
                                        }
                                        if (propertyObj.getJSONObject("Expression").getJSONObject("Column").has("Property")) {
                                            propertyName = propertyObj.getJSONObject("Expression").getJSONObject("Column").getString("Property");
                                        }
                                    }
                                }
                            }
                            if (reportQueryInfoMap.containsKey("Query")) {
                                //str = reportQueryInfoMap.get("Query") + "\n" + selectJsonObject.getString("Name");
                                str = selectJsonObject.getString("Name");
                                //reportQueryInfoMap.put("Query", str);
                                if (reportQueryInfoMap.containsKey("Query")) {
                                    Set<String> queryCondition = reportQueryInfoMap.get("Query");
                                    queryCondition.add(str);
                                    reportQueryInfoMap.put("Query", queryCondition);
                                } else {
                                    Set<String> queryCondition = new HashSet<>();
                                    queryCondition.add(str);
                                    reportQueryInfoMap.put("Query", queryCondition);
                                }
                            } else {
                                //reportQueryInfoMap.put("Query", selectJsonObject.getString("Name"));
                                Set<String> queryCondition = new HashSet<>();
                                queryCondition.add(selectJsonObject.getString("Name"));
                                reportQueryInfoMap.put("Query", queryCondition);

                            }

                            if (selectJsonObject.has("Column")) {
                                if (selectJsonObject.getJSONObject("Column").has("Expression")) {
                                    if (selectJsonObject.getJSONObject("Column").getJSONObject("Expression").has("SourceRef")) {
                                        if (selectJsonObject.getJSONObject("Column").getJSONObject("Expression").getJSONObject("SourceRef").has("Source")) {
                                            sourceJSONObject = selectJsonObject.getJSONObject("Column").getJSONObject("Expression").getJSONObject("SourceRef").getString("Source");
                                            sourceJSONObject = sourceJSONObject.replaceAll("\\(\\d+\\)", "").trim();
                                        }
                                    }
                                }
                                if (selectJsonObject.getJSONObject("Column").has("Property")) {
                                    propertyName = selectJsonObject.getJSONObject("Column").getString("Property");
                                }
                            } else if (selectJsonObject.has("Measure")) {
                                if (selectJsonObject.getJSONObject("Measure").has("Expression")) {
                                    if (selectJsonObject.getJSONObject("Measure").getJSONObject("Expression").has("SourceRef")) {
                                        if (selectJsonObject.getJSONObject("Measure").getJSONObject("Expression").getJSONObject("SourceRef").has("Source")) {
                                            sourceJSONObject = selectJsonObject.getJSONObject("Measure").getJSONObject("Expression").getJSONObject("SourceRef").getString("Source");
                                            sourceJSONObject = sourceJSONObject.replaceAll("\\(\\d+\\)", "").trim();
                                        }
                                    }
                                }
                                if (selectJsonObject.getJSONObject("Measure").has("Property")) {
                                    propertyName = selectJsonObject.getJSONObject("Measure").getString("Property");
                                }
                            }

                            if (selectJsonObject.has("Name")) {
                                String name = selectJsonObject.get("Name").toString();
                                if (name.contains("(") && name.contains(".") && name.contains(")")) {
                                    getTableAndColumnFromBusinessRule(name, displayName, "", report);
                                }
                                name = name.replaceAll("\\(\\d+\\)", "").trim();
                                if (sourceJSONObject.equals(aliasName)) {
                                    if (name.contains("(") && !name.contains(" (")) {
                                        String tempTable = name.substring(name.indexOf("(") + 1, name.lastIndexOf(")")).split("\\.")[1];
                                        if (name.split("\\.")[1].contains(")")) {
                                            if (PowerBIConfigPropertiesBuilder.mapOfActualTableAgainstQuery.get(name) == null) {
                                                PowerBIConfigPropertiesBuilder.mapOfActualTableAgainstQuery.put(name, name.substring(0, name.indexOf("(") + 1) + entityName + "." + propertyName + ")");
                                            } else if (!PowerBIConfigPropertiesBuilder.mapOfActualTableAgainstQuery.get(name).contains(name.substring(0, name.indexOf("(") + 1) + entityName + "." + propertyName + ")")) {
                                                PowerBIConfigPropertiesBuilder.mapOfActualTableAgainstQuery.put(name, PowerBIConfigPropertiesBuilder.mapOfActualTableAgainstQuery.get(name) + "#ERWIN#" + name.substring(0, name.indexOf("(") + 1) + entityName + "." + propertyName + ")");
                                            }
                                        }
                                    } else {
                                        if (PowerBIConfigPropertiesBuilder.mapOfActualTableAgainstQuery.get(name) == null) {
                                            PowerBIConfigPropertiesBuilder.mapOfActualTableAgainstQuery.put(name, entityName + "." + propertyName);
                                        } else if (!PowerBIConfigPropertiesBuilder.mapOfActualTableAgainstQuery.get(name).contains(entityName + "." + propertyName)) {
                                            PowerBIConfigPropertiesBuilder.mapOfActualTableAgainstQuery.put(name, PowerBIConfigPropertiesBuilder.mapOfActualTableAgainstQuery.get(name) + "#ERWIN#" + entityName + "." + propertyName);
                                        }
                                    }
                                }
                            }

                        }
                    }
                    if (queyObj.has("Where")) {
                        PowerBIFiltersPropertiesBuilder.parseWhereCondition(queyObj);
                        Map<String, Set<String>> reportDEtailsMaap = PowerBIFiltersPropertiesBuilder.parseWhereCondition(queyObj);
                        if (reportDEtailsMaap != null) {
                            PowerBIConfigPropertiesBuilder.manageWhereCondition(reportDEtailsMaap);

                        }

                    }
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
            StringWriter exception = new StringWriter();
            e.printStackTrace(new PrintWriter(exception));
//            exceptionLog.append("Exception In getPowerBIReportQueryInfo() \n" + exception.toString());
//            exceptionLog.append("\n ================================");
        }
        return reportQueryInfoMap;
    }

}
