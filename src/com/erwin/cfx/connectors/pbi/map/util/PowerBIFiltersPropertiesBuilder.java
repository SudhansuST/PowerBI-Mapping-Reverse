/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.connectors.pbi.map.util;

import static com.erwin.cfx.connectors.pbi.map.v11.PowerBIReportParser.exceptionLog;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 *
 * @author Toshiba
 */
public class PowerBIFiltersPropertiesBuilder {

    public static Map<String, Set<String>> filterInfoMap = new HashMap();

    public static Logger logger = Logger.getLogger(PowerBIFiltersPropertiesBuilder.class);

    /**
     *
     * This method extract the extended properties participated in PowerBI
     *
     * @param singleVisualJson
     * @return returns key and values of extended properties
     *
     */
    public static Map<String, Set<String>> getPowerBIReportFiltersInfo(JSONObject singleVisualJson) {
        try {

            // "Projections" has the projective information about the Report.
            if (singleVisualJson.has("filter")) {
                String filterJson = singleVisualJson.get("filter").toString();
                JSONObject filterJsonJson = new JSONObject(filterJson);
                if (filterJsonJson.has("From")) {
                    String fromString = filterJsonJson.get("From").toString();
                    JSONArray fromJsonArry = new JSONArray(fromString);
                    // Need to Add for Loop if from has more than one table
                    JSONObject fromEntityJson = fromJsonArry.get(0) != null ? (JSONObject) fromJsonArry.get(0) : null;
                }
                if (filterJsonJson.has("Where")) {
                    Map<String, Set<String>> reportDEtailsMaap = parseWhereCondition(filterJsonJson);
                    if (reportDEtailsMaap != null) {
                        PowerBIConfigPropertiesBuilder.manageWhereCondition(reportDEtailsMaap);
                    }
                }
            }
            JSONObject orderByJsonObject = null;
            if (singleVisualJson.has("prototypeQuery")) {
                JSONObject prototypeQueryJson = (JSONObject) singleVisualJson.get("prototypeQuery");

                // OrderBy Clause to GET the Columns and BusinessRule Details
                if (prototypeQueryJson.has("OrderBy")) {
                    String orderByClause = "";
                    JSONArray orderByJsonObjectArr = new JSONArray(prototypeQueryJson.get("OrderBy").toString());
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
                                        orderByClause = orderByJsonObject.getString("Property");
                                        //filterInfoMap.put("Order By", orderByClause);
                                        if (filterInfoMap.containsKey("Order By")) {
                                            Set<String> orderByCond = filterInfoMap.get("Order By");
                                            orderByCond.add(orderByClause);
                                            filterInfoMap.put("Order By", orderByCond);
                                        } else {
                                            Set<String> orderByCond = new HashSet<>();
                                            orderByCond.add(orderByClause);
                                            filterInfoMap.put("Order By", orderByCond);
                                        }
                                    }
                                }
                            }
                            // new for measurements added chirisma
                            if (orderByJsonObject.has("Measure")) {
                                orderByClause = orderByJsonObject.get("Measure").toString();
                                orderByJsonObject = new JSONObject(orderByClause);
                                if (orderByJsonObject.has("Property")) {
                                    orderByJsonObject.get("Property").toString();
                                    if (filterInfoMap.containsKey("orderBy")) {
                                        Set<String> conditions = filterInfoMap.get("orderBy");
                                        conditions.add(orderByJsonObject.get("Property").toString());
                                        filterInfoMap.put("orderBy", conditions);
                                    } else {
                                        Set<String> condition = new HashSet();
                                        condition.add(orderByJsonObject.get("Property").toString());
                                        filterInfoMap.put("orderBy", condition);
                                    }

                                }

                            }
                            if (orderByJsonObject.has("Column")) {
                                orderByClause = orderByJsonObject.get("Column").toString();
                                orderByJsonObject = new JSONObject(orderByClause);
                                if (orderByJsonObject.has("Property")) {
                                    orderByJsonObject.get("Property").toString();
                                    if (filterInfoMap.containsKey("orderBy")) {
                                        Set<String> conditions = filterInfoMap.get("orderBy");
                                        conditions.add(orderByJsonObject.get("Property").toString());
                                        filterInfoMap.put("orderBy", conditions);
                                    } else {
                                        Set<String> condition = new HashSet();
                                        condition.add(orderByJsonObject.get("Property").toString());
                                        filterInfoMap.put("orderBy", condition);
                                    }

                                }

                            }
                        }
                    }
                }

                // From Clause to GET the Entity Details
                if (prototypeQueryJson.has("From")) {
                    String fromJson = prototypeQueryJson.get("From").toString();
                    JSONArray fromJsonArr = new JSONArray(fromJson);
                    for (int table_i = 0; table_i < fromJsonArr.length(); table_i++) {
                        JSONObject tableJsonObject = (JSONObject) fromJsonArr.get(table_i);
                        String entityName = tableJsonObject.get("Entity").toString();
                    }
                }

                // Select Clause to GET the Columns and BusinessRule Details
                if (prototypeQueryJson.has("Select")) {
                    String selectJson = prototypeQueryJson.get("Select").toString();
                    JSONArray selectJsonArr = new JSONArray(selectJson);
                    for (int select_i = 0; select_i < selectJsonArr.length(); select_i++) {
                        JSONObject selectJsonObject = (JSONObject) selectJsonArr.get(select_i);
                        if (selectJsonObject.has("Property")) {
                            //filterInfoMap.put("EntitySelectProperty", selectJsonObject.getString("Property"));
                            //filterInfoMap.put("EntitySelectProperty", selectJsonObject.getString("Property"));
                            if (filterInfoMap.containsKey("EntitySelectProperty")) {
                                Set<String> entitySelectProperty = filterInfoMap.get("EntitySelectProperty");
                                entitySelectProperty.add(selectJsonObject.getString("Property"));
                                filterInfoMap.put("EntitySelectProperty", entitySelectProperty);
                            } else {
                                Set<String> entitySelectProperty = new HashSet<>();
                                entitySelectProperty.add(selectJsonObject.getString("Property"));
                                filterInfoMap.put("EntitySelectProperty", entitySelectProperty);
                            }
                        }
                        if (selectJsonObject.has("Name")) {
                            //filterInfoMap.put("EntitySelectName", selectJsonObject.getString("Property"));
                            if (filterInfoMap.containsKey("EntitySelectName")) {
                                Set<String> entitySelectName = filterInfoMap.get("EntitySelectName");
                                entitySelectName.add(selectJsonObject.getString("Property"));
                                filterInfoMap.put("EntitySelectName", entitySelectName);
                            } else {
                                Set<String> entitySelectName = new HashSet<>();
                                entitySelectName.add(selectJsonObject.getString("Property"));
                                filterInfoMap.put("EntitySelectName", entitySelectName);
                            }
                        }
                    }
                }
            }
            if (singleVisualJson.has("filter")) {

            }
        } catch (Exception e) {
            logger.error(e.getMessage());
            StringWriter exception = new StringWriter();
            e.printStackTrace(new PrintWriter(exception));
            PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Error::Exception In PowerBIFiltersPropertiesBuilder.getPowerBIReportFiltersInfo() Method " + exception + "\n");
//            exceptionLog.append("Exception In getPowerBIReportFiltersInfo() \n" + exception.toString());
//            exceptionLog.append("\n ================================");
        }
        return filterInfoMap;
    }

    /**
     * This method extract where conditions
     *
     * @param colList
     * @param valueList
     * @return returns where conditions as String
     */
    public static String getWhereConditionInInfo(LinkedList<String> colList, LinkedList<String> valueList, String condition) {
        String colName = "";

        StringBuilder inValueSb = new StringBuilder();
        String value = "";
        for (int i = 0; i < colList.size(); i++) {
            colName = colList.get(i);

            if (valueList != null) {

                for (int vl = 0; vl < valueList.size(); vl++) {
                    if (vl > 0) {
                        value = value + ", ";
                    }
                    value = value + "'" + valueList.get(vl) + "'";

                }
                //value = valueList != null ? valueList.get(i) : "";
                //inValueSb.append(colName).append(" IN ").append("'").append(value).append("'");

            }
            inValueSb.append(colName).append(condition + " IN ").append("(").append(value).append(")");
        }
        return inValueSb.toString();
    }

    /**
     *
     * @param lowerBound
     * @return
     */
    public static String getWhereConditionLowerBoundInfo(JSONObject lowerBound) {

        JSONObject jsonObj = null;
        String amountValue = null;
        try {
            if (lowerBound.has("DateSpan")) {
                jsonObj = lowerBound.getJSONObject("DateSpan");
                if (jsonObj.has("Expression")) {
                    jsonObj = jsonObj.getJSONObject("Expression");
                    if (jsonObj.has("DateAdd")) {
                        jsonObj = jsonObj.getJSONObject("DateAdd");
                        jsonObj.get("Amount");
                        amountValue = jsonObj.get("Amount").toString();
                    }
                    if (jsonObj.has("Now")) {
                        amountValue = "CurrentTimeStamp";
                    }
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
            StringWriter exception = new StringWriter();
            e.printStackTrace(new PrintWriter(exception));
//            PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Error::Exception In PowerBIFiltersPropertiesBuilder.getWhereConditionLowerBoundInfo() Method " + exception + "\n");
        }
        return amountValue;
    }

    public static Map parseWhereCondition(JSONObject filterJsonJson) {
        Map<String, Set<String>> filterWhereInfoMap = new HashMap();

        try {
            Set<String> whereConditions = new HashSet<>();
            String colName = "";
            String WhereString = filterJsonJson.get("Where").toString();
            JSONArray whereJsonArry = new JSONArray(WhereString);
            // Need to Add for Loop if where has more than one condition
            for (int where_i = 0; where_i < whereJsonArry.length(); where_i++) {
                JSONObject whereEntityJson = whereJsonArry.get(where_i) != null ? (JSONObject) whereJsonArry.get(0) : null;
                JSONObject conditionObject = whereEntityJson.getJSONObject("Condition");

                if (conditionObject.has("Between")) {
                    JSONObject btwJson = conditionObject.getJSONObject("Between");
                    JSONObject expressionJson = btwJson.getJSONObject("Expression");
                    expressionJson.getJSONObject("Column").get("Property");
                    JSONObject lowerBoundJson = btwJson.getJSONObject("LowerBound");
                    JSONObject upperBoundJson = btwJson.getJSONObject("UpperBound");

                    String lowerBound = getWhereConditionLowerBoundInfo(lowerBoundJson);
                    String upperBound = getWhereConditionLowerBoundInfo(upperBoundJson);

                    if (expressionJson.has("Column")) {
                        colName = expressionJson.getJSONObject("Column").get("Property").toString();
                        // Need to write the code 
                    }
                    //colName BETWEEN lowerBound and upperBound
                    colName = colName + " BETWEEN " + lowerBound + " AND " + upperBound;
                    //filterInfoMap.put("BETWEEN", colName);

                    if (filterWhereInfoMap.containsKey("BETWEEN")) {
                        Set<String> betweenCond = filterWhereInfoMap.get("BETWEEN");
                        betweenCond.add(colName);
                        filterWhereInfoMap.put("BETWEEN", betweenCond);
                    } else {
                        Set<String> betweenCond = new HashSet<>();
                        betweenCond.add(colName);
                        filterWhereInfoMap.put("BETWEEN", betweenCond);
                    }
                }
                if (conditionObject.has("In")) {
                    LinkedList<String> colList = new LinkedList();
                    LinkedList<String> valueList = new LinkedList();
                    String literal = "";
                    JSONObject inJson = conditionObject.getJSONObject("In");
                    JSONArray expressionsJson = inJson.getJSONArray("Expressions");
                    //filterInfoMap.put("IN", inJson.toString());
//                        if (filterInfoMap.containsKey("IN")) {
//                            Set<String> inCond = filterInfoMap.get("IN");
//                            inCond.add(inJson.toString());
//                            filterInfoMap.put("IN", inCond);
//                        } else {
//                            Set<String> inCond = new HashSet<>();
//                            inCond.add(inJson.toString());
//                            filterInfoMap.put("IN", inCond);
//                        }

                    for (int expressionsJson_i = 0; expressionsJson_i < expressionsJson.length(); expressionsJson_i++) {
                        JSONObject expressionJson = (JSONObject) expressionsJson.get(expressionsJson_i);
                        if (expressionJson.has("Column")) {
                            // column JSON
                            expressionJson = expressionJson.getJSONObject("Column");
                            colName = expressionJson.get("Property").toString();
                            colList.add(colName);
                        }
                    }
                    if (inJson.has("Values")) {
                        JSONArray valuesJson = inJson.getJSONArray("Values");

                        for (int valuesJson_i = 0; valuesJson_i < valuesJson.length(); valuesJson_i++) {
                            JSONArray valueJson = (JSONArray) valuesJson.get(valuesJson_i);
                            for (int valuesJson_j = 0; valuesJson_j < valueJson.length(); valuesJson_j++) {
                                JSONObject valueJson1 = (JSONObject) valueJson.get(valuesJson_j);
                                if (valueJson1.has("Literal")) {
                                    // Literal JSON
                                    valueJson1 = valueJson1.getJSONObject("Literal");
                                    literal = valueJson1.get("Value").toString();
                                    valueList.add(literal);
                                }
                            }
                        }
                    } else if (inJson.has("Table")) {

                        //"SourceRef": {
//									"Source": "subquery"
//								}
                        if (inJson.getJSONObject("Table").has("SourceRef")) {
                            if (inJson.getJSONObject("Table").getJSONObject("SourceRef").has("Source")) {
                                valueList.add(inJson.getJSONObject("Table").getJSONObject("SourceRef").getString("Source"));
                            }

                        }

                    }
                    String inConditionInfo = getWhereConditionInInfo(colList, valueList, "");
                    //filterInfoMap.put("IN", inConditionInfo);
                    if (filterWhereInfoMap.containsKey("IN")) {
                        Set<String> inCond = filterWhereInfoMap.get("IN");
                        inCond.add(inConditionInfo);
                        filterWhereInfoMap.put("IN", inCond);
                    } else {
                        Set<String> inCond = new HashSet<>();
                        inCond.add(inConditionInfo);
                        filterWhereInfoMap.put("IN", inCond);
                    }
                }// newly added 30-10-2020
                if (conditionObject.has("Not")) {
                    LinkedList<String> colList = new LinkedList();
                    LinkedList<String> valueList = new LinkedList();
                    String literal = "";
                    JSONObject inJson = conditionObject.getJSONObject("Not");
                    JSONObject OuterexpressionJson = inJson.getJSONObject("Expression");
                    if (OuterexpressionJson.has("In")) {
                        JSONObject OuterInJson = OuterexpressionJson.getJSONObject("In");
                        JSONArray valuesJson = OuterInJson.getJSONArray("Values");
                        JSONArray expressionsJson = OuterInJson.getJSONArray("Expressions");

// filterInfoMap.put("IN", inJson.toString());
                        for (int expressionsJson_i = 0; expressionsJson_i < expressionsJson.length(); expressionsJson_i++) {
                            JSONObject expressionJson = (JSONObject) expressionsJson.get(expressionsJson_i);
                            if (expressionJson.has("Column")) {
// column JSON
                                expressionJson = expressionJson.getJSONObject("Column");
                                colName = expressionJson.get("Property").toString();
                                colList.add(colName);
                            }
                        }
                        for (int valuesJson_i = 0; valuesJson_i < valuesJson.length(); valuesJson_i++) {
                            JSONArray valueJson = (JSONArray) valuesJson.get(valuesJson_i);
                            for (int valuesJson_j = 0; valuesJson_j < valueJson.length(); valuesJson_j++) {
                                JSONObject valueJson1 = (JSONObject) valueJson.get(valuesJson_j);
                                if (valueJson1.has("Literal")) {
// Literal JSON
                                    valueJson1 = valueJson1.getJSONObject("Literal");
                                    literal = valueJson1.get("Value").toString();
                                    valueList.add(literal);
                                }
                            }
                        }
                        String inConditionInfo = getWhereConditionInInfo(colList, valueList, " Not");
                        //filterInfoMap.put("Not In", inConditionInfo);
                        if (filterWhereInfoMap.containsKey("Not In")) {
                            Set<String> inCond = filterInfoMap.get("Not In");
                            inCond.add(inConditionInfo);
                            filterWhereInfoMap.put("Not In", inCond);
                        } else {
                            Set<String> inCond = new HashSet<>();
                            inCond.add(inConditionInfo);
                            filterWhereInfoMap.put("Not In", inCond);
                        }
                    }
                }
            }
        } catch (Exception ex) {
            StringWriter exception = new StringWriter();
            ex.printStackTrace(new PrintWriter(exception));
//            PowerBI_LOGGER.detailLogger.append(PowerBI_LOGGER.getTimestampForLogging() + "::Error::Exception In PowerBIFiltersPropertiesBuilder.parseWhereCondition() Method " + exception + "\n");

        }
        return filterWhereInfoMap;
    }
}
