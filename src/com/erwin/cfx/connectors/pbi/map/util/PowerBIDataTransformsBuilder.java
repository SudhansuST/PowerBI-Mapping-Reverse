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
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 *
 * @author Toshiba
 */
public class PowerBIDataTransformsBuilder {

    public static Logger logger = Logger.getLogger(PowerBIDataTransformsBuilder.class);
    public static Set<String> colLevel = new HashSet<>();

    /**
     * This method extracts PowerBI datatypes and transformation rules from the
     * PowerBI Layout JSON
     *
     * @param configJsonObj
     * @param powerBIMetadata
     * @return Returns Map of Transformation Rules
     *
     *
     *
     */
    public static Map<String, Set<String>> getPowerBIReportDataTransformsInfo(JSONObject configJsonObj, Map<String, String> powerBIMetadata, String displayName, String report) {

        Map<String, Set<String>> reportDataTransformsInfoMap = new HashMap();
        String selectJsonArr = "";
        String filtersJsonArr = "";
        JSONObject selectJsonObj = null;
        JSONArray selectJsonObjArr = null;
        JSONObject filtersJsonObj = null;
        JSONArray filtersJsonObjArr = null;
        Map<String, Set<String>> tableLevelMap = new HashMap();

        try {
            //"singleVisual" HAS Complete information about particular Report
//            if (configJsonObj.has("objects")) {
//
//                if (configJsonObj.getJSONObject("objects").has("columnWidth")) {
//                    JSONArray columnWidthJSONArray = configJsonObj.getJSONObject("objects").getJSONArray("columnWidth");
//                    for (int cw_index = 0; cw_index < columnWidthJSONArray.length(); cw_index++) {
//
//                        JSONObject eachCW = columnWidthJSONArray.getJSONObject(cw_index);
//                        if (eachCW.has("selector")) {
//                            if (eachCW.getJSONObject("selector").has("metadata")) {
//                                String metadata = eachCW.getJSONObject("selector").getString("metadata");
//                                if (metadata.contains("(") && metadata.contains(".") && metadata.contains(")")) {
//                                    PowerBIConfigPropertiesBuilder.getTableAndColumnFromBusinessRule(metadata, displayName, "");
//                                }
//
//                                metadata = metadata.replaceAll("\\(\\d+\\)", "").trim();
//                                colLevel.add(metadata);
//                            }
//                        }
//
//                    }
//                }
//
//            }

            if (configJsonObj.has("queryMetadata")) {
                JSONObject queryMetadataJson = (JSONObject) configJsonObj.get("queryMetadata");
                if (queryMetadataJson.has("Select")) {
                    selectJsonArr = queryMetadataJson.get("Select").toString();
                    selectJsonObjArr = new JSONArray(selectJsonArr);
                    for (int selectArr_i = 0; selectArr_i < selectJsonObjArr.length(); selectArr_i++) {
                        selectJsonObj = selectJsonObjArr.getJSONObject(selectArr_i);
                        //reportDataTransformsInfoMap.put("Restatement", selectJsonObj.getString("Restatement"));
                        if (reportDataTransformsInfoMap.containsKey("Restatement")) {
                            Set<String> restatemnt = reportDataTransformsInfoMap.get("Restatement");
                            restatemnt.add(selectJsonObj.getString("Restatement"));
                            reportDataTransformsInfoMap.put("Restatement", restatemnt);
                        } else {
                            Set<String> restatemnt = new HashSet<>();
                            restatemnt.add(selectJsonObj.getString("Restatement"));
                            reportDataTransformsInfoMap.put("Restatement", restatemnt);
                        }
                        //reportDataTransformsInfoMap.put("Name", selectJsonObj.getString("Name"));
                        if (reportDataTransformsInfoMap.containsKey("Name")) {
                            Set<String> stateName = reportDataTransformsInfoMap.get("Name");
                            stateName.add(selectJsonObj.getString("Name"));
                            reportDataTransformsInfoMap.put("Name", stateName);
                        } else {
                            Set<String> stateName = new HashSet<>();
                            stateName.add(selectJsonObj.getString("Name"));
                            reportDataTransformsInfoMap.put("Name", stateName);
                        }

                        //reportDataTransformsInfoMap.put("Type", selectJsonObj.getString("Name"));
                        if (reportDataTransformsInfoMap.containsKey("Type")) {
                            Set<String> typeSet = reportDataTransformsInfoMap.get("Type");
                            typeSet.add(selectJsonObj.getString("Name"));
                            reportDataTransformsInfoMap.put("Type", typeSet);
                        } else {
                            Set<String> typeSet = new HashSet<>();
                            typeSet.add(selectJsonObj.getString("Name"));
                            reportDataTransformsInfoMap.put("Type", typeSet);
                        }
//                        String colName=selectJsonObj.getString("Name");
//                        String colName1=colName.replaceAll("\\(", "").replaceAll("\\)", "");
//                        if(colName1.contains("(") && colName.contains(")")){
//                            colName1=colName1.substring(colName1.indexOf("(")+1, colName1.indexOf(")"));
//                        }

                        if (selectJsonObj.has("Format")) {
                            powerBIMetadata.put(selectJsonObj.getString("Name").replaceAll("\\(", "").replaceAll("\\)", ""), selectJsonObj.getString("Type") + "!Erwin!" + selectJsonObj.getString("Format"));
                            powerBIMetadata.put(selectJsonObj.getString("Name"), selectJsonObj.getString("Type") + "!Erwin!" + selectJsonObj.getString("Format"));
                            if (selectJsonObj.getString("Name").contains("(") && selectJsonObj.getString("Name").contains(")")) {
                                powerBIMetadata.put(selectJsonObj.getString("Name").substring(selectJsonObj.getString("Name").indexOf("(") + 1, selectJsonObj.getString("Name").indexOf(")")), selectJsonObj.getString("Type") + "!Erwin!" + selectJsonObj.getString("Format"));
                            }
                        } else {
                            powerBIMetadata.put(selectJsonObj.getString("Name").replaceAll("\\(", "").replaceAll("\\)", ""), selectJsonObj.getString("Type") + "!Erwin!");
                            powerBIMetadata.put(selectJsonObj.getString("Name"), selectJsonObj.getString("Type") + "!Erwin!");
                            if (selectJsonObj.getString("Name").contains("(") && selectJsonObj.getString("Name").contains(")")) {
                                powerBIMetadata.put(selectJsonObj.getString("Name").substring(selectJsonObj.getString("Name").indexOf("(") + 1, selectJsonObj.getString("Name").indexOf(")")), selectJsonObj.getString("Type") + "!Erwin!");
                            }
                        }
                    }
                }
                if (queryMetadataJson.has("Filters")) {
                    filtersJsonArr = queryMetadataJson.get("Filters").toString();
                    filtersJsonObjArr = new JSONArray(filtersJsonArr);
                    filtersJsonObj = filtersJsonObjArr.getJSONObject(0);
                    if (filtersJsonObj.has("expression")) {
                        filtersJsonArr = filtersJsonObj.get("expression").toString();
                        filtersJsonObj = new JSONObject(filtersJsonArr);
                        if (filtersJsonObj.has("Column")) {
                            filtersJsonArr = filtersJsonObj.get("Column").toString();
                            filtersJsonObj = new JSONObject(filtersJsonArr);
                            if (filtersJsonObj.has("Expression")) {
                                filtersJsonArr = filtersJsonObj.get("Expression").toString();
                                filtersJsonObj = new JSONObject(filtersJsonArr);
                                if (filtersJsonObj.has("SourceRef")) {
                                    filtersJsonArr = filtersJsonObj.get("SourceRef").toString();
                                    filtersJsonObj = new JSONObject(filtersJsonArr);
                                }
                            }
                        }
                    }
                }
            }
            if (configJsonObj.has("selects")) {
                JSONArray selectsJsonArr = configJsonObj.getJSONArray("selects");

                String transforms = "";
                for (int select_i = 0; select_i < selectsJsonArr.length(); select_i++) {
                    selectJsonObj = selectsJsonArr.getJSONObject(select_i);
                    String displayName1 = selectJsonObj.get("displayName").toString();
                    transforms = selectJsonObj.get("queryName").toString();
                    if (transforms.contains("(") && transforms.contains(".") && transforms.contains(")")) {
                        PowerBIConfigPropertiesBuilder.getTableAndColumnFromBusinessRule(transforms, displayName, "", report);
                    }
                    if (reportDataTransformsInfoMap.containsKey("Transformations")) {
                        //transforms = reportDataTransformsInfoMap.get("Transformations") + "," + transforms;
                        //transforms = reportDataTransformsInfoMap.get("Transformations") + "," + transforms;
                        //reportDataTransformsInfoMap.put("Transformations", transforms);
                        if (reportDataTransformsInfoMap.containsKey("Transformations")) {
                            Set<String> transformationsSet = reportDataTransformsInfoMap.get("Transformations");
                            transformationsSet.add(transforms);
                            reportDataTransformsInfoMap.put("Transformations", transformationsSet);
                        } else {
                            Set<String> transformationsSet = new HashSet<>();
                            transformationsSet.add(transforms);
                            reportDataTransformsInfoMap.put("Transformations", transformationsSet);
                        }
                    } else {
                        //reportDataTransformsInfoMap.put("Transformations", transforms);
                        if (reportDataTransformsInfoMap.containsKey("Transformations")) {
                            Set<String> transformationsSet = reportDataTransformsInfoMap.get("Transformations");
                            transformationsSet.add(transforms);
                            reportDataTransformsInfoMap.put("Transformations", transformationsSet);
                        } else {
                            Set<String> transformationsSet = new HashSet<>();
                            transformationsSet.add(transforms);
                            reportDataTransformsInfoMap.put("Transformations", transformationsSet);
                        }
                    }

                    JSONObject expressionJson = selectJsonObj.getJSONObject("expr");
                    Iterator expJsonObjIterator = expressionJson.keys();

                    if (expressionJson.has("expression")) {
                        filtersJsonArr = expressionJson.get("expression").toString();
                        filtersJsonObj = new JSONObject(filtersJsonArr);
                        if (filtersJsonObj.has("Column")) {
                            filtersJsonArr = filtersJsonObj.get("Column").toString();
                            filtersJsonObj = new JSONObject(filtersJsonArr);
                            if (filtersJsonObj.has("Expression")) {
                                filtersJsonArr = filtersJsonObj.get("Expression").toString();
                                filtersJsonObj = new JSONObject(filtersJsonArr);
                                if (filtersJsonObj.has("SourceRef")) {
                                    filtersJsonArr = filtersJsonObj.get("SourceRef").toString();
                                    filtersJsonObj = new JSONObject(filtersJsonArr);
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
//            exceptionLog.append("Exception In getPowerBIReportDataTransformsInfo() \n" + exception.toString());
//            exceptionLog.append("\n ================================");
        }
        return reportDataTransformsInfoMap;
    }
}
