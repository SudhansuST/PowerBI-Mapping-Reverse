/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.connectors.pbi.map.util;

import java.util.Map;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author SudhansuTarai
 */
public class PowerBIMetadataBuilder {
    
    /**
     * This method extract the PowerBI datatypes.
     * @param configJsonObj
     * @param powerBIMetadata
     * @throws JSONException 
     */
    
    public static Logger logger=Logger.getLogger(PowerBIMetadataBuilder.class);

    public static void getMetadataInfo(JSONObject configJsonObj, Map<String, String> powerBIMetadata) throws JSONException {
        
        String selectJsonArr = "";
        String filtersJsonArr = "";
        JSONObject selectJsonObj = null;
        JSONArray selectJsonObjArr = null;

        if (configJsonObj.has("queryMetadata")) {
            JSONObject queryMetadataJson = (JSONObject) configJsonObj.get("queryMetadata");
            if (queryMetadataJson.has("Select")) {
                selectJsonArr = queryMetadataJson.get("Select").toString();
                selectJsonObjArr = new JSONArray(selectJsonArr);
                for (int selectArr_i = 0; selectArr_i < selectJsonObjArr.length(); selectArr_i++) {
                    selectJsonObj = selectJsonObjArr.getJSONObject(selectArr_i);
                    powerBIMetadata.put(selectJsonObj.getString("Restatement"),selectJsonObj.getString("Type"));
                }
            }
        }

    }

}
