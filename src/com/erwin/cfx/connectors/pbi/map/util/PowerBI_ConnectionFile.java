/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.erwin.cfx.connectors.pbi.map.util;

import java.io.File;
import org.apache.commons.io.FileUtils;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 *
 * @author STarai
 */
public class PowerBI_ConnectionFile {

    public String connectionEnvName = "";

    public boolean readConnectionFile(File connectionFile) {

        try {
            String connectionInfo = FileUtils.readFileToString(connectionFile);
            JSONObject connectionJSON = new JSONObject(connectionInfo);
            if (connectionJSON.has("Connections")) {
                JSONArray connectionArray = connectionJSON.getJSONArray("Connections");
                for (int i = 0; i < connectionArray.length(); i++) {
                    JSONObject connection = connectionArray.getJSONObject(i);
                    String connectionString = connection.getString("ConnectionString");
                    if (connectionString.contains("Initial Catalog=")) {
                        connectionString = connectionString.split("Initial Catalog=")[1].split(";")[0];
                        connectionEnvName = connectionString;
                        return true;
                    }
                }
            }

        } catch (Exception ex) {
            return false;
        }
        return false;
    }

}
