/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.connectors.pbi.map.util;

import com.erwin.cfx.connectors.pbi.map.pojo.PowerBI_Bean;
import com.ads.api.beans.mm.MappingSpecificationRow;
import com.ads.api.util.MappingManagerUtil;
import static com.erwin.cfx.connectors.pbi.map.v11.PowerBIReportParser.unzip;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.MappingJsonFactory;
import org.codehaus.jackson.node.ArrayNode;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 *
 * @author STarai
 */
public class PowerBI_DatasetUtil {

    public Map<String, String> datasetNameAgainstID = new HashMap<>();

    public Map<String, ArrayList<MappingSpecificationRow>> prepareDatasetInfo(PowerBI_Bean bean, Set<File> datasetFiles, String fileSaperator, MappingManagerUtil mappingManagerUtil, String filePath_param, String zipDir_param) {
        Map<String, ArrayList<MappingSpecificationRow>> specAgainstDatasetID = new HashMap<>();
        File targetDir = null;
        try {
            File sourceFile = null;
            String filePath = filePath_param;
            String zipDir = zipDir_param;
            for (File file : datasetFiles) {

                sourceFile = file;
                String absolutefilePath = file.getAbsolutePath();
                String zipDirPath = sourceFile.getPath();
                String zipFileName = zipDirPath.substring(zipDirPath.lastIndexOf(fileSaperator) + 1);
                zipFileName = zipFileName.replace(".pbix", "").replace(".pbit", "");
                filePath = FilenameUtils.normalizeNoEndSeparator(filePath, true);
                filePath = filePath + "/";

                absolutefilePath = FilenameUtils.normalizeNoEndSeparator(absolutefilePath, true);
                absolutefilePath = absolutefilePath.replace(filePath, "");

                absolutefilePath = absolutefilePath.replace(zipFileName, "").replace(".pbix", "").replace(".pbit", "").trim();

                String indivisualZipDir = zipDir + "/" + zipFileName + "/";
                targetDir = new File(indivisualZipDir);
                if (!targetDir.isDirectory()) {
                    targetDir.mkdir();
                }
                String targetZipFile = targetDir + "/" + zipFileName + ".zip";

                File connectionFile = new File(targetDir + "/Connections");
                File datamodelSchemaFile = new File(targetDir + "/DataModelSchema");
                FileUtils.copyFile(sourceFile, new File(targetDir + "/" + zipFileName + ".zip"));
                unzip(targetZipFile, indivisualZipDir);
                if (connectionFile.exists()) {
                    String connectionJSON = FileUtils.readFileToString(connectionFile);
                    JSONObject connJSON = new JSONObject(connectionJSON);
                    if (connJSON.has("RemoteArtifacts")) {
                        JSONArray artifactsArray = connJSON.getJSONArray("RemoteArtifacts");
                        for (int i = 0; i < artifactsArray.length(); i++) {
                            JSONObject eachArtifactIndex = artifactsArray.getJSONObject(i);
                            if (eachArtifactIndex.has("DatasetId")) {
                                String datasetID = eachArtifactIndex.getString("DatasetId");
                                bean.setPOWERBI_DATASET_ID(datasetID);
                                datasetNameAgainstID.put(datasetID, zipFileName);
                            }
                        }

                    }
                }

                if (bean.getPOWERBI_DATASET_ID() != null && !bean.getPOWERBI_DATASET_ID().equals("") && datamodelSchemaFile.exists()) {
                    Map<String, Set<String>> TableColumnMap = getDataModelTableColumnInfo(datamodelSchemaFile);
                    ArrayList<MappingSpecificationRow> mapspecs = getDatasetSpecifications(zipFileName, TableColumnMap, bean);
                    specAgainstDatasetID.put(bean.getPOWERBI_DATASET_ID(), mapspecs);

                }
                FileUtils.deleteQuietly(targetDir);
            }
        } catch (Exception ex) {
            FileUtils.deleteQuietly(targetDir);
        }
        bean.setDatasetNameAgainstID(datasetNameAgainstID);
        return specAgainstDatasetID;
    }

    public ArrayList<MappingSpecificationRow> getDatasetSpecifications(String datasetName, Map<String, Set<String>> TableColumnMap, PowerBI_Bean bean) {
        ArrayList<MappingSpecificationRow> mapspecs = new ArrayList<>();
        for (Map.Entry<String, Set<String>> entry : TableColumnMap.entrySet()) {
            String tableName = entry.getKey();
            Set<String> columns = entry.getValue();

            for (String column : columns) {

                MappingSpecificationRow mapspec = new MappingSpecificationRow();
                mapspec.setSourceSystemName(bean.getPOWERBI_SYSTEM());
                mapspec.setSourceSystemEnvironmentName("DATASET");
                mapspec.setSourceTableName(tableName);
                mapspec.setSourceColumnName(column);
                mapspec.setTargetSystemName(bean.getPOWERBI_SYSTEM());
                mapspec.setTargetSystemEnvironmentName(bean.getPOWERBI_ENV_PAGE());
                mapspec.setTargetTableName(datasetName);
                mapspec.setTargetColumnName(column);
                mapspecs.add(mapspec);
            }
        }

        return mapspecs;
    }

    public Map<String, Set<String>> getDataModelTableColumnInfo(File datamodelSchemaFile) {

        Set<String> columnsList = null;
        Map<String, Set<String>> TableColumnMap = new HashMap<>();
        try {
            JsonFactory f = new MappingJsonFactory();
            JsonParser jp = f.createJsonParser(datamodelSchemaFile);
            JsonNode parsernode = jp.readValueAsTree();
            if (parsernode.has("model")) {
                JsonNode modelNode = parsernode.get("model");
                if (modelNode.has("tables")) {
                    columnsList = new HashSet<>();
                    ArrayNode tableArray = (ArrayNode) modelNode.get("tables");
                    for (int i = 0; i < tableArray.size(); i++) {
                        JsonNode jsonTableNode = tableArray.get(i);
                        String tableName = jsonTableNode.get("name").getTextValue().toUpperCase();
                        if (tableName.contains("DateTableTemplate".toUpperCase())) {
                            continue;
                        } else if (tableName.contains("LocalDateTable".toUpperCase())) {
                            continue;
                        }
                        if (jsonTableNode.has("columns")) {
                            JSONArray columnArray = new JSONArray(jsonTableNode.get("columns").toString());
                            for (int j = 0; j < columnArray.length(); j++) {
                                JSONObject columnJSON = columnArray.getJSONObject(j);
                                if (columnJSON.has("name")) {
                                    columnsList.add(columnJSON.getString("name"));
                                }
                            }
                        }
                        TableColumnMap.put(tableName, columnsList);
                    }
                }
            }
            jp.close();
        } catch (Exception ex) {

        }
        return TableColumnMap;
    }

}
