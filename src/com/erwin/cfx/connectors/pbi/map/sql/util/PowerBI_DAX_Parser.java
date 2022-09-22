/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.connectors.pbi.map.sql.util;

import com.ads.api.beans.mm.MappingSpecificationRow;
import com.erwin.cfx.connectors.pbi.map.pojo.PowerBI_Bean;
import com.erwin.cfx.connectors.pbi.map.util.ReadXMLFileToObject;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author STarai
 */
public class PowerBI_DAX_Parser {

    public void dax_ParseDAXQuery(String query, String targetTableName, PowerBI_Bean bean) {

        ArrayList<MappingSpecificationRow> rows = new ArrayList<>();

        Pattern pattern = Pattern.compile("[\\'a-zA-Z0-9\\s\\_\\-']*\\[.*?\\]");
        Matcher matcher = pattern.matcher(query);

        while (matcher.find()) {
            String matchString = matcher.group();
            matchString = matchString.replace("'", "");
            String tableName = matchString.split("\\[")[0].trim();
            String columnName = matchString.substring(matchString.indexOf("[") + 1, matchString.indexOf("]"));
            rows.add(getMappingSpecification(targetTableName, columnName, targetTableName, columnName, bean));
        }

        ReadXMLFileToObject.MergeQuerySpecs.put(targetTableName, rows);
    }

    public MappingSpecificationRow getMappingSpecification(String sourceTableName, String sourceColumnName, String targetTableName, String targetColumnName, PowerBI_Bean bean) {

        MappingSpecificationRow row = new MappingSpecificationRow();

        row.setSourceSystemName(bean.getSQL_SYSTEM_NAME());
        row.setSourceSystemEnvironmentName(bean.getSQL_ENVIRONMENT_NAME());
        row.setSourceTableName(sourceTableName);
        row.setSourceColumnName(sourceColumnName);
        row.setTargetSystemName(bean.getSQL_SYSTEM_NAME());
        row.setTargetSystemEnvironmentName(bean.getSQL_ENVIRONMENT_NAME());
        row.setTargetTableName(targetTableName);
        row.setTargetColumnName(targetColumnName);

        return row;

    }

   
}
