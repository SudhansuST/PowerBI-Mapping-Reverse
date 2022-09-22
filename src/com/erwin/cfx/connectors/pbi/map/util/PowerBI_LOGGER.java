/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.connectors.pbi.map.util;

import com.erwin.cfx.connectors.pbi.map.v11.PowerBI_Constanst;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;

/**
 *
 * @author STarai
 */
public class PowerBI_LOGGER {

    public static StringBuilder detailLogger;
    public static StringBuilder overviewLogger;

    public static String getTimestampForLogging() {
        String timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime());
        return "[" + timeStamp + "]::";
    }

    public static void initializeLogger(Map<String, String> optionProperties) {
        detailLogger = new StringBuilder();
        detailLogger.append(getTimestampForLogging() + "Header::Input Parameter::ProjectName = " + optionProperties.get(PowerBI_Constanst.OPTION_KEY_PROJECT_NAME) + "\n");
        detailLogger.append(getTimestampForLogging() + "Header::Input Parameter::Mapping Root SubjectName = " + optionProperties.get(PowerBI_Constanst.OPTION_KEY_UPLOAD_SUBJECT) + "\n");
        detailLogger.append(getTimestampForLogging() + "Header::Input Parameter::Metadata Creation=" + optionProperties.get(PowerBI_Constanst.OPTION_KEY_CHECK_METADATA_CREATION) + "\n");
        detailLogger.append(getTimestampForLogging() + "Header::Input Parameter::Sync Source/Target Metadata=" + optionProperties.get(PowerBI_Constanst.OPTION_KEY_SYNC_STATUS) + "\n");
        detailLogger.append(getTimestampForLogging() + "Header::Input Parameter::Mapping Version Control=" + optionProperties.get(PowerBI_Constanst.OPTION_KEY_MAPPING_VERSION) + "\n");
        detailLogger.append(getTimestampForLogging() + "Header::Input Parameter::Delete/Archive Source File=" + optionProperties.get(PowerBI_Constanst.OPTION_KEY_ARCHIVE_FOLDER_PATH) + "\n");
        
        overviewLogger = new StringBuilder();
        overviewLogger.append(getTimestampForLogging() + "Header::Input Parameter::ProjectName = " + optionProperties.get(PowerBI_Constanst.OPTION_KEY_PROJECT_NAME) + "\n");
        overviewLogger.append(getTimestampForLogging() + "Header::Input Parameter::Mapping Root SubjectName = " + optionProperties.get(PowerBI_Constanst.OPTION_KEY_UPLOAD_SUBJECT) + "\n");
        overviewLogger.append(getTimestampForLogging() + "Header::Input Parameter::Metadata Creation=" + optionProperties.get(PowerBI_Constanst.OPTION_KEY_CHECK_METADATA_CREATION) + "\n");
        overviewLogger.append(getTimestampForLogging() + "Header::Input Parameter::Sync Source/Target Metadata=" + optionProperties.get(PowerBI_Constanst.OPTION_KEY_SYNC_STATUS) + "\n");
        overviewLogger.append(getTimestampForLogging() + "Header::Input Parameter::Mapping Version Control=" + optionProperties.get(PowerBI_Constanst.OPTION_KEY_MAPPING_VERSION) + "\n");
        overviewLogger.append(getTimestampForLogging() + "Header::Input Parameter::Delete/Archive Source File=" + optionProperties.get(PowerBI_Constanst.OPTION_KEY_ARCHIVE_FOLDER_PATH) + "\n");
    }

}
