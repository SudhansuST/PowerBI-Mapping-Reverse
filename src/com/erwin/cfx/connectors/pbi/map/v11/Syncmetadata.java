package com.erwin.cfx.connectors.pbi.map.v11;

import com.ads.api.beans.common.APIConstants;
import com.ads.api.beans.common.Node;
import com.ads.api.beans.mm.Mapping;
import com.ads.api.beans.mm.MappingSpecificationRow;
import com.ads.api.beans.sm.SMEnvironment;
import com.ads.api.beans.sm.SMSystem;
import com.ads.api.beans.sm.SMTable;
import com.ads.api.util.MappingManagerUtil;
import com.ads.api.util.SystemManagerUtil;
import static com.erwin.cfx.connectors.pbi.map.v11.PowerBIReportParser.exceptionLog;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

public class Syncmetadata {

    public static Logger logger = Logger.getLogger(Syncmetadata.class);

    public static Mapping metadataSync(Map<String, String> envMap, Mapping mapping, SystemManagerUtil smutill, int projectid, int subjectid, String folderName, Map<String, String> tabMap) {
        String jsonvalue = "";
        try {
            ArrayList<MappingSpecificationRow> mapSPecsLists = mapping.getMappingSpecifications();
            String mapName = mapping.getMappingName();
            String storproc = mapName.substring(0, mapName.lastIndexOf("."));
            String storprocName = storproc.replaceAll("[0-9]", "");
            for (MappingSpecificationRow mapSPecs : mapSPecsLists) {
                String Sourcetablename = mapSPecs.getSourceTableName();
                sourceNamesSet(Sourcetablename, envMap, mapSPecs, mapName, storprocName, folderName, tabMap);
                String Targettablename = mapSPecs.getTargetTableName();
                String[] trgtname = Targettablename.split("\n");
                for (String tgtName : trgtname) {
                    targetNameSet(tgtName, envMap, mapSPecs, mapName, storprocName, folderName, tabMap);
                }
            }
            String query = mapping.getSourceExtractQuery();
            Mapping mapingobj = createMapFromMappingSpecifiactionRow(mapSPecsLists, mapName, query, projectid, subjectid);
            return mapingobj;
        } catch (Exception ex) {
            logger.error(ex.getMessage());
            StringWriter exception = new StringWriter();
            ex.printStackTrace(new PrintWriter(exception));
            exceptionLog.append("Exception In metadataSync() \n" + exception.toString());
            exceptionLog.append("\n ================================");
        }
        return null;

    }

    public static void deleteMappings(MappingManagerUtil mmutil, String projectName) {
        try {
            int projectId = mmutil.getProjectId(projectName);
            mmutil.deleteMappings(projectId, Node.NodeType.MM_PROJECT, APIConstants.VersionMode.ALL_VERSIONS, 0.0f);
        } catch (Exception ex) {

        }
    }

    public static void sourceNamesSet(String Sourcetablename, Map<String, String> envMap, MappingSpecificationRow mapSPecs, String mapName, String storprocName, String folderName, Map<String, String> tabMap) {
        if (Sourcetablename.split("\n").length > 1) {
            String[] sourcetablecolumn = Sourcetablename.split("\n");
            List<String> sourcesystem = getSourceSystem(sourcetablecolumn, mapName, envMap, mapSPecs, storprocName, folderName);
            List<String> sourceenv = getSourceEnv(sourcetablecolumn, mapName, envMap, storprocName, folderName);
            List<String> sourcetab = getSourcetab(sourcetablecolumn, mapName, envMap, storprocName, folderName, tabMap);
            String sourceSystem = StringUtils.join(sourcesystem, "\n");
            String sourceEnv = StringUtils.join(sourceenv, "\n");
            String sourceTab = StringUtils.join(sourcetab, "\n");
            mapSPecs.setSourceSystemEnvironmentName(sourceEnv);
            mapSPecs.setSourceSystemName(sourceSystem);
        } else {
            if (envMap.containsKey(Sourcetablename)) {
                String sourceenvSys = envMap.get(Sourcetablename);
                if (setsrcSysenvNames(Sourcetablename, mapName) == 0) {
                    String SystemName = sourceenvSys.split("#")[1];
                    String environmentName = sourceenvSys.split("#")[0];
                    String tableName = tabMap.get(Sourcetablename);
                    mapSPecs.setSourceSystemName(SystemName);
                    mapSPecs.setSourceSystemEnvironmentName(environmentName);
                    mapSPecs.setSourceTableName(tableName);

                } else {
                    mapSPecs.setSourceSystemEnvironmentName(folderName + "_" + storprocName);
                    mapSPecs.setSourceSystemName(folderName + "_" + storprocName);
                }
            } else {
                mapSPecs.setSourceSystemEnvironmentName(folderName + "_" + storprocName);
                mapSPecs.setSourceSystemName(folderName + "_" + storprocName);
            }
            if (Sourcetablename.toUpperCase().contains("RESULT_OF_") || Sourcetablename.toUpperCase().contains("INSERT-SELECT")) {
                mapSPecs.setSourceTableName(Sourcetablename + "_" + mapName);
            }

        }
    }

    public static void targetNameSet(String Targettablename, Map<String, String> envMap, MappingSpecificationRow mapSPecs, String mapName, String storprocName, String folderName, Map<String, String> tabMap) {

        if (envMap.containsKey(Targettablename)) {
            String targetenvSys = envMap.get(Targettablename);
            String SystemName = targetenvSys.split("#")[1];
            mapSPecs.setTargetSystemName(SystemName);
            String environmentName = targetenvSys.split("#")[0];
            mapSPecs.setTargetSystemEnvironmentName(environmentName);
            String tableName = tabMap.get(Targettablename);
            mapSPecs.setTargetTableName(tableName);
        } else {
            mapSPecs.setTargetSystemName(folderName + "_" + storprocName);
            mapSPecs.setTargetSystemEnvironmentName(folderName + "_" + storprocName);
        }
        if (Targettablename.toUpperCase().contains("RESULT_OF_") || Targettablename.toUpperCase().contains("INSERT-SELECT")) {
            mapSPecs.setTargetTableName(Targettablename + "_" + mapName);
        }

    }

    public static Map<String, String> metaDatacreation(SystemManagerUtil smutill) {
        Map<String, String> metaDatacreationmap = new HashMap();
        try {
            ArrayList<SMSystem> systems = smutill.getSystems();
            for (int i = 0; i < systems.size(); i++) {
                String sysName = systems.get(i).getSystemName();
                int sysid = systems.get(i).getSystemId();
                ArrayList<SMEnvironment> environments = smutill.getEnvironments(sysid);
                for (int j = 0; j < environments.size(); j++) {
                    int envid = environments.get(j).getEnvironmentId();
                    SMEnvironment environment = smutill.getEnvironment(envid);
                    String envName = environment.getSystemEnvironmentName();
                    SMEnvironment smEnv = smutill.getEnvironment(envid, true);
                    List<SMTable> envtables = smutill.getEnvironmentTables(envid);
                    for (int k = 0; k < envtables.size(); k++) {
                        String tableName = envtables.get(k).getTableName();
                        String envsys = envName + "#" + sysName;
                        metaDatacreationmap.put(tableName.toUpperCase(), envsys);
                    }
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
            StringWriter exception = new StringWriter();
            e.printStackTrace(new PrintWriter(exception));
            exceptionLog.append("Exception In metadataSync() \n" + exception.toString());
            exceptionLog.append("\n ================================");
        }
        return metaDatacreationmap;
    }

    public static int setsrcSysenvNames(String src_table_name, String mapName) {
        int skipFlag = 0;
        String Sourcetablename = src_table_name.toUpperCase();
        String schemaNm = "";
        if (Sourcetablename.contains(".")) {
            schemaNm = Sourcetablename.split("[.]")[0];
            Sourcetablename = Sourcetablename.split("[.]")[1] + "load";
        }
        if (!"dbo".equals(schemaNm) && !"".equals(schemaNm)) {
            Sourcetablename = schemaNm + "_" + Sourcetablename;
        }
        if (mapName.toUpperCase().contains(Sourcetablename)) {
            skipFlag = 1;
        }
        return skipFlag;
    }

    public static Mapping createMapFromMappingSpecifiactionRow(ArrayList<MappingSpecificationRow> specrowlist, String mapfileName, String query, int projectid, int subjectid) {
        Mapping mapping = new Mapping();
        mapping.setMappingName(mapfileName);
        mapping.setMappingSpecifications(specrowlist);
        mapping.setSourceExtractQuery(query);
        mapping.setProjectId(projectid);
        mapping.setSubjectId(subjectid);
        return mapping;
    }

    public static List<String> getSourceSystem(String[] sourcetablename, String mapname, Map<String, String> envMap, MappingSpecificationRow mapSPecs, String storprocName, String folderName) {
        List<String> sourcesystemlist = new LinkedList<>();
        try {
            for (String sourceTab : sourcetablename) {
                if (envMap.get(sourceTab) != null) {
                    String sourceenvSys = envMap.get(sourceTab);
                    if (setsrcSysenvNames(sourceTab, mapname) == 0) {
                        sourcesystemlist.add(sourceenvSys.split("#")[1]);
                    } else {
                        sourcesystemlist.add(folderName + "_" + storprocName);
                    }
                } else {
                    sourcesystemlist.add(folderName + "_" + storprocName);
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
            StringWriter exception = new StringWriter();
            e.printStackTrace(new PrintWriter(exception));
            exceptionLog.append("Exception In getSourceSystem() \n" + exception.toString());
            exceptionLog.append("\n ================================");
        }
        return sourcesystemlist;
    }

    public static List<String> getSourceEnv(String[] sourcetablename, String mapname, Map<String, String> envMap, String storprocName, String folderName) {
        List<String> sourceenvlist = new LinkedList<>();
        try {
            for (String sourceTab : sourcetablename) {
                if (envMap.get(sourceTab) != null) {
                    String sourceenvSys = envMap.get(sourceTab);
                    if (setsrcSysenvNames(sourceTab, mapname) == 0) {
                        sourceenvlist.add(sourceenvSys.split("#")[0]);
                    } else {
                        sourceenvlist.add(folderName + "_" + storprocName);
                    }
                } else {
                    sourceenvlist.add(folderName + "_" + storprocName);
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
            StringWriter exception = new StringWriter();
            e.printStackTrace(new PrintWriter(exception));
            exceptionLog.append("Exception In getSourceEnv() \n" + exception.toString());
            exceptionLog.append("\n ================================");
        }
        return sourceenvlist;
    }

    public static List<String> getSourcetab(String[] sourcetablename, String mapname, Map<String, String> envMap, String storprocName, String folderName, Map<String, String> tabMap) {
        List<String> sourcetablist = new LinkedList<>();
        try {
            for (String sourceTab : sourcetablename) {
                if (envMap.get(sourceTab) != null) {
                    String tableName = envMap.get(sourceTab);
                    sourcetablist.add(tableName);
                } else {
                    sourcetablist.add(sourceTab);
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
            StringWriter exception = new StringWriter();
            e.printStackTrace(new PrintWriter(exception));
            exceptionLog.append("Exception In getSourcetab() \n" + exception.toString());
            exceptionLog.append("\n ================================");
        }
        return sourcetablist;
    }

}
