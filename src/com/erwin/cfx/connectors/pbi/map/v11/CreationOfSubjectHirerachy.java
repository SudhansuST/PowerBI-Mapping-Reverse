/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.connectors.pbi.map.v11;

import com.ads.api.beans.common.AuditHistory;
import com.ads.api.beans.common.Node;
import com.ads.api.beans.mm.Subject;
import com.ads.api.util.MappingManagerUtil;
import static com.erwin.cfx.connectors.pbi.map.v11.PowerBIReportParser.exceptionLog;
import com.icc.util.RequestStatus;
import java.io.PrintWriter;
import java.io.StringWriter;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

/**
 *
 * @author LakshmiKakarla
 */
public class CreationOfSubjectHirerachy {

    static Logger LOGGER = Logger.getLogger(CreationOfSubjectHirerachy.class);

    public static int parentSubjectId(String subjectName, MappingManagerUtil mappingManagerUtil, String projectName, int projectId) {
        int subId = 0;

        try {
            subjectName = FilenameUtils.normalizeNoEndSeparator(subjectName, true);
            String subjectNameArray[] = subjectName.split("/");
            int subjectCount = 0;

            for (String subjectHierarchyName : subjectNameArray) {
                if (subjectCount == 0 && !StringUtils.isBlank(subjectHierarchyName)) {
                    createSubject(projectId, mappingManagerUtil, subjectHierarchyName);
                    subId = mappingManagerUtil.getSubjectId(projectName, subjectHierarchyName);
                } else {
                    if (!StringUtils.isBlank(subjectHierarchyName)) {
                        createChildSubject(subjectHierarchyName, projectId, subId, mappingManagerUtil);
                        try {
                            if (subId > 0) {
                                subId = mappingManagerUtil.getSubjectId(subId, Node.NodeType.MM_SUBJECT, subjectHierarchyName);
                            }

                        } catch (Exception e) {
                            LOGGER.error(e);
                            StringWriter exception = new StringWriter();
                            e.printStackTrace(new PrintWriter(exception));
                            exceptionLog.append("Exception In parentSubjectId() \n" + exception.toString());
                            exceptionLog.append("\n ================================");
                        }
                    }

                }
                if (!StringUtils.isBlank(subjectHierarchyName)) {
                    subjectCount++;
                }

            }

        } catch (Exception e) {
            LOGGER.error(e);
            StringWriter exception = new StringWriter();
            e.printStackTrace(new PrintWriter(exception));
            exceptionLog.append("Exception In parentSubjectId() \n" + exception.toString());
            exceptionLog.append("\n ================================");
        }
        return subId;
    }

    public static int parentSubjectId(String subjectName, MappingManagerUtil mappingManagerUtil, String projectName, int projectId, int ParentSubjectId) {
        int subId = 0;
        subId = ParentSubjectId;

        try {
            subjectName = FilenameUtils.normalizeNoEndSeparator(subjectName, true);
            String subjectNameArray[] = subjectName.split("/");
            int subjectCount = 0;

            for (String subjectHierarchyName : subjectNameArray) {
                if (StringUtils.isBlank(subjectHierarchyName)) {
                    continue;
                }
                if (subjectCount == 0 && !StringUtils.isBlank(subjectHierarchyName) && ParentSubjectId <= 0) {
                    createSubject(projectId, mappingManagerUtil, subjectHierarchyName);
                    subId = mappingManagerUtil.getSubjectId(projectName, subjectHierarchyName);
                } else {
                    if (!StringUtils.isBlank(subjectHierarchyName)) {
                        createChildSubject(subjectHierarchyName, projectId, subId, mappingManagerUtil);
                        try {
                            if (subId > 0) {
                                subId = mappingManagerUtil.getSubjectId(subId, Node.NodeType.MM_SUBJECT, subjectHierarchyName);
                            }

                        } catch (Exception e) {
                            LOGGER.error(e);
                            StringWriter exception = new StringWriter();
                            e.printStackTrace(new PrintWriter(exception));
                            exceptionLog.append("Exception In parentSubjectId() \n" + exception.toString());
                            exceptionLog.append("\n ================================");
                        }
                    }

                }
                if (!StringUtils.isBlank(subjectHierarchyName)) {
                    subjectCount++;
                }

            }

        } catch (Exception e) {
            LOGGER.error(e);
            StringWriter exception = new StringWriter();
            e.printStackTrace(new PrintWriter(exception));
            exceptionLog.append("Exception In parentSubjectId() \n" + exception.toString());
            exceptionLog.append("\n ================================");
        }
        return subId;
    }

    public static String createSubject(int projectId, MappingManagerUtil mappingManagerUtil, String fileName) {
        StringBuilder sb = new StringBuilder();
        String projectName = "";
        int subId = -1;
        try {
            projectName = mappingManagerUtil.getProject(projectId).getProjectName();
            subId = mappingManagerUtil.getSubjectId(projectName, fileName);
            if (subId > 0) {
                sb.append("subject creation msg :" + "Subject Is Already Avaliable. Subject Name : " + fileName + "\n\n");
            } else {
                Subject subjectDetails = new Subject();

                subjectDetails.setSubjectName(fileName);
                subjectDetails.setSubjectDescription("Description");
                AuditHistory auditHistory = new AuditHistory();
                auditHistory.setCreatedBy("Administrator");
                subjectDetails.setAuditHistory(auditHistory);
                subjectDetails.setProjectId(projectId);
                subjectDetails.setConsiderUserDefinedFlag("Y");
                subjectDetails.setParentSubjectId(-1);
                RequestStatus retRS = mappingManagerUtil.createSubject(subjectDetails);
                sb.append("subject creation msg " + retRS.getStatusMessage() + "\n\n");
                try {
                    if (retRS.isRequestSuccess() == true) {

                        subId = mappingManagerUtil.getSubjectId(projectName, fileName);
//                        mainSubjectId = subId;
                    }
                } catch (Exception e) {
                    LOGGER.error(e);
                    StringWriter exception = new StringWriter();
                    e.printStackTrace(new PrintWriter(exception));
                    exceptionLog.append("Exception In createSubject() \n" + exception.toString());
                    exceptionLog.append("\n ================================");
                }
            }
        } catch (Exception e) {
            LOGGER.error(e);
            StringWriter exception = new StringWriter();
            e.printStackTrace(new PrintWriter(exception));
            exceptionLog.append("Exception In createSubject() \n" + exception.toString());
            exceptionLog.append("\n ================================");
        }

        return sb.toString();
    }

    public static String createChildSubject(String name, int projectId, int subId, MappingManagerUtil mappingManagerUtil) {
        StringBuilder sb = new StringBuilder();
        String projectName = "";

        int chileSubjectId = -1;
        try {
            projectName = mappingManagerUtil.getProject(projectId).getProjectName();
            try {
                if (subId > 0) {
                    chileSubjectId = mappingManagerUtil.getSubjectId(subId, Node.NodeType.MM_SUBJECT, name);
                } else {
                    chileSubjectId = mappingManagerUtil.getSubjectId(projectName, name);
                }

            } catch (Exception e) {
                LOGGER.error(e);
                StringWriter exception = new StringWriter();
                e.printStackTrace(new PrintWriter(exception));
                exceptionLog.append("Exception In createChildSubject() \n" + exception.toString());
                exceptionLog.append("\n ================================");
            }

            if (chileSubjectId > 0) {
                sb.append("SubjectName : " + name + " Already Avalible." + "\n\n");
            } else {
                Subject subjectDetails = new Subject();

                subjectDetails.setSubjectName(name);
                subjectDetails.setSubjectDescription("Description");
                AuditHistory auditHistory = new AuditHistory();
                auditHistory.setCreatedBy("Administrator");
                subjectDetails.setAuditHistory(auditHistory);
                subjectDetails.setProjectId(projectId);
                subjectDetails.setConsiderUserDefinedFlag("Y");
                if (subId > 0) {
                    subjectDetails.setParentSubjectId(subId);
                }

                try {
                    RequestStatus retRS = mappingManagerUtil.createSubject(subjectDetails);
                    sb.append(name + " " + retRS.getStatusMessage() + "\n\n");
                } catch (Exception e) {
                    LOGGER.error(e);
                    StringWriter exception = new StringWriter();
                    e.printStackTrace(new PrintWriter(exception));
                    exceptionLog.append("Exception In createChildSubject() \n" + exception.toString());
                    exceptionLog.append("\n ================================");
                }
            }
        } catch (Exception e) {
            LOGGER.error(e);
            StringWriter exception = new StringWriter();
            e.printStackTrace(new PrintWriter(exception));
            exceptionLog.append("Exception In createChildSubject() \n" + exception.toString());
            exceptionLog.append("\n ================================");
        }

        return sb.toString();
    }

}
