/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.connectors.pbi.map.v11;

import com.ads.api.beans.common.Node;
import com.ads.api.beans.mm.Subject;
import com.ads.api.util.MappingManagerUtil;
import com.erwin.cfx.connectors.pbi.map.pojo.PowerBI_Bean;
import java.io.File;

/**
 *
 * @author STarai
 */
public class Create_MMSubject_Hierarchy {

    int parentSubjectId = -1;
    PowerBI_Bean bean = null;

    public void createSubject(String directoryName, int projectID, MappingManagerUtil mmutil) {
        int subjectId = -1;
        try {
            subjectId = mmutil.getSubjectId(parentSubjectId, Node.NodeType.MM_SUBJECT, directoryName);

            if (subjectId > 0) {
                parentSubjectId = subjectId;
            } else {
                Subject subject = new Subject();
                subject.setSubjectName(directoryName);
                subject.setParentSubjectId(parentSubjectId);
                subject.setProjectId(projectID);
                mmutil.createSubject(subject).getStatusMessage();
                parentSubjectId = mmutil.getSubjectId(parentSubjectId, Node.NodeType.MM_SUBJECT, directoryName);
            }
        } catch (Exception ex) {

        }
    }

    public void extractFolderFiles(File folderPath, int projectID, MappingManagerUtil mmutil) {
        if (folderPath.isDirectory()) {
            for (File file : folderPath.listFiles()) {
                if (file.isDirectory()) {
                    String directoryName = file.getName();
                    createSubject(directoryName, projectID, mmutil);
                }
            }
        } 
//        else {
//
//        }

    }

}
