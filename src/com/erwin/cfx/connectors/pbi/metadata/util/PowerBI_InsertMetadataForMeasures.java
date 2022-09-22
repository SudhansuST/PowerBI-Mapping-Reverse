package com.erwin.cfx.connectors.pbi.metadata.util;

import com.ads.api.beans.sm.SMSystem;
import com.erwin.cfx.connectors.pbi.map.pojo.PowerBI_Bean;

/**
 *
 * @author SudhansuTarai
 */
public class PowerBI_InsertMetadataForMeasures {

    public int getPowerBISystem(PowerBI_Bean bean) {
        int systemId = 0;
        try {
            systemId = bean.getSystemManagerUtil().getSystemId(bean.getPOWERBI_SYSTEM());
            if (systemId <= 0) {
                SMSystem system = new SMSystem();
                system.setSystemName(bean.getPOWERBI_SYSTEM());
                String systemCreationStatus = bean.getSystemManagerUtil().createSystem(system).getStatusMessage();
            }
        } catch (Exception ex) {

        }
        return systemId;
    }

}
