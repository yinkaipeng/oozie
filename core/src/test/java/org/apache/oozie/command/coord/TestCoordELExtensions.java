package org.apache.oozie.command.coord;

import java.io.File;
import java.util.Date;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.service.Services;
import org.apache.oozie.store.CoordinatorStore;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.DateUtils;

public class TestCoordELExtensions extends XDataTestCase{
    private Services services;
    private String[] excludedServices = { "org.apache.oozie.service.StatusTransitService",
            "org.apache.oozie.service.PauseTransitService",
            "org.apache.oozie.service.RecoveryService" };

    @Override
    protected void setUp() throws Exception {
        setSystemProperty("oozie.test.config.file",
                new File(OOZIE_SRC_DIR, "core/src/test/resources/oozie-site-coordel.xml").getAbsolutePath());
        super.setUp();
        services = new Services();
        setClassesToBeExcluded(services.getConf(), excludedServices);
        services.init();
        cleanUpDBTables();
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    public void testCoordELActionMater() throws Exception {
        Date startTime = DateUtils.parseDateUTC("2009-03-06T010:00Z");
        Date endTime = DateUtils.parseDateUTC("2009-03-11T10:00Z");
        CoordinatorJobBean job = createCoordJob("coord-job-for-elext.xml", CoordinatorJob.Status.PREMATER, startTime, endTime, false, false, 0);
        addRecordToCoordJobTable(job);

        new CoordActionMaterializeCommand(job.getId(), startTime, endTime).call();
        checkCoordAction(job.getId() + "@1");
    }

    protected CoordinatorActionBean checkCoordAction(String actionId) throws StoreException {
        CoordinatorStore store = new CoordinatorStore(false);
        try {
            CoordinatorActionBean action = store.getCoordinatorAction(actionId, false);
            return action;
        }
        catch (StoreException se) {
            se.printStackTrace();
            fail("Action ID " + actionId + " was not stored properly in db");
        }
        return null;
    }
}
