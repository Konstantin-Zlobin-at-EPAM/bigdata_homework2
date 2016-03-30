package com.epam.bigdata.homework2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * This class implements a simple async app master.
 * In real usages, the callbacks should execute in a separate thread or thread pool
 */
public class ApplicationMasterAsync implements AMRMClientAsync.CallbackHandler {
    Configuration configuration;
    NMClient nmClient;
    String mainClass;
    int numContainersToWaitFor;

    public ApplicationMasterAsync(String mainClass, int numContainersToWaitFor) {
        this.mainClass = mainClass;
        configuration = new YarnConfiguration();
        this.numContainersToWaitFor = numContainersToWaitFor;
        nmClient = NMClient.createNMClient();
        nmClient.init(configuration);
        nmClient.start();
    }

    public void onContainersAllocated(List<Container> containers) {
        for (Container container : containers) {
            try {
                // Launch container by create ContainerLaunchContext
                ContainerLaunchContext ctx =
                        Records.newRecord(ContainerLaunchContext.class);
                Path jarPath = new Path("./homework2.jar");
                LocalResource containerJobsJar = Records.newRecord(LocalResource.class);
                FileStatus jarStat = FileSystem.get(configuration).getFileStatus(jarPath);
                containerJobsJar.setResource(ConverterUtils.getYarnUrlFromPath(jarPath));
                containerJobsJar.setSize(jarStat.getLen());
                containerJobsJar.setTimestamp(jarStat.getModificationTime());
                containerJobsJar.setType(LocalResourceType.FILE);
                containerJobsJar.setVisibility(LocalResourceVisibility.APPLICATION);
                ctx.setLocalResources(
                        Collections.singletonMap("homework2.jar", containerJobsJar));
                ctx.setCommands(
                        Collections.singletonList(
                                "$JAVA_HOME/bin/java" +
                                        " -jar ./homework2.jar" +
                                        " " + mainClass +
                                        " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
                                        " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
                        ));
                System.out.println("[AM] Launching container " + container.getId());
                nmClient.startContainer(container, ctx);
            } catch (Exception ex) {
                System.err.println("[AM] Error launching container " + container.getId() + " " + ex);
            }
        }
    }

    public void onContainersCompleted(List<ContainerStatus> statuses) {
        for (ContainerStatus status : statuses) {
            System.out.println("[AM] Completed container " + status.getContainerId());
            synchronized (this) {
                numContainersToWaitFor--;
            }
        }
    }

    public void onNodesUpdated(List<NodeReport> updated) {
    }

    public void onReboot() {
    }

    public void onShutdownRequest() {
    }

    public void onError(Throwable t) {
    }

    public float getProgress() {
        return 0;
    }

    public boolean doneWithContainers() {
        return numContainersToWaitFor == 0;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public static void main(String[] args) throws Exception {
        final String command = args[0];
        final int n = Integer.valueOf(args[1]);

        ApplicationMasterAsync master = new ApplicationMasterAsync(command, n);
        master.runMainLoop();

    }

    public void runMainLoop() throws Exception {

        AMRMClientAsync<ContainerRequest> rmClient = AMRMClientAsync.createAMRMClientAsync(100, this);
        rmClient.init(getConfiguration());
        rmClient.start();

        // Register with ResourceManager
        System.out.println("[AM] registerApplicationMaster 0");
        rmClient.registerApplicationMaster("", 0, "");
        System.out.println("[AM] registerApplicationMaster 1");

        // Priority for worker containers - priorities are intra-application
        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(0);

        // Resource requirements for worker containers
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(512);
        capability.setVirtualCores(4);

        // Make container requests to ResourceManager
        for (int i = 0; i < numContainersToWaitFor; ++i) {
            ContainerRequest containerAsk = new ContainerRequest(capability, null, null, priority);
            System.out.println("[AM] Making res-req " + i);
            rmClient.addContainerRequest(containerAsk);
        }

        System.out.println("[AM] waiting for containers to finish");
        while (!doneWithContainers()) {
            Thread.sleep(100);
        }

        System.out.println("[AM] unregisterApplicationMaster 0");
        // Un-register with ResourceManager
        rmClient.unregisterApplicationMaster(
                FinalApplicationStatus.SUCCEEDED, "", "");
        System.out.println("[AM] unregisterApplicationMaster 1");
    }
}
