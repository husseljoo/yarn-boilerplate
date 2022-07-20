import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;


public class ApplicationMaster {
    public static void main(String[] args) {
        // Read Yarn configuration and input arguments
        System.out.println("Running ApplicationMaster...");
        final String shellCommand = args[0];
        final int numOfContainers = Integer.valueOf(args[1]);
        Configuration conf = new YarnConfiguration();

        // Initialize the AMRMClient and NMClient clients
        System.out.println("Initializing AMRMCLient");
        AMRMClient<ContainerRequest> rmClient = AMRMClient.createAMRMClient();
        rmClient.init(conf);
        rmClient.start();
        System.out.println("Initializing NMCLient");
        NMClient nmClient = NMClient.createNMClient();
        nmClient.init(conf);
        nmClient.start();

        // Register attempt with resource manager
        System.out.println("Register ApplicationMaster");
        rmClient.registerApplicationMaster(NetUtils.getHostname(), 0, "");

        // Define ContainerRequest and add the contianer's request
        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(0);
        System.out.println("Setting Resource capability for Containers");
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(128);
        capability.setVirtualCores(1);
        for (int i = 0; i < numOfContainers; ++i) {
            ContainerRequest containerRequested = new ContainerRequest(capability, null, null, priority, true);
            // Resource, nodes, racks, priority and relax locality flag
            rmClient.addContainerRequest(containerRequested);
        }

        // Request allocation, define ContainerLaunchContext and start the containers
        int allocatedContainers = 0;
        while (allocatedContainers < numOfContainers) {
            AllocateResponse response = rmClient.allocate(0);
            for (Container container : response.getAllocatedContainers()) {
                ++allocatedContainers;
                // Launch container by creating ContainerLaunchContext
                ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
                ctx.setCommands(Collections.singletonList(shellCommand + " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR +
                        "/stdout" + " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"));
                nmClient.startContainer(container, ctx);
            }
            Thread.sleep(100);
        }

        //On completion, unregister ApplicationMaster from ResourceManager
        int completedContainers = 0;
        while (completedContainers < numOfContainers) {
            AllocateResponse response = rmClient.allocate(completedContainers / numOfContainers);
            for (ContainerStatus status : response.getCompletedContainersStatuses()) {
                ++completedContainers;
                System.out.println("Completed container " + completedContainers);
            }
            Thread.sleep(100);
        }
        rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");

    }
}
