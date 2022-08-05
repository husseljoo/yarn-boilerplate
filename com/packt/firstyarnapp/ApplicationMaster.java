import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;


public class ApplicationMaster {
    public static void main(String[] args) throws IOException, YarnException, InterruptedException {
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
                final Path jarPath = new Path("hdfs://namenode:9000/user/root/actual-application/ActualApplication.jar");
                FileStatus jarStat = FileSystem.get(conf).getFileStatus(jarPath);
                LocalResource actualAppJar = Records.newRecord(LocalResource.class);
                actualAppJar.setResource(ConverterUtils.getYarnUrlFromPath(jarPath));
                actualAppJar.setSize(jarStat.getLen());
                actualAppJar.setTimestamp(jarStat.getModificationTime());
                actualAppJar.setType(LocalResourceType.FILE);
                actualAppJar.setVisibility(LocalResourceVisibility.PUBLIC);
                // Set up CLASSPATH for ApplicationMaster
                System.out.println("Setting environment");
                Map<String, String> applicationEnv = new HashMap<String, String>();
                for (String c : conf.getStrings(
                        YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                        YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
                    Apps.addToEnvironment(applicationEnv, ApplicationConstants.Environment.CLASSPATH.name(), c.trim());
                }
                Apps.addToEnvironment(applicationEnv, ApplicationConstants.Environment.CLASSPATH.name(), ApplicationConstants.Environment.PWD.$() + File.separator + "*");
                ctx.setLocalResources(Collections.singletonMap("ActualApplication.jar", actualAppJar));
                ctx.setEnvironment(applicationEnv);

                final String shellCommandd = "$JAVA_HOME/bin/java -jar ActualApplication.jar";

                ctx.setCommands(Collections.singletonList(shellCommandd + " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR +
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
