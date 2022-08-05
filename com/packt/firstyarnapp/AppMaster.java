import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

/**
 * The ApplicationMaster, who is responsible for sending container
 * requests to the ResourceManager, and talking to NodeManagers when
 * the RM provides us with available containers.
 */
public class AppMaster {

    public static void main(String[] args) throws Exception {

        try {

            Configuration conf = new YarnConfiguration();

            // create a client to talk to the ResourceManager
            AMRMClient<ContainerRequest> rmClient = AMRMClient.createAMRMClient();
            rmClient.init(conf);
            rmClient.start();

            // create a client to talk to the NodeManagers
            NMClient nmClient = NMClient.createNMClient();
            nmClient.init(conf);
            nmClient.start();

            // register with ResourceManager
            System.out.println("registerApplicationMaster: pending");
            rmClient.registerApplicationMaster("", 0, "");
            System.out.println("registerApplicationMaster: complete");

            // Priority for worker containers - priorities are intra-application
            Priority priority = Records.newRecord(Priority.class);
            priority.setPriority(0);

            // Resource requirements for worker containers
            Resource capability = Records.newRecord(Resource.class);
            capability.setMemory(128);
            capability.setVirtualCores(1);

            // Make container requests to ResourceManager
            ContainerRequest containerAsk = new ContainerRequest(capability, null, null, priority);
            System.out.println("adding container ask:" + containerAsk);
            rmClient.addContainerRequest(containerAsk);

            //final String cmd = "/bin/date";
            final String cmd = "$JAVA_HOME/bin/java -Xmx256M ActualApplication.jar";

            // Obtain allocated containers and launch
            boolean allocatedContainer = false;
            while (!allocatedContainer) {
                System.out.println("allocate");
                AllocateResponse response = rmClient.allocate(0);
                for (Container container : response.getAllocatedContainers()) {
                    allocatedContainer = true;

                    // Launch container by create ContainerLaunchContext
                    ContainerLaunchContext ctx =
                            Records.newRecord(ContainerLaunchContext.class);

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
                    ctx.setLocalResources(Collections.singletonMap("YarnClient.jar", actualAppJar));
                    ctx.setEnvironment(applicationEnv);


                    ctx.setCommands(
                            Collections.singletonList(
                                    String.format("%s 1>%s/stdout 2>%s/stderr",
                                            cmd,
                                            ApplicationConstants.LOG_DIR_EXPANSION_VAR,
                                            ApplicationConstants.LOG_DIR_EXPANSION_VAR)
                            ));
                    System.out.println("Launching container " + container);
                    nmClient.startContainer(container, ctx);
                }
                TimeUnit.SECONDS.sleep(1);
            }

            // Now wait for containers to complete
            boolean completedContainer = false;
            while (!completedContainer) {
                System.out.println("allocate (wait)");
                AllocateResponse response = rmClient.allocate(0);
                for (ContainerStatus status : response.getCompletedContainersStatuses()) {
                    completedContainer = true;
                    System.out.println("Completed container " + status);
                }
                TimeUnit.SECONDS.sleep(1);
            }

            System.out.println("unregister");
            // Un-register with ResourceManager
            rmClient.unregisterApplicationMaster(
                    FinalApplicationStatus.SUCCEEDED, "", "");
            System.out.println("exiting");
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }
}
