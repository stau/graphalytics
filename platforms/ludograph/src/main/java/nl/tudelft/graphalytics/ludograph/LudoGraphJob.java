package nl.tudelft.graphalytics.ludograph;

import nl.tudelft.graphalytics.PlatformExecutionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.tudelft.ludograph.cluster.message.*;
import org.tudelft.ludograph.comm.Communicator;
import org.tudelft.ludograph.comm.netty.Message;
import org.tudelft.ludograph.comm.netty.MessageProcessor;
import org.tudelft.ludograph.comm.netty.ServerBindException;
import org.tudelft.ludograph.comm.netty.ServerUnreachableException;
import org.tudelft.ludograph.comm.utils.ConnectionInfo;
import org.tudelft.ludograph.conf.LudographConfiguration;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

public class LudoGraphJob {

    private static final Logger LOG = LogManager.getLogger();

    private Configuration jobConf;
    private int numWorkers;
    private int workerMemory;
    private int workerCores;
    private int masterMemory;
    private int masterCores;
    private String platformHost;
    private int platformPort;
    private int clientPort;
    private String jobJarPath;

    protected LudoGraphJob() {
    }

    public void setJobConf(Configuration jobConf) {
        this.jobConf = jobConf;
    }

    public void setNumWorkers(int numWorkers) {
        this.numWorkers = numWorkers;
    }

    public void setWorkerMemory(int workerMemory) {
        this.workerMemory = workerMemory;
    }

    public void setWorkerCores(int workerCores) {
        this.workerCores = workerCores;
    }

    public void setMasterMemory(int masterMemory) {
        this.masterMemory = masterMemory;
    }

    public void setMasterCores(int masterCores) {
        this.masterCores = masterCores;
    }

    public void setPlatformHost(String platformHost) {
        this.platformHost = platformHost;
    }

    public void setPlatformPort(int platformPort) {
        this.platformPort = platformPort;
    }

    public void setClientPort(int clientPort) {
        this.clientPort = clientPort;
    }

    public void setJobJarPath(String jobJarPath) {
        this.jobJarPath = jobJarPath;
    }

    public void sendJobJarAndJobConfToDfsAndNotifyPlatform(Communicator communicator, ConnectionInfo platformInfo, int jobId) throws IOException, ServerUnreachableException {
        // Program jar
        Path srcJarPath = new Path(jobJarPath);
        FileSystem fs = FileSystem.get(new Configuration());
        Path dstJarPath = new Path(fs.getHomeDirectory(), "LudoGraph" + "/" + jobId + "/" + srcJarPath.getName());
        LOG.info("srcJar:" + srcJarPath + " dstJar = " + dstJarPath);
        fs.copyFromLocalFile(false, true, srcJarPath, dstJarPath);
        FileStatus jarHdfsStatus = fs.getFileStatus(dstJarPath);

        String jobConfName = "jobconf.xml";

        // job configuration
        Path dstConfPath = new Path(fs.getHomeDirectory(), "LudoGraph" + "/" + jobId + "/" + jobConfName);
        LOG.info("srcConf:" + srcJarPath + " dstConf = " + dstConfPath);
        FSDataOutputStream outputStream = fs.create(dstConfPath, true);
        jobConf.writeXml(outputStream);
        outputStream.close();

        FileStatus xmlHdfsStatus = fs.getFileStatus(dstConfPath);

        String jobPathString = fs.getHomeDirectory().toString() + "/LudoGraph" + "/" + jobId;
        LOG.info("DFS job path: " + jobPathString);
        fs.close();

        communicator.sendMessageBlocking(
                platformInfo,
                new JobDfsDataDone(jobId, jobPathString, dstJarPath.toUri().toString(), jarHdfsStatus
                        .getLen(), jarHdfsStatus.getModificationTime(), dstConfPath.toUri().toString(),
                        xmlHdfsStatus.getLen(), xmlHdfsStatus.getModificationTime()));
    }

    /**
     * The run method submits a job to the PlatformMaster and subsequently polls to see whether the job has ended or not.
     *
     * @throws UnknownHostException
     * @throws ServerBindException
     * @throws ServerUnreachableException
     * @throws InterruptedException
     * @throws PlatformExecutionException
     */
    public void run() throws UnknownHostException, ServerBindException, ServerUnreachableException, InterruptedException, PlatformExecutionException {
        final String hostName = InetAddress.getLocalHost().getHostName();
        Communicator communicator = new Communicator(new LudographConfiguration(), new ConnectionInfo(-1, hostName, clientPort));
        try {
            communicator.bind();
        } catch (ServerBindException e) {
            LOG.error("Could not bind to hostname:port " + hostName + ":" + clientPort,e);
            throw e;
        }

        ConnectionInfo platformInfo = new ConnectionInfo(-2, platformHost, platformPort);
        try {
            communicator.connect(platformInfo);
        } catch (ServerUnreachableException e) {
            LOG.error("Could not connect to " + platformHost + ":" + platformPort);
            communicator.close();
            throw e;
        }

        // latch for waiting for response
        final Semaphore semaphore = new Semaphore(0);
        final AtomicInteger jobId = new AtomicInteger(-1);
        final JobStatusMessageResponse response = new JobStatusMessageResponse();
        MessageProcessor messageHandler = new MessageProcessor() {

            @Override
            public void processMessage(int i, Message message) {
                if (message instanceof JobStatusMessageResponse) {
                    JobStatusMessageResponse resp = (JobStatusMessageResponse) message;
                    response.setFinished(resp.isFinished());
                    response.setJobId(resp.getJobId());
                    response.setSuccess(resp.isSuccess());
                    semaphore.release();
                } else if (message instanceof JobIdMessage) {
                    JobIdMessage jobIdMessage = (JobIdMessage) message;
                    jobId.set(jobIdMessage.getJobId());
                    semaphore.release();
                } else {
                    LOG.error("Unexpected message: " + message);
                }
            }
        };

        communicator.addMessageProcessor(JobStatusMessageResponse.class, messageHandler);
        communicator.addMessageProcessor(JobIdMessage.class, messageHandler);

        // create job submission
        NewJobMessage newJobMessage = new NewJobMessage(
                masterCores, masterCores,
                masterMemory, masterMemory,
                numWorkers, numWorkers,
                workerCores, workerCores,
                workerMemory, workerMemory,
                "","","",  // legacy variables that are not used.
                hostName, clientPort,
                ""); // extra parameters for schedulers, but not used for this single job
        try {
            communicator.sendMessage(platformInfo, newJobMessage);
            semaphore.acquire();
        } catch (ServerUnreachableException e) {
            LOG.error("Could not send message to " + platformInfo);
            communicator.close();
            throw e;
        } catch (InterruptedException e) {
            LOG.error("Interrupted during countdown wait");
            communicator.close();
            throw e;
        }

        // send job jar and configuration and notify platform
        try {
            sendJobJarAndJobConfToDfsAndNotifyPlatform(communicator, platformInfo, jobId.get());
        } catch (IOException e) {
            LOG.error("IOException whhen sending job program and configuration", e);
            communicator.close();
        } catch (ServerUnreachableException e) {
            LOG.error("Could not send message to " + platformInfo);
            communicator.close();
            throw e;
        }

        JobStatusMessageRequest jobStatusMessageRequest = new JobStatusMessageRequest();
        jobStatusMessageRequest.setHostname(hostName);
        jobStatusMessageRequest.setPort(clientPort);
        jobStatusMessageRequest.setJobId(jobId.get());
        while (true) {

            // poll the platform for completion
            communicator.sendMessageBlocking(platformInfo, jobStatusMessageRequest);

            // wait for response
            semaphore.acquire();
            if (response.isFinished()) {
                break;
            }

            synchronized (this){
                wait(500);
            }
        }

        communicator.close();

        if (!response.isSuccess()) {
            throw new PlatformExecutionException("Job did not succeed, check logs");
        }
    }
}
