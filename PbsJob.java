/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.iu.gp;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.log4j.Logger;
import org.genepattern.drm.DrmJobSubmission;

/**
 *
 * @author lewu@iu.edu
 */

public class PbsJob {

    private static final Logger log = Logger.getLogger(PbsJob.class);
    
    private static void writeToFile(final String message, final File toFile) {
        toFile.getParentFile().mkdirs();
        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new FileWriter(toFile));
            writer.write(message);
        } catch (IOException e) {
            log.error("Error writing file=" + toFile, e);
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private String id;
    private String Name = "N/A";
    private String nodes = "1";
    private String ppn = "4";
    private ArrayList<String> afterany = new ArrayList<String>();
    private ArrayList<String> afterOK = new ArrayList<String>();
    private HashMap<String, String> variables = new HashMap<String, String>();
    private String SubmitArgs = "N/A";
    private String ctime = "N/A";
    private String qtime = "N/A";
    private String mtime = "N/A";
    private String stime = "N/A";
    private String comp_time = "N/A";
    private String owner = "N/A";
    private String executableFile = "N/A";
    private String epilogueFile = "N/A";
    private String wallTime = "1:00:00";
    private String queue = "N/A";
    private String vmem = "64gb";
    private String status = "N/A";
    private String executeNode = "N/A";
    private String ellapsedTime = "N/A";
    private String usedMem = "N/A";
    private String usedcput = "N/A";
    private String errrorPath = "N/A";
    private String outputPath = "N/A";
    private String VariablesList = "N/A";
    private String outputDir = "N/A";
    private String pbsScript = "";
    private String hostName = "N/A";
    private String mem = "N/A";

    /**
     *
     * 
     */
    public PbsJob(DrmJobSubmission drmJobSubmission) throws IOException {

        // set the PBS job name
        this.Name = "gp-job-" + drmJobSubmission.getGpJobNo();

        // Get job running directory
        String workDir = drmJobSubmission.getWorkingDir().getAbsolutePath();
        String stdOut = drmJobSubmission.getStdoutFile().getAbsolutePath();
        String stdErr = drmJobSubmission.getStderrFile().getAbsolutePath();

        // Set job running directory
        this.outputPath = stdOut;
        this.errrorPath = stdErr;
        this.outputDir = workDir;

        // Iterate the commandLine array to build the real command string
        // for further job submission
        final String[] commandLine = drmJobSubmission.getCommandLine().toArray(new String[0]);
        String pbsCommand = "";
        for (String command : commandLine) {
            pbsCommand += command + " ";
        }

        // We will create a PBS script file and store it in the working
        // diretoy for further job submission
        CommandTemplate ct = new CommandTemplate(workDir, pbsCommand);
        ct.createExecutableFile();
        ct.createEpilogueFile();

        this.executableFile = ct.getPbsFileName();
        this.epilogueFile = ct.getEpiFileName();

        // Set some PBS parameters, we can also change some default settings
        // if we have such needs
        // get The default PBS parameters
        if (drmJobSubmission.getWalltime() != null) {
            this.wallTime = drmJobSubmission.getWalltime().toString();
        }

        if (drmJobSubmission.getQueue() != null) {
            this.queue = drmJobSubmission.getQueue().toString();
        }

        if (drmJobSubmission.getCpuCount() != null) {
            this.ppn = drmJobSubmission.getCpuCount().toString();
        }

        if (drmJobSubmission.getExtraArgs() != null && drmJobSubmission.getExtraArgs().size() > 0) {
            String extraArgs = drmJobSubmission.getExtraArgs().toString();
            //we will need to do some extra setting , if we have extra arguments
        }

        // We can then overwrite the default PBS parameters before building the script
        if (drmJobSubmission.getProperty("pbs.host") != null) {
            this.hostName = drmJobSubmission.getProperty("pbs.host");
        }

        if (drmJobSubmission.getProperty("pbs.mem") != null) {
            this.mem = drmJobSubmission.getProperty("pbs.mem").toString();
        }

        if (drmJobSubmission.getProperty("pbs.ppn") != null) {
            this.ppn = drmJobSubmission.getProperty("pbs.ppn").toString();
        }

        if (drmJobSubmission.getProperty("pbs.cput") != null) {
            this.ctime = drmJobSubmission.getProperty("pbs.cput").toString();
        }

        if (drmJobSubmission.getProperty("pbs.vmem") != null) {
            this.vmem = drmJobSubmission.getProperty("pbs.vmem").toString();
        }
        
    }

    public void buildSubmissionScript() {

        StringBuilder excuter = new StringBuilder("qsub -N " + getName() + " ");

        if (!"N/A".equals(getQueue()) && "N/A".equals(getHostName())) {
            excuter.append(" -q ").append(getQueue());
        } else if ("N/A".equals(getQueue()) && !"N/A".equals(getHostName())) {
            excuter.append(" -q @").append(getHostName());
        } else if (!"N/A".equals(getQueue()) && !"N/A".equals(getHostName())) {
            excuter.append(" -q ").append(getQueue()).append("@").append(getHostName());
        }

        // We need to put a epilogue script to print out the exit code and other
        // information
        excuter.append(" -l " + "epilogue=").append(getEpilogueFile());

        if (!"N/A".equals(getNodes()) && !"N/A".equals(getPpn())) {
            excuter.append(" -l nodes=").append(getNodes()).append(":ppn=").append(getPpn()).append(" ");
        } else if (getPpn() == null && getNodes() != null) {
            excuter.append(" -l nodes=").append(getNodes());
        }

        // if there are some job dependencies, we need to put them into the qsub command
        //<editor-fold desc="AfterOK">
        if (afterOK.size() > 0) {
            StringBuilder strOk = new StringBuilder(" -W depend=afterok");
            for (int i = 0; i < getAfterOK().size(); i++) {
                strOk.append(":").append(getAfterOK().get(i));
            }
            excuter.append(strOk);
        }

        // if there are some job dependencies, we need to put them into the qsub command
        if (afterany.size() > 0) {
            StringBuilder strAny = new StringBuilder(" -W depend=afterany");
            for (int i = 0; i < getAfterany().size(); i++) {
                strAny.append(":").append(getAfterany().get(i));
            }
            excuter.append(strAny);
        }

        if (!"N/A".equals(getVmem())) {
            excuter.append(" -l vmem=").append(getVmem());
        }

        if (!"N/A".equals(getMem())) {
            excuter.append(" -l mem=").append(getMem());
        }
                
        if (!"N/A".equals(getWallTime())) {
            excuter.append(" -l walltime=" + getWallTime());
        }

        // we join the standard error and output together
        // we remove join standard error and output 05/23/2014
        if (!"N/A".equals(getOutputPath())) {
            excuter.append(" -o " + getOutputPath());
        }

        if (!"N/A".equals(getErrrorPath())) {
            excuter.append(" -e " + getErrrorPath());
        }

        if (!"N/A".equals(getOutputDir())) {
            excuter.append(" -d " + getOutputDir());
        }

        excuter.append(" " + getExecutableFile());
        String st = excuter.toString();

        setPbsScript(st);

        //******TODO: Remove this in case of Release; ******/
        BufferedWriter out = null;
        try {
            FileWriter fstream = new FileWriter("/N/dc2/scratch/gpserver/pbscommand.txt", true); //true tells to append data.
            out = new BufferedWriter(fstream);
            out.write(st + "\n");
            out.close();

        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
        }
        //**************************************************/
    }

    public void setPbsScript(String st) {
        this.pbsScript = st;
    }

    public String getPbsScript() {
        return pbsScript;
    }



    /**
     *
     * @return String Representation of a Job
     */
    @Override
public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append("Job ID: " + this.id + "\n");
    sb.append("Job Name: " + this.Name + "\n");
    sb.append("Job Owner: " + this.owner + "\n");
    sb.append("Job Status: " + this.status + "\n");
    sb.append("Job Queue: " + this.queue + "\n");
    sb.append("\n");
    sb.append("Resources\n");
    sb.append("CPU Time: " + this.usedcput + "\n");
    sb.append("Mem Used: " + this.usedMem + "\n");
    sb.append("Used WallTime: " + this.ellapsedTime + "\n");
    sb.append("execute Node : " + this.executeNode + "\n");
    sb.append("\nTimes:\n");
    sb.append("ctime: " + this.ctime + "\n");
    sb.append("qtime:" + qtime + "\n");
    sb.append("mtime: " + mtime + "\n");
    sb.append("\n");
    sb.append("Files\n");
    sb.append("Output File: " + this.outputPath + "\n");
    sb.append("Error File: " + this.errrorPath + "\n");
    return sb.toString();
}

    /**
     * @return the nodes
     */
    public String getNodes() {
        return nodes;
    }

    /**
     * @param nodes the nodes to set
     */
        public void setNodes(String nodes) {
            this.nodes = nodes;
        }

    /**
     * @return the ppn
     */
    public String getPpn() {
        return ppn;
    }

    /**
     * @param ppn the ppn to set
     */
    public void setPpn(String ppn) {
        this.ppn = ppn;
    }

    /**
     * @return the ppn
     */
    public String getMem() {
        return mem;
    }

    /**
     * @param ppn the ppn to set
     */
    public void setMem(String mem) {
        this.mem = mem;
    }

    /**
     * @return the ppn
     */
    public String getHostName() {
        return hostName;
    }

    /**
     * @param ppn the ppn to set
     */
    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    /**
     * @return the executableFile
     */
    public String getExecutableFile() {
        return executableFile;
    }

    /**
     * @param executableFile the executableFile to set
     */
    public void setExecutableFile(String executableFile) {
        this.executableFile = executableFile;
    }

    public String getEpilogueFile() {
        return epilogueFile;
    }

    public void setEpilogueFile(String epilogueFile) {
        this.epilogueFile = epilogueFile;
    }

    /**
     * @return the wallTime
     */
    public String getWallTime() {
        return wallTime;
    }

    /**
     * @param wallTime the wallTime to set
     */
    public void setWallTime(String wallTime) {
        this.wallTime = wallTime;
    }

    /**
     * @return the wallTime
     */
    public String getcTime() {
        return ctime;
    }

    /**
     * @param wallTime the wallTime to set
     */
    public void setCTime(String ctime) {
        this.ctime = ctime;
    }

    /**
     * @return the vmem
     */
    public String getVmem() {
        return vmem;
    }

    /**
     * @param vmem the vmem to set
     */
    public void setVmem(String vmem) {
        this.vmem = vmem;
    }

    /**
     * @return the queue
     */
    public String getQueue() {
        return queue;
    }

    /**
     * @param queue the queue to set
     */
    public void setQueue(String queue) {
        this.queue = queue;
    }

    /**
     * @return the executeNode
     */
    public String getExecuteNode() {
        return executeNode;
    }

    /**
     * @param executeNode the executeNode to set
     */
    public void setExecuteNode(String executeNode) {
        this.executeNode = executeNode;
    }

    /**
     * @return the errrorPath
     */
    public String getErrrorPath() {
        return errrorPath;
    }

    /**
     * @param errrorPath the errrorPath to set
     */
    public void setErrrorPath(String errrorPath) {
        this.errrorPath = errrorPath;
    }

    /**
     * @return the outputPath
     */
    public String getOutputPath() {
        return outputPath;
    }

    /**
     * @param outputPath the outputPath to set
     */
    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
    }

    /**
     * @param outputDir the outputPath to set
     */
    public String getOutputDir() {
        return outputDir;
    }

    /**
     * @param outputDir the outputDir to set
     */
    public void setOutputDir(String outputDir) {
        this.outputDir = outputDir;
    }

    /**
     * @return the afterany
     */
    public ArrayList<String> getAfterany() {
        return afterany;
    }

    /**
     * @param afterany the afterany to set
     */
    public void setAfterany(ArrayList<String> afterany) {
        this.setAfterany(afterany);
    }

    /**
     * @return the afterOK
     */
    public ArrayList<String> getAfterOK() {
        return afterOK;
    }

    /**
     * @param afterOK the afterOK to set
     */
    public void setAfterOK(ArrayList<String> afterok) {
        this.afterOK = afterok;
    }

    /**
     * @return the id
     */
    public String getId() {
        return id;
    }

    /**
     * @param id the id to set
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * @return the Name
     */
    public String getName() {
        return Name;
    }

    /**
     * @param Name the Name to set
     */
    public void setName(String Name) {
        this.Name = Name;
    }

 

}
