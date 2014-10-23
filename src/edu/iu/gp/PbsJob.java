
package edu.iu.gp;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
    private String executableFile = "N/A";
    private String epilogueFile = "N/A";
    private String wallTime = "1:00:00";
    private String ctime = "N/A";
    private String queue = "N/A";
    private String vmem = "4gb";
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
        // initialize the commandLine as a string.
        String pbsCommand = wrapCommandLineArgsInSingleQuotes(drmJobSubmission.getCommandLine());

        // if there is a logFile, stream stdout from the module to the stdoutFile
        final String jobReportFilename;
        if (drmJobSubmission.getLogFile() != null) {
            pbsCommand += " >> " + wrapInSingleQuotes(drmJobSubmission.getStdoutFile().getAbsolutePath());
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

    }

    public void setPbsScript(String st) {
        this.pbsScript = st;
    }

    public String getPbsScript() {
        return pbsScript;
    }

    
     /**
     * Construct a command line string from the list of args.
     * Wrap each arg in single quote characters, make sure to escape any single quote characters in the args.
     * 
     * @param commandLine
     * @return
     */
    private String wrapCommandLineArgsInSingleQuotes(List<String> commandLine) {
        String rval = "";
        boolean first = true;
        for(String arg : commandLine) {
            arg = wrapInSingleQuotes(arg);
            if (first) {
                first = false;
            }
            else {
                rval += " ";
            }
            rval += arg;
        }
        return rval;
    }

    private String wrapInSingleQuotes(String arg) {
        if (arg.contains("'")) {
            // replace each ' with '\''
            arg = arg.replace("'", "'\\''");
        }
        arg = "'"+arg+"'";
        return arg;
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
    sb.append("Job Queue: " + this.queue + "\n");
    sb.append("Wall Time:" + this.ctime + "\n");
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
     * @param ppn the processors per node to set
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
     * @return the cpu Time
     */
    public String getcTime() {
        return ctime;
    }

    /**
     * @param ctime the cpu Time to set
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
