/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.iu.gp;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Scanner;
import org.genepattern.drm.DrmJobRecord;

/**
 *
 * @author lewu@iu.edu
 */
public class PBS {



    public static String qsub(String input) throws IOException, InterruptedException, PbsException {

        Process p = Runtime.getRuntime().exec(input);

        p.waitFor();

        BufferedInputStream ef = new BufferedInputStream(p.getErrorStream());

        if (ef.available() > 0) {
            byte[] errdata = new byte[ef.available()];
            ef.read(errdata, 0, ef.available());
            String st = new String(errdata);
            ef.close();
            p.getOutputStream().close();
            p.getInputStream().close();
            throw new PbsException(st);
        }

        //return new String(data);
        BufferedInputStream b = new BufferedInputStream(p.getInputStream());
        byte[] data = new byte[b.available()];
        b.read(data);
        p.getOutputStream().close();
        p.getErrorStream().close();

        b.close();
        return new String(data).replaceAll("\n", "");
    }

    public static String qstat(DrmJobRecord drmJobRecord) throws IOException, InterruptedException, PbsException {

        final String drmJobId=drmJobRecord.getExtJobId();
        
        String workDirPath = drmJobId.split("__")[0];
        String gpId = drmJobId.split("__")[1];
        String pbsId = drmJobId.split("__")[2];
        String clusterName = drmJobId.split("__")[3];
        String pbsJobId = pbsId + "@" + clusterName;

        //String pbsJobId = jobID + "@m1.mason.indiana.edu";
        Process p = Runtime.getRuntime().exec("qstat -f " + pbsJobId);
        p.waitFor();

        BufferedInputStream errStream = new BufferedInputStream(p.getErrorStream());

        if (errStream.available() > 0) {
            byte[] errdata = new byte[errStream.available()];
            errStream.read(errdata, 0, errStream.available());
            p.getOutputStream().close();
            p.getErrorStream().close();
            errStream.close();

            // If we got errors, such as "Unknown Job Id Error 206815.m1", while checking the job status,
            // it is possible that this job has been finished long time ago and 
            // its record has been removed from the pbs history.
            // We will need to check whether the stderr, stdout, and epilogue.pbs have been write to
            // job working directory.
            File stderr = drmJobRecord.getStderrFile();
            File epilogueOut = new File(workDirPath, "epilogue.pbs");

            if (epilogueOut.exists()) {

                String epiData = (new Scanner(epilogueOut)).useDelimiter("\\z").next();
                String exitCode = getKeyValue("Job_Exit_Code", epiData.split("\n"));

                // job finished successfully, we got exit_code = 0
                if (exitCode.compareToIgnoreCase("0") == 0) {
                    
                    // PBS script finshed successfully, but we need to do one more check on
                    // stderr file to see whether there are some error messages shown.
                    // We use some pre-defined key words for checking the erros.
                    //
                    // If we have the stderr file
                    if (stderr.exists()) {
                        if (hasErrorsInStdout(stderr)) {
                            return "F";
                        } else {
                            return "C";
                        }
                    }
                    // stderr does not exist, it means everything looking good. 
                    else{
                        return "C";
                    }
                } // we got non-zero job exit code or we got null 
                else {
                    return "F";
                }
            }

            // There are some problems for getting this job's status
            // and we can not find stdout and epiloigue files
            throw new PbsException((new String(errdata)));
        }

        BufferedInputStream ef = new BufferedInputStream(p.getInputStream());
        byte[] data = new byte[ef.available()];
        ef.read(data, 0, ef.available());
        ef.close();
        p.getOutputStream().close();
        p.getErrorStream().close();
        String Result = new String(data);
        return getKeyValue("job_state", Result.split("\n"));

    }

    public static String getKeyValue(String key, String[] info) {

        String[] line;
        String header = "";
        String value = "";

        for (int i = 0; i < info.length; i++) {
            if (info[i].contains("=")) {
                line = info[i].split("=", 2);
            } else {
                line = info[i].split(":", 2);
            }
            header = line[0].trim();
            //System.out.println("Header = " + header);
            value = line[1].trim();
            // System.out.println("value = " + value);

            if (key.equals(header)) {
                return value;
            }
        }

        return null;

    }

    public static boolean qdel(String JobID) throws IOException, InterruptedException, PbsException {

        Process p = Runtime.getRuntime().exec("qdel " + JobID);
        p.waitFor();

        BufferedInputStream errStream = new BufferedInputStream(p.getErrorStream());

        // if we receive some error message while deleting the job, we
        // will need to report the message back to the job runner
        if (errStream.available() > 0) {
            byte[] errdata = new byte[errStream.available()];
            errStream.read(errdata, 0, errStream.available());
            p.getOutputStream().close();
            p.getErrorStream().close();
            errStream.close();
            throw new PbsException((new String(errdata)));
        }

        return true;
    }

    public static String qstat2(String pbsJobId) throws IOException, InterruptedException, PbsException {

        // we need to parse the gp id to dirid and pbs id
        //String[] Info = getJobInfo(pbsId + "@" + clusterName);
        String[] Info = getJobInfo2(pbsJobId);

        String header = "";
        String value = "";
        String[] line;

        for (int i = 0; i < Info.length; i++) {
            if (Info[i].contains("=")) {
                line = Info[i].split("=", 2);
            } else {
                line = Info[i].split(":", 2);
            }
            header = line[0].trim();
            //System.out.println("Header = " + header);
            value = line[1].trim();
            // System.out.println("value = " + value);

            if ("job_state".equals(header)) {
                return value;
            }
        }

        return null;
    }

    private static String[] getJobInfo2(String pbsJobId) throws IOException, InterruptedException, PbsException {

        Process p = Runtime.getRuntime().exec("qstat -f " + pbsJobId);
        p.waitFor();

        BufferedInputStream errStream = new BufferedInputStream(p.getErrorStream());

        if (errStream.available() > 0) {
            byte[] errdata = new byte[errStream.available()];
            errStream.read(errdata, 0, errStream.available());
            p.getOutputStream().close();
            p.getErrorStream().close();
            errStream.close();
            throw new PbsException((new String(errdata)));
        }

        BufferedInputStream ef = new BufferedInputStream(p.getInputStream());
        byte[] data = new byte[ef.available()];
        ef.read(data, 0, ef.available());
        ef.close();
        p.getOutputStream().close();
        p.getErrorStream().close();
        String Result = new String(data);
        return Result.split("\n");
    }

    public static boolean hasErrorsInStdout(File stderr) throws IOException {

        String[] errorKeyWords = {"error", "errors", "abort", "aborted", "core dump", "exception"};

        Scanner scanner = new Scanner(stderr);

        //now read the stdout file line by line...
        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();

            for (String errorKeyWord : errorKeyWords) {

                // If we find an error keyword, we will return true
                if (org.apache.commons.lang.StringUtils.containsIgnoreCase(line, errorKeyWord)) {
                    return true;
                }
            }
        }
        return false;
    }

}
