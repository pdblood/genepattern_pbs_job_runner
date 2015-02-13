/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.iu.gp;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.genepattern.drm.DrmJobRecord;

/**
 *
 * @author gpserver
 */
public class PBStest {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception{
        // TODO code application logic here

        String date = "Tue Dec 16 15:33:42 2014";
        SimpleDateFormat dt = new SimpleDateFormat("EEE MMM d HH:mm:ss yyyy");
        Date sdate = dt.parse(date);
        System.out.println(sdate.toString());




        //String pbsid = "265066.m1.mason";
        //String out = PBS.showstart(pbsid);
        //System.out.println(out);

        
        
        //System.out.println( PBStest.PbsResUsage("1002__1002__265066.m1.mason__m1.mason.indiana.edu","cput"));
       //System.out.println( PBStest.PbsResUsage("1002__1002__265066.m1.mason__m1.mason.indiana.edu","vmem"));
        //System.out.println( PBStest.PbsResUsage("1002__1002__265066.m1.mason__m1.mason.indiana.edu","start_time"));
      //System.out.println( PBStest.PbsResUsage("1002__1002__265066.m1.mason__m1.mason.indiana.edu","qtime"));
        
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
 
            if (line.length >1){            
            header = line[0].trim();
            //System.out.println("Header = " + header);
            value = line[1].trim();
            // System.out.println("value = " + value);

            if (header.contains(key)) {
                return value;
            }
            }
  
        }

        return null;

    }
    
    public static String PbsResUsage(String drmJobRecord, String resKey) throws IOException, InterruptedException, PbsException {

        final String drmJobId = drmJobRecord;

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

            return null;
            // If we got errors, means we can get the estimated starting time.
            // we will try next time but log the info and return the message 
            // as "can not get the starting time"
        } else {
            BufferedInputStream ef = new BufferedInputStream(p.getInputStream());
            byte[] data = new byte[ef.available()];
            ef.read(data, 0, ef.available());
            ef.close();
            p.getOutputStream().close();
            p.getErrorStream().close();
            String Result = new String(data);
            String value = PBStest.getKeyValue(resKey, Result.split("\n"));

            if (resKey.equals("cput") || resKey.equals("walltime")) {
                String[] info = value.split(":", 3);
                int tSec = Integer.parseInt(info[2]);
                int tMin = Integer.parseInt(info[1]);
                int tHur = Integer.parseInt(info[0]);
                Long rt = tHur * 60 * 60 + tMin * 60 + tSec * 1L;
                return rt.toString();
            } else if (resKey.equals("mem") || resKey.equals("vmem")) {
                String mem = value.substring(0, value.length() - 2);
                Long memValue = Long.parseLong(mem) * 1024L;
                return memValue.toString();

            } else if (resKey.equals("start_time") || resKey.equals("qtime")){
                String[] info = value.split(" ", 5);
                String year = info[4];    
                String month = info[1];
                String date = info[2];
                String hrs = info[3].split(":",3)[0];
                String min = info[3].split(":",3)[1];
                String sec = info[3].split(":",3)[2];
                return value;
            }
            
             else {
                return value;
            }

        }

    }
}
