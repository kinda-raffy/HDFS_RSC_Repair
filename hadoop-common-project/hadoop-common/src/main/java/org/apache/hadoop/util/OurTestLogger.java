package org.apache.hadoop.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class OurTestLogger {

    private static OurTestLogger instance;
    private String TestLogFilePath = "TestLog/OurTestLog-TestCaseName.log";
    private FileWriter fileWriter;

    private OurTestLogger(String testCaseName) {
        TestLogFilePath = this.TestLogFilePath.replaceAll("TestCaseName", testCaseName + "");
        System.out.println("starting OurECLogger, logging at " + TestLogFilePath);
    }

    public static synchronized OurTestLogger getInstance(String testCaseName) {

        if (instance == null) {
            synchronized (OurTestLogger.class) {
                if (instance == null)
                    instance = new OurTestLogger(testCaseName);
            }
        }
        return instance;
    }

    public static synchronized OurTestLogger getInstance() {

        if (instance == null) {
            synchronized (OurTestLogger.class) {
                if (instance == null)
                    instance = new OurTestLogger("default");
            }
        }
        return instance;
    }

    /**
     * Write any message into a file log declared as source
     * @param msg - input string with the message to be logged
     */
    public synchronized void write(String msg) {

        //if (!enabled)
        //return;
        //check if the file writer is initialized
        try {
            if (fileWriter == null)
                //the true in the end declared the file in appending mode
                fileWriter = new FileWriter(TestLogFilePath, true);
            //after open, write the message
            StringBuilder builder = new StringBuilder();
            SimpleDateFormat ft = new SimpleDateFormat("dd:MM:yyyy HH:mm:ss.SSS");
            //add date time info
            //add the message
            builder.append(msg);
            //Console output
            System.out.println(builder.toString());
            //add the newline character
            builder.append("\n");
            //write it in the file
            fileWriter.write(builder.toString());
        } catch (IOException ioe) {
            System.err.println("Aborting writing... file couldn't be opened!!");

        } finally {
            close();
        }
    }

    /**
     * Method executed at the end of the process tested
     */
    private void close() {
        try {
            if (fileWriter != null)
                fileWriter.close();
            //forcing garbage collection for sanity
            fileWriter = null;
        } catch (IOException ioe) {
            System.err.println("Aborting writing... file couldn't be closed!!");
            //writeFile("E");
        }
    }
    public void saveCurrentFileAsBackup(long currentTime) {
        File logFile = new File(TestLogFilePath);
        File logFileBackup = new File(TestLogFilePath + "-" +  currentTime);
        if (!logFile.renameTo(logFileBackup)) {
            System.err.println("Aborting writing... file couldn't be renamed!!");
        }
    }
}
