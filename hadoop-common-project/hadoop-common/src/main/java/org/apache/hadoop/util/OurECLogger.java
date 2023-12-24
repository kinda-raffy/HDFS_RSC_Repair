package org.apache.hadoop.util;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class OurECLogger {

    private static OurECLogger instance;
    private String logFilePath = "OurECLog-TestCaseName.log";


    // added to avoid all the messages from hadoop internal classes
    // private final boolean enabled = false;
    private String className;
    private FileWriter fileWriter;

    public OurECLogger(String testCaseName) {
        logFilePath = logFilePath.replaceAll("TestCaseName", testCaseName);
        System.out.println("starting OurECLogger, logging at " + logFilePath);

    }


    private String getClassName(){
        return this.className;
    }
    //double check locking implementation (faster with threads...)
    public static synchronized OurECLogger getInstance() {

        if (instance == null) {
            synchronized (OurECLogger.class) {
                if (instance == null)
                    instance = new OurECLogger("default");
            }
        }
        return instance;
    }

    public static synchronized OurECLogger getInstance(String testCaseName) {

        if (instance == null) {
            synchronized (OurECLogger.class) {
                if (instance == null)
                    instance = new OurECLogger(testCaseName);
            }
        }
        return instance;
    }

    public synchronized void write(Object object, String msg) {
        className = object.getClass().getName();
        write(msg);
    }

    public synchronized void write(Object object, String tag, String msg) {
        this.className = object.getClass().getSimpleName() + "-" + tag;
        write(msg);
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
                fileWriter = new FileWriter(logFilePath, true);
            //after open, write the message
            StringBuilder builder = new StringBuilder();
            SimpleDateFormat ft = new SimpleDateFormat("dd:MM:yyyy HH:mm:ss.SSS");
            //add date time info
            //builder.append("[" + format.format(date) + "] (" + System.nanoTime() + ") ");
            builder.append("[").append(ft.format(new Date())).append("] ");
            className = this.getClassName();
            //if the class name exists then add it
            if (className != null && !className.isEmpty())
                builder.append("{").append(className).append("} ");
            //add the message
            builder.append(msg);
            //Console output
            System.out.println(builder.toString());
            //add the newline character
            builder.append("\n");
            //write it in the file
            fileWriter.write(builder.toString());
            fileWriter.flush();
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
        File logFile = new File(logFilePath);
        File logFileBackup = new File(logFilePath + "-" +  currentTime);
        if (!logFile.renameTo(logFileBackup)) {
            System.err.println("Backup aborted as the file could not be renamed.");
        }
    }
}
