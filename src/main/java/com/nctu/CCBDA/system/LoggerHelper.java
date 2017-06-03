package com.nctu.CCBDA.system;

import java.text.SimpleDateFormat;
import java.util.Date;

public class LoggerHelper {
    private static SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
    public static void infoLog(String message) {
        System.err.println(dateFormat.format(new Date()) + " [INFO ] " + message);
    }
    public static void errLog(String message) {
        System.err.println(dateFormat.format(new Date()) + " [ERROR] " + message);
    }
    public static void debugLog(String message) {
        System.err.println(dateFormat.format(new Date()) + " [DEBUG] " + message);
    }
}