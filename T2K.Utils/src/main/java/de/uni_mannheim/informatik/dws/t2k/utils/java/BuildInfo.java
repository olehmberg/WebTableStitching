package de.uni_mannheim.informatik.dws.t2k.utils.java;

import java.io.File;
import java.net.JarURLConnection;

import org.apache.commons.lang3.time.DateFormatUtils;

public class BuildInfo {

    public static Long getBuildTime(Class<?> cl) {
        try {
            String rn = cl.getName().replace('.', '/') + ".class";
            JarURLConnection j = (JarURLConnection) ClassLoader.getSystemResource(rn).openConnection();
            return j.getJarFile().getEntry("META-INF/MANIFEST.MF").getTime();
        } catch (Exception e) {
            return 0L;
        }
    }
    
    public static String getBuildTimeString(Class<?> cl) {
        return DateFormatUtils.format(getBuildTime(cl), "yyyy-MM-dd HH:mm:ss");
    }
    
    public static File getJarPath(Class<?> cl) {
    	try {
    		return new File(cl.getProtectionDomain().getCodeSource().getLocation().toURI().getPath());
        } catch (Exception e) {
            return null;
        }
    }
}
