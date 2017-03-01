package de.uni_mannheim.informatik.dws.t2k.utils.cli;

import java.util.List;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import de.uni_mannheim.informatik.dws.t2k.utils.StringUtils;
import de.uni_mannheim.informatik.dws.t2k.utils.java.BuildInfo;

public class Executable {

    @Parameter
    protected List<String> params;
    
    public List<String> getParams() {
		return params;
	}
    
    protected boolean parseCommandLine(Class<?> cls, String... args) {
        try {
        	System.out.println(BuildInfo.getJarPath(getClass()));
        	System.out.println(String.format("%s version %s", cls.getName(), BuildInfo.getBuildTimeString(cls)));
        	
            @SuppressWarnings("unused")
			JCommander cmd = new JCommander(this, args);
            
            return true;
        } catch(Exception e) {
            //e.printStackTrace();
        	System.out.println(e.getMessage());
        	usage(args);
            return false;
        }
    }
    
    protected void usage(String... args) {
    	System.out.println(StringUtils.join(args, " "));
        JCommander cmd = new JCommander(this);
        cmd.usage();
    }
	
}
