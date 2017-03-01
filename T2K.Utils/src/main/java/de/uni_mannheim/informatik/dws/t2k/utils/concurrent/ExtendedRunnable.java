package de.uni_mannheim.informatik.dws.t2k.utils.concurrent;

public abstract class ExtendedRunnable implements Runnable {

    private Exception exception;
    public Exception getException() {
        return exception;
    }
    public void setException(Exception exception) {
        this.exception = exception;
    }    
//    public boolean running =false;
//    public long start;
}
