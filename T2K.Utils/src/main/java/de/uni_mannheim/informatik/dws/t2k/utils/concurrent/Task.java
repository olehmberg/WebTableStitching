package de.uni_mannheim.informatik.dws.t2k.utils.concurrent;

import de.uni_mannheim.informatik.dws.t2k.utils.concurrent.Parallel.ITask;

public abstract class Task implements ITask {
	private Object userData;
	public Object getUserData() {
		return userData;
	}
	public void setUserData(Object userData) {
		this.userData = userData;
	}
	public abstract void execute();
}
