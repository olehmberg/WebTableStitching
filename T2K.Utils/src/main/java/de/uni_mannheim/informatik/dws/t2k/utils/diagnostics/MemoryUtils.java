package de.uni_mannheim.informatik.dws.t2k.utils.diagnostics;

public class MemoryUtils {

    /**
     * Calls System.gc() several times, until freeMemory() returns stable values
     * @return
     */
    public static long waitGetFreeMemory() {
        // waits for free memory measurement to stabilize
      long init = Runtime.getRuntime().freeMemory(), init2;
      int count = 0;
      do {
          //System.out.println("waiting..." + init);
          System.gc();
          try { Thread.sleep(250); } catch (Exception x) { }
          init2 = init;
          init = Runtime.getRuntime().freeMemory();
          if (init == init2) ++ count; else count = 0;
      } while (count < 5);
      //System.out.println("ok..." + init);
      return init;
    }
    
}
