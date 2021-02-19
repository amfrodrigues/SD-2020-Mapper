import java.rmi.Remote;
import java.rmi.RemoteException;

import java.util.ArrayList;

public interface ReducerServiceInterface  extends Remote {
    boolean process_combinations(ArrayList<ArrayList<String>> combinations) throws RemoteException;
}
