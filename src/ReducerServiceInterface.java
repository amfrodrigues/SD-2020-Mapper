import java.rmi.Remote;
import java.rmi.RemoteException;

import java.util.ArrayList;

public interface ReducerServiceInterface  extends Remote {
    void process_combinations(ArrayList<ArrayList<String>> combinations, int fileCount) throws RemoteException;
}