import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.LinkedList;


public interface MasterServiceInterface extends Remote {
    LinkedList<CombinationProcessingData> task_combinations(int len) throws RemoteException;
    void heartbeat_check(String id,String type) throws RemoteException;
    ArrayList<String> heartbeat_revive(String type) throws RemoteException;
    void reducerTaskRevive(String reducerAddress) throws RemoteException;
}
