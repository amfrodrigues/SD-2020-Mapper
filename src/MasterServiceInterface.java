import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;


public interface MasterServiceInterface extends Remote {
    boolean task_combinations(int len) throws RemoteException;
    void heartbeat_check(String id,String type) throws RemoteException;
    ArrayList<String> heartbeat_revive(String type) throws RemoteException;
    void reducerTaskRevive(String reducerAddress) throws RemoteException;
}
