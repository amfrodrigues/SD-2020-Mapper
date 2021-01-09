import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Set;

public interface ReducerServiceInterface  extends Remote {
    boolean process_combinations(List<Set<String>> combinations, int fileCount) throws RemoteException;


}
