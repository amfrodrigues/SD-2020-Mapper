import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;

public interface MapperServiceInterface extends Remote {
    boolean process_data(int len,ArrayList<String> arrayReducer) throws RemoteException;
    void redo_taskReducer(String reducerAddress) throws RemoteException;
}
