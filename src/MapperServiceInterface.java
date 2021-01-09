import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.LinkedHashMap;

public interface MapperServiceInterface extends Remote {
    boolean process_data(int len,ArrayList<String> arrayReducer) throws RemoteException;
}
