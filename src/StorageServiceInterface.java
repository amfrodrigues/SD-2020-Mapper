import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;


public interface StorageServiceInterface extends Remote {
    void LoadFiles(String filename) throws RemoteException;
    boolean sendData(String filename, byte[] data, int len) throws RemoteException;
    LinkedHashMap<String, ArrayList<ResourceInfo>>  getTimeHarMap() throws RemoteException;
    int getFileCount() throws RemoteException;
    void process_reducer_data(ProcessCombinationModel combinationInfo) throws RemoteException;
    int getcombinationsStatisticsize() throws RemoteException;
    void clearCombStatistics() throws RemoteException;
    LinkedList<ProcessCombinationModel> getcombinationsStatistic() throws RemoteException;
}
