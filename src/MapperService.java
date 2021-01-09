import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

public class MapperService extends UnicastRemoteObject implements MapperServiceInterface {
    private final String storage_rmi_address = "rmi://localhost:2022/storageservice";


    public MapperService() throws RemoteException {
    }

    //interface MapperServiceInterface functions


    @Override
    public boolean process_data(int len, ArrayList<String> arrayReducer) throws RemoteException {
        boolean status = true;
        StorageServiceInterface storage_rmi = null;
        try{
            storage_rmi = (StorageServiceInterface) Naming.lookup(storage_rmi_address);
        }catch(Exception e){e.printStackTrace(); return false;}
        Set<Set<String>> combinations = define_combinations(len,storage_rmi.getTimeHarMap());
        int i = 0;
        for( List<Set<String>> partition : Iterables.partition(combinations,combinations.size()/arrayReducer.size())){
                String reducer_rmi_address  = "rmi://localhost:"+arrayReducer.get(i)+"/reducerservice";
                ReducerServiceInterface reducer_rmi = null;
                try{
                    reducer_rmi = (ReducerServiceInterface) Naming.lookup(reducer_rmi_address);
                }catch(Exception e){e.printStackTrace(); return false;}
                status = reducer_rmi.process_combinations( partition,storage_rmi.getFileCount());
                i++;
        }
        return status;
    }



    //private functions
    private Set<Set<String>>  define_combinations(int len,LinkedHashMap<String, ArrayList<ResourceInfo>> timeHarMap) throws RemoteException {
        ArrayList<String> resources= new ArrayList<>(timeHarMap.keySet());
        System.out.println("Número resources " + resources.size());
        return Sets.combinations(ImmutableSet.copyOf(resources), len);
    }
}
