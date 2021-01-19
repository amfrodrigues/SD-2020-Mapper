import java.io.Serializable;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;

import com.google.common.collect.*;

import javax.annotation.concurrent.Immutable;

public class MapperService extends UnicastRemoteObject implements MapperServiceInterface, Serializable {
    private final String storage_rmi_address = "rmi://localhost:2022/storageservice";


    public MapperService() throws RemoteException {
    }

    //interface MapperServiceInterface functions


    @Override
    public boolean process_data(int len, ArrayList<String> arrayReducer) throws RemoteException {
        boolean status = true;
        StorageServiceInterface storage_rmi = null;
        System.out.println("MAPPER DEBUG: Nª Reducers = "+arrayReducer.size());

        try{
            storage_rmi = (StorageServiceInterface) Naming.lookup(storage_rmi_address);
        }catch(Exception e){e.printStackTrace(); return false;}

        LinkedHashMap<String,ArrayList<ResourceInfo>> timeHarMap = storage_rmi.getTimeHarMap(); // get timeHarMap
        ArrayList<String> resources= new ArrayList<>(timeHarMap.keySet());// serialize timeHarMap keySet
        System.out.println("MAPPER: Número resources " + resources.size());
        Set<Set<String>> combinations = Sets.combinations(ImmutableSet.copyOf(resources), len); // process combinations
        System.out.println("MAPPER: Total Nª Combinations " + combinations.size());

        int n_combinations_por_reducer = (int) Math.ceil(combinations.size()/arrayReducer.size()); // nº combinations por reducer
        System.out.println("MAPPER: Nº sets por reducer " +n_combinations_por_reducer);
        int count = 0;
        ReducerServiceInterface reducer_rmi;

       ArrayList<ArrayList<String>> combinations_serialized =  serialize_sets_to_ArrayList(combinations);
       //debug code erase after test
        if(combinations_serialized instanceof Serializable) {System.out.println("Mapper DEBUG : combinations_serialized is serializable");}
        else {System.out.println("Mapper DEBUG : combinations is not serializable");}
        if(combinations_serialized.get(0) instanceof Serializable) {System.out.println("Mapper DEBUG : combinations_serialized[0] is serializable");}
        else {System.out.println("Mapper DEBUG : combinations is not serializable");}
        int i = 0;
        // partition the Array list in sub ArrayList of n size
        for(List partition : Lists.partition(combinations_serialized,n_combinations_por_reducer)){
            String reducer_rmi_address  = "rmi://localhost:"+arrayReducer.get(i)+"/reducerservice";
            ArrayList<ArrayList<String>> partition_serialized = new ArrayList<>(partition);
            if(partition_serialized instanceof Serializable) {System.out.println("Mapper DEBUG : partition is serializable");}
            else {System.out.println("Mapper DEBUG : partition is not serializable");}
            System.out.println("Mapper DEBUG : Size of partition ["+i+"] is "+partition_serialized.size());
            i++;
            try{
                reducer_rmi = (ReducerServiceInterface) Naming.lookup(reducer_rmi_address);
                System.out.println("Mapper DEBUGGER : " + reducer_rmi_address);
                int filecount = storage_rmi.getFileCount();
                System.out.println("Mapper DEBUGGER : fileCount " + filecount);
                reducer_rmi.process_combinations(partition_serialized,filecount);

            }catch(Exception e){e.printStackTrace(); return false;}
        }
        return true;
    }

    private ArrayList<ArrayList<String>> serialize_sets_to_ArrayList(Set<Set<String>> combinations){
        System.out.println("Mapper : Starting Serializing Sets Combinations ...");
        ArrayList<ArrayList<String>> combinations_serialized = new ArrayList<>();
        Iterator combIterator = combinations.iterator();
        Set auxSet;
        while(combIterator.hasNext()){
            auxSet = (Set) combIterator.next();
            ArrayList<String> auxArrayList = new ArrayList<>(auxSet);
            combinations_serialized.add(auxArrayList);
        }
        return combinations_serialized;
    }

}
