import java.io.Serializable;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;

import com.google.common.collect.*;


public class MapperService extends UnicastRemoteObject implements MapperServiceInterface, Serializable {
    private final String mapperID ;
    private final String storage_rmi_address = "rmi://localhost:2022/storageservice";

    private Map<String,ArrayList<ArrayList<String>>> hashPartitionPerReducer = Collections.synchronizedMap(new HashMap<>());

    private List<String> usedReducer = Collections.synchronizedList(new ArrayList<>());


    public MapperService(String mapperID) throws RemoteException {

        this.mapperID = mapperID;
    }

    //interface MapperServiceInterface functions


    /*
        Method that processed the combination of  n elements with Sets.combinations()
        and then partition that n elements in m partitions where m is the number os reducers assigned to this mapper
     */
    @Override
    public boolean process_data(int len, ArrayList<String> arrayReducer) throws RemoteException {
        HashMap<String,Thread> threadList = new HashMap<>();
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


       ArrayList<ArrayList<String>> combinations_serialized =  serialize_sets_to_ArrayList(combinations);


        int i = 0;
        // partition the Array list in sub ArrayList of n size
        for(List partition : Lists.partition(combinations_serialized,n_combinations_por_reducer)){
            String reducer_rmi_address  = arrayReducer.get(i);
            ArrayList<ArrayList<String>> partition_serialized = new ArrayList<>(partition);
          //  if(partition_serialized instanceof Serializable) {System.out.println("Mapper DEBUG : partition is serializable");}
          //  else {System.out.println("Mapper DEBUG : partition is not serializable");}
            System.out.println("Mapper DEBUG : Size of partition ["+i+"] is "+partition_serialized.size());
            ReducerServiceInterface reducer_rmi;
            try{
                reducer_rmi = (ReducerServiceInterface) Naming.lookup(reducer_rmi_address);
                System.out.println("Mapper DEBUGGER : " + reducer_rmi_address);

                String thread_name = "thread"+i;
                Thread t  = new Thread(()->{
                    try{
                       hashPartitionPerReducer.put(reducer_rmi_address,partition_serialized); // saves the id of the reducer and partition used
                       usedReducer.add(reducer_rmi_address);
                      if(reducer_rmi.process_combinations(partition_serialized)){

                          usedReducer.remove(reducer_rmi_address);
                          System.out.println("Mapper "+reducer_rmi_address +"finish still waiting for "+usedReducer.size());
                      }
                    }catch(Exception e){
                        e.printStackTrace();
                    }
                });
               // threadList.put(thread_name,t);
                t.start();

            }catch(Exception e){e.printStackTrace(); return false;}
            i++;
        }

        /*
            Blocks this thread until all others launched threads finishes
 */
        for(Thread values : threadList.values()){
            try{
                values.join();
            }catch(Exception e){e.printStackTrace();}
        }
/*
            failsafe thread for unfinished task requests
         */

            while(usedReducer.size() > 0) {}

        System.out.println("Mapper Finished Task");
        return true;
    }



    /*
        Method used to redo a issued task to a reducer that failed to be completed
     */
    @Override
    public void redo_taskReducer(String reducerAddress) throws RemoteException {
        if(!usedReducer.isEmpty()){
            ReducerServiceInterface reducer_rmi = null;
            try{
                reducer_rmi = (ReducerServiceInterface) Naming.lookup(reducerAddress);

            }catch(Exception e){e.printStackTrace();}
            ReducerServiceInterface finalReducer_rmi = reducer_rmi;
            Thread threadReducer = new Thread (()->{
                try {
                    if(finalReducer_rmi.process_combinations(hashPartitionPerReducer.get(reducerAddress))) usedReducer.remove(reducerAddress);
                } catch (RemoteException e) {
                    e.printStackTrace();
                }
            });
            threadReducer.start();
        }
        else{System.out.println("MAPPER["+mapperID+"] -> there is no task to revive from "+reducerAddress);}
    }

    /*
        Method that returns a serialized <Set<Set<String>> as ArrayList<ArrayList<String>>
    */
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
