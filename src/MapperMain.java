import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class MapperMain {
    public static void main(String[] args){
        Registry r = null;
        MapperServiceInterface mapperService;
        Integer port = Integer.parseInt(args[0]);
        try{
            r = LocateRegistry.createRegistry(port);
        }catch(RemoteException a){
            a.printStackTrace();
        }

        try{
            mapperService = new MapperService();
            r.rebind("mapperservice", mapperService);

            System.out.println("Mapper ready port:" +port);
        }catch(Exception e) {
            System.out.println("Mapper main " + e.getMessage());
        }
    }
}
