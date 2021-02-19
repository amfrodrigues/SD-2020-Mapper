import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class MapperMain {
    public static void main(String[] args){
       Thread threadMain = new Thread(()->{

               Registry r = null;
               MapperServiceInterface mapperService;
               int port = Integer.parseInt(args[0]);
               try{
                   r = LocateRegistry.createRegistry(port);
               }catch(RemoteException a){
                   a.printStackTrace();
               }

               try{
                   mapperService = new MapperService(args[0]);
                   r.rebind("mapperservice", mapperService);

                   System.out.println("Mapper ready port:" +port);
               }catch(Exception e) {
                   System.out.println("Mapper main " + e.getMessage());
               }

       });
       threadMain.start();


        Thread threadHeathbeat = new Thread(() -> {
            while (true){
                MasterServiceInterface masterService;
                try{
                    Thread.sleep(1000);
                    System.out.println("Mapper["+args[0]+"] is heartbeat alive");
                    masterService = (MasterServiceInterface) Naming.lookup("rmi://localhost:2023/masterservice");
                    masterService.heartbeat_check("rmi://localhost:"+args[0]+"/mapperservice","mapper");
                    Thread.sleep(14*1000);
                }catch(Exception e){e.printStackTrace();}
            }
        });
        threadHeathbeat.start();
    }
}
