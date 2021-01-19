import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class MapperMain {
    public static void main(String[] args){
       new Thread(){
           public void run(){
               Registry r = null;
               MapperServiceInterface mapperService;
               Integer port = Integer.parseInt(args[0]);
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
           }
       }.start();


        new Thread(){
            public void run(){
                while (true){
                    MasterServiceInterface masterService = null;
                    try{
                        Thread.sleep(1000);
                        System.out.println("Mapper["+args[0]+"] is heartbeat alive");
                        masterService = (MasterServiceInterface) Naming.lookup("rmi://localhost:2023/masterservice");
                        masterService.heartbeat_check("rmi://localhost:"+args[0]+"/mapperservice","mapper");
                        Thread.sleep(14*1000);
                    }catch(Exception e){e.printStackTrace();}
                }
            }
        }.start();
    }
}
