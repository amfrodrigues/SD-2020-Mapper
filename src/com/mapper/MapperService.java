package com.mapper;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class MapperService extends UnicastRemoteObject implements MapperServiceInterface {

    public MapperService() throws RemoteException {
    }
}
