/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package modell;

import java.rmi.server.UnicastRemoteObject;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import hrremote.*;
import java.net.MalformedURLException;
import java.rmi.AlreadyBoundException;
import java.rmi.Naming;
import java.sql.SQLException;
/**
 *
 * @author User
 */
public class ModellSzerver {
  ModellSzerver(){
    szolgáltatásLétrehoz();
  }
  public static void main(String[] args) {
    new ModellSzerver();
  }
  
   private  void szolgáltatásLétrehoz() {
        try {
            LocateRegistry.createRegistry(1099);
            Naming.bind("hrRemote", new Szolgáltatás());
        } catch (RemoteException ex) {
            ;
        } catch (AlreadyBoundException ex) {
           ;// Logger.getLogger(BarlangSzerver.class.getName()).log(Level.SEVERE, null, ex);
        } catch (MalformedURLException ex) {
           ; // Logger.getLogger(BarlangSzerver.class.getName()).log(Level.SEVERE, null, ex);
        }
   }
  private class Szolgáltatás extends UnicastRemoteObject implements HrRemote {

    public Szolgáltatás() throws RemoteException {
      //System.out.println("eddig jó");
    }

  @Override
  public ArrayList<hrremote.Dolgozo> getDolgozokListajaAdottReszleghez(int reszlegID) throws RemoteException {
    return AdatBazisKezeles.lekerdezDolgozokListajaAdottReszleghez(reszlegID );
  }
  @Override
  public ArrayList <hrremote.Dolgozo> getDolgozokListajaOsszesReszleghez (int reszlegID)throws RemoteException{
     throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    //return AdatBazisKezeles.lekerdezOsszesDolgozoListaja(reszlegID);
  }
  @Override
  public ArrayList<hrremote.Reszleg> getReszlegList() throws RemoteException {
    //System.out.println("kérés szerver listáért");
    //System.out.println(AdatBazisKezeles.lekerdezReszleg());
   return AdatBazisKezeles.lekerdezReszleg();
  }

  @Override
  public int getMinfizetes(hrremote.Dolgozo dolgozo) throws RemoteException {
    return AdatBazisKezeles.lekerdezMinFizetes(dolgozo.getMunkakor());
  }

  @Override
  public int getMaxFizetss(Dolgozo dolgozo) throws RemoteException {
   return AdatBazisKezeles.lekerdezMaxFizetés(dolgozo.getMunkakor());
  }

  @Override
  public boolean setFizetes(Dolgozo dolgozo, int fizetes) throws RemoteException {
    return AdatBazisKezeles.modositFizetés(dolgozo.getEmpID(), fizetes);
            }

  @Override
  public boolean setDolgozo(String firstName, String lastName, String email,
          String phoneNumber, String jobId, int salary, double commissionPCT, 
          int managerID, int departmentID) throws RemoteException, SQLException {
//    public static boolean ujDolgozoFelvetele(String firstName, String lastName, 
//      String email, String phoneNumber, String jobId, int salary, double commissionPCT, int managerID, int departmentID)
  return AdatBazisKezeles.ujDolgozoFelvetele(firstName,lastName,email, phoneNumber, jobId,  salary,  commissionPCT,  managerID, departmentID); 
  }

  @Override
  public int[] getOsszFizetesReszlegben(int reszlegId) throws RemoteException {
   return AdatBazisKezeles.lekerdezesOsszFizLetszReszlegenBelul(reszlegId);
           }

  @Override
  public ArrayList<String> getEmailList() throws RemoteException {
    return AdatBazisKezeles.lekerdezEmail(); 
  }

  @Override
  public ArrayList<hrremote.Dolgozo> getFonokList(int reszlegId) throws RemoteException {
    return AdatBazisKezeles.lekerdezFonokokListaja(reszlegId);
  }

  @Override
  public ArrayList<hrremote.Munkakor> getMunkakorok() throws RemoteException {
   return AdatBazisKezeles.lekerdezMunkakorok();
  }

  }
}