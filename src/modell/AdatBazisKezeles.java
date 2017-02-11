package modell;

import hrremote.AdatbazisKapcsolat;
import hrremote.Reszleg;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

public class AdatBazisKezeles implements AdatbazisKapcsolat {
  private static Connection kapcsolat;

  private static void kapcsolatNyit() {
    try {
      Class.forName(DRIVER);
      kapcsolat = DriverManager.getConnection(URL, USER, PASSWORD);
    }
    catch (ClassNotFoundException e) {
      System.out.println("Hiba! Hiányzik a JDBC driver.");
    }
    catch (SQLException e) {
      System.out.println("Hiba! Nem sikerült megnyitni a kapcsolatot az adatbázis-szerverrel.");
    }
  }

  private static void kapcsolatZar() {
    try {
      kapcsolat.close();
    }
    catch (SQLException e) {
      System.out.println("Hiba! Nem sikerült lezárni a kapcsolatot az adatbázis-szerverrel.");
    }
  } 
  
  //***************************************************
  
  public static int lekerdezReszlegFonoke(int reszlegId) {
     int managerId=100;
     try {
      kapcsolatNyit();
      PreparedStatement ps=kapcsolat.prepareStatement(
        "SELECT MANAGER_ID\n" +
        "FROM DEPARTMENTS\n" +
        "WHERE DEPARTMENT_ID=?");
      ps.setInt(1, reszlegId);
      ResultSet rs=ps.executeQuery();        
      rs.next();
      managerId=rs.getInt("MANAGER_ID");
    }
    catch (SQLException e) {
      System.out.println(e.getMessage());
    }
    kapcsolatZar(); 
    return managerId;
  }
  
  public static int[] lekerdezMinMaxFizetes(String munkakorAzonosito) { //Adott munkakorhoz tartozo min es max fizetes
    int[] minmaxFizetes={0,0};
    try {
      kapcsolatNyit();
      PreparedStatement ps=kapcsolat.prepareStatement(
        "SELECT MIN_SALARY AS MINFIZETÉS, MAX_SALARY AS MAXFIZETÉS \n" +
        "FROM JOBS\n" +
        "WHERE JOB_ID=?");
      ps.setString(1, ""+munkakorAzonosito);
      ResultSet rs=ps.executeQuery();        
      rs.next();
      minmaxFizetes[0]=rs.getInt("MINFIZETÉS");
      minmaxFizetes[1]=rs.getInt("MAXFIZETÉS");
    }
    catch (SQLException e) {
      System.out.println(e.getMessage());
    }
    kapcsolatZar();
    return minmaxFizetes;
  }
    
  public static int lekerdezMinFizetes(String munkakorAzonosito) { //Adott munkakorhoz tartozo min fizetes
    int fizetes=0;
    try {
      kapcsolatNyit();
      PreparedStatement ps=kapcsolat.prepareStatement(
        "SELECT MIN_SALARY AS MINFIZETÉS \n" +
        "FROM JOBS\n" +
        "WHERE JOB_TITLE=?");
      ps.setString(1, ""+munkakorAzonosito);
      ResultSet rs=ps.executeQuery();        
      rs.next();
      fizetes=rs.getInt("MINFIZETÉS");
    }
    catch (SQLException e) {
      System.out.println(e.getMessage());
    }
    kapcsolatZar();    
    return fizetes;
  }

  public static int lekerdezMaxFizetés(String munkakorAzonosito) { //Adott munkakorhoz tartozo max fizetes
    int fizetes=0;
    try {
      kapcsolatNyit();
      PreparedStatement ps=kapcsolat.prepareStatement(
        "SELECT MAX_SALARY AS MAXFIZETÉS \n" +
        "FROM JOBS\n" +
        "WHERE JOB_TITLE=?");
      ps.setString(1, ""+munkakorAzonosito);
      ResultSet rs=ps.executeQuery();        
      rs.next();
      fizetes=rs.getInt("MAXFIZETÉS");
    }
    catch (SQLException e) {
      System.out.println(e.getMessage());
    }
    kapcsolatZar();
    return fizetes;
  }

  public static int[] lekerdezesOsszFizLetszReszlegenBelul(int reszlegID) { 
    //Adott reszlegen belul dolgozok osszfizetese es letszama
    int[] osszFizetesEsLetszam={0,0};
    try {
      kapcsolatNyit();
      PreparedStatement ps=kapcsolat.prepareStatement(
        "SELECT SUM(SALARY) AS osszFizetes, COUNT(SALARY) AS osszLetszam \n" +
        "FROM EMPLOYEES\n" +
        "WHERE DEPARTMENT_ID=?");
      ps.setString(1, ""+reszlegID);
      ResultSet rs=ps.executeQuery();        
      rs.next();
      osszFizetesEsLetszam[0]=rs.getInt("osszFizetes");
      osszFizetesEsLetszam[1]=rs.getInt("osszLetszam");
    }
    catch (SQLException e) {
      System.out.println(e.getMessage());
    }
    kapcsolatZar();
    return osszFizetesEsLetszam;    
  }

  //******************Insert!
	public static boolean ujDolgozoFelvetele(String firstName, String lastName, 
      String email, String phoneNumber, String jobId, int salary, double commissionPCT, int managerID, int departmentID) 
      throws SQLException {
    boolean beszurasOk=false;
    kapcsolatNyit();
		String insertTableSQL = 
        "INSERT INTO EMPLOYEES \n" +
        "(EMPLOYEE_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE_NUMBER, HIRE_DATE, JOB_ID, SALARY, " +
        "COMMISSION_PCT, MANAGER_ID, DEPARTMENT_ID) \n" +
        "VALUES \n" +
        "( (select max(employee_id) from employees)+1, ?1, ?2, ?3, "+
        "?4, (select sysdate from sys.dual), ?5, ?6, ?7, ?8, ?9)";    
    try {
      kapcsolatNyit();
      PreparedStatement ps=kapcsolat.prepareStatement(insertTableSQL);
      ps.setString(1, firstName);
      ps.setString(2, lastName);
      ps.setString(3, email);
      ps.setString(4, phoneNumber);
      ps.setString(5, jobId);
      ps.setInt(6, salary);
      if (commissionPCT==-1)
        ps.setInt(7, 0); //ide null ertek kellene, ps.setNull(7, NULL);
      else
        ps.setDouble(7, commissionPCT);
      ps.setInt(8, managerID);
      ps.setInt(9, departmentID);      
      ResultSet rs=ps.executeQuery();        
      rs.next();
      beszurasOk=true;
    }
    catch (SQLException e) {
      System.out.println(e.getMessage());
    }
    kapcsolatZar();
    return beszurasOk;
  }
 
public static boolean modositFizetés(int dolgozoID, int ujFizetes){
  PreparedStatement ps = null;
  boolean ok=false;
  String fizetesModositoSQL =
      "UPDATE EMPLOYEES \n" +
      "SET SALARY=? \n" +
      "WHERE EMPLOYEE_ID=? ";
  
  kapcsolatNyit();
  try {
      kapcsolat.setAutoCommit(false);
      ps = kapcsolat.prepareStatement(fizetesModositoSQL);
      ps.setString(1, ""+ujFizetes);
      ps.setDouble(2, dolgozoID);
      ps.executeUpdate();
      kapcsolat.commit();
      ok=true;
  }
  catch (SQLException e ) {
    System.out.println(e.getMessage());
    //JDBCTutorialUtilities.printSQLException(e);
    if (kapcsolat != null) {
      try {
        System.err.print("A tranzakció visszagörgetésre kerül!");
        kapcsolat.rollback();
      } catch(SQLException excep) {
        System.out.println(e.getMessage());
        //JDBCTutorialUtilities.printSQLException(excep);
        }
      }
    } finally {
      try {
        if (ps != null) {
          ps.close();
        }
        kapcsolat.setAutoCommit(true);
      } catch (SQLException sQLException) {
        sQLException.printStackTrace();
      }
    }
    kapcsolatZar();
    return ok;
  }

  public static ArrayList<hrremote.Dolgozo> lekerdezDolgozokListajaAdottReszleghez(int reszlegId) { //Adott reszleg dolgozoi
    ArrayList<hrremote.Dolgozo> lista=new ArrayList<>();
    try {
      kapcsolatNyit();
      PreparedStatement ps;
      if(reszlegId==-1){
      ps=kapcsolat.prepareStatement("SELECT E.EMPLOYEE_ID as empId, \n" +
                                    "E.FIRST_NAME || ' ' || E.LAST_NAME AS name, \n" +
                                    "D.DEPARTMENT_ID as depId, \n" +
                                    "D.DEPARTMENT_NAME AS depName, \n" +
                                    "JOBS.JOB_TITLE as jobTitle,\n" +
                                    "E.SALARY as SALARY,\n" +
                                    "E.HIRE_DATE \n" +
                                    "FROM JOBS, EMPLOYEES E \n" +
                                    "LEFT JOIN DEPARTMENTS D \n" +
                                    "ON D.DEPARTMENT_ID = E.DEPARTMENT_ID \n" +
                                    "WHERE JOBS.JOB_ID=E.JOB_ID \n" +
                                    "ORDER BY name");
        
      }else{
        ps=kapcsolat.prepareStatement(
              "SELECT EMP_DETAILS_VIEW.EMPLOYEE_ID as empId,\n" +
              "EMP_DETAILS_VIEW.FIRST_NAME || ' ' || EMP_DETAILS_VIEW.LAST_NAME as name,\n" +
              "EMP_DETAILS_VIEW.DEPARTMENT_ID as depId,\n" +
              "EMP_DETAILS_VIEW.DEPARTMENT_NAME as depName,\n" +
              "EMP_DETAILS_VIEW.JOB_TITLE as jobTitle,\n" +
              "EMP_DETAILS_VIEW.SALARY as SALARY,\n" +
              "EMPLOYEES.HIRE_DATE \n" +
              "FROM JOBS JOBS,\n" +
              "EMP_DETAILS_VIEW EMP_DETAILS_VIEW,\n" +
              "EMPLOYEES\n" +
              "WHERE EMP_DETAILS_VIEW.JOB_ID=JOBS.JOB_ID\n" +
              "AND EMP_DETAILS_VIEW.DEPARTMENT_ID=? \n" +
              "AND EMP_DETAILS_VIEW.EMPLOYEE_ID=EMPLOYEES.EMPLOYEE_ID\n" +
              "ORDER BY NAME");
        ps.setString(1, ""+reszlegId);
      }
      ResultSet rs=ps.executeQuery();
      while(rs.next()){
        hrremote.Dolgozo dolgozo = new hrremote.Dolgozo(rs.getInt("empId"), 
                                      rs.getString("name"), 
                                      rs.getInt("depId"), 
                                      rs.getInt("depId") == 0 ? "Részleg nélküli":rs.getString("depName"),
                                      rs.getString("jobTitle"), 
                                      rs.getInt("SALARY"),
                                      rs.getDate("HIRE_DATE"));
        lista.add(dolgozo);
      }
    }
    catch(SQLException e) {
      System.out.println(e.getMessage());
    }
    kapcsolatZar();
    return lista;    
  }

  public static ArrayList<Reszleg> lekerdezReszleg() {
    ArrayList<Reszleg> lista = new ArrayList<>();
    try {
      kapcsolatNyit();
      Statement s = kapcsolat.createStatement();
      ResultSet rs = s.executeQuery(
              "SELECT DEPARTMENT_ID, DEPARTMENT_NAME\n" +
              "FROM DEPARTMENTS\n" +
              "WHERE DEPARTMENT_ID IN \n" +
              "(SELECT DISTINCT DEPARTMENT_ID FROM EMPLOYEES)\n" +
              "ORDER BY 2");      
      while (rs.next()){
        Reszleg reszleg = new Reszleg(rs.getString("DEPARTMENT_NAME"), rs.getInt("DEPARTMENT_ID"));
        lista.add(reszleg);
      }
    }
    catch (SQLException e) {
      e.printStackTrace();
    }
    kapcsolatZar();
    return lista;
  }
  
  public static ArrayList<hrremote.Munkakor> lekerdezMunkakorok() { //Összes munkakör
    ArrayList<hrremote.Munkakor> lista=new ArrayList<>();
    try {
      kapcsolatNyit();
      Statement s = kapcsolat.createStatement();
      ResultSet rs = s.executeQuery(
        "SELECT JOB_ID, JOB_TITLE, MIN_SALARY, MAX_SALARY\n"+
        "FROM JOBS J\n" +
        "ORDER BY JOB_TITLE");
      while(rs.next()) {
        hrremote.Munkakor munkakor = new hrremote.Munkakor(rs.getString("JOB_TITLE"), rs.getString("JOB_ID"), 
              rs.getInt("MIN_SALARY"), rs.getInt("MAX_SALARY"));
        lista.add(munkakor);
      }
      kapcsolatZar();
    }
    catch(SQLException e) {
      System.out.println(e.getMessage());
    }
    return lista;    
  }
  
  public static ArrayList<String> lekerdezEmail() {
    ArrayList<String> lista = new ArrayList<>();
    try {
      kapcsolatNyit();
      Statement s = kapcsolat.createStatement();
      ResultSet rs = s.executeQuery(
              "SELECT EMAIL FROM EMPLOYEES ORDER BY EMAIL");      
      while (rs.next())
        lista.add(rs.getString("EMAIL"));
    }
    catch (SQLException e) {
      e.printStackTrace();
    }
    kapcsolatZar();
    return lista;
  }

  public static ArrayList<hrremote.Dolgozo> lekerdezFonokokListaja(int reszlegId) {
    ArrayList<hrremote.Dolgozo> lista=new ArrayList<>();
    try {
      kapcsolatNyit();
      PreparedStatement ps;
      ps=kapcsolat.prepareStatement(
              "select employee_id as empId, first_name || ' ' || last_name as name, "+
              "e.department_id as depId, "+
              "d.department_name as depName, "+
              "JOB_TITLE as jobTitle, "+
              "salary, \n" +
              "e.HIRE_DATE \n"+
              "from employees e, departments d, jobs j\n" +
              "where e.department_id=d.department_id and e.job_id=j.job_id and\n" +
              "e.employee_id in (select distinct e.manager_id\n" +
              "from employees e, departments d\n" +
              "where e.department_id=d.department_id and e.department_id=?)\n" +
              "order by name");
      ps.setInt(1, reszlegId);
      ResultSet rs=ps.executeQuery();
      while(rs.next()){
        hrremote.Dolgozo dolgozo = new hrremote.Dolgozo(rs.getInt("empId"), 
                                      rs.getString("name"), 
                                      rs.getInt("depId"), 
                                      rs.getString("depName"), 
                                      rs.getString("jobTitle"), 
                                      rs.getInt("SALARY"), 
                                      rs.getDate("HIRE_DATE"));
        lista.add(dolgozo);
      }
    }
    catch(SQLException e) {
      System.out.println(e.getMessage());
    }
    kapcsolatZar();
    return lista;   
  }
}
