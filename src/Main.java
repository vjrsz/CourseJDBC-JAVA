import db.DB;
import db.DBException;
import db.DBIntegrityException;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class Main {
    public static Connection conn = null;
    public static void main(String[] args) {
        conn = DB.getConn();

        try {

            // insert
            insertSeller("joão", "joao@gmail.com", "10/08/2003", 1000.0, 4);

            // update
            updateBaseSalarySellerByDepartmentID(100.0, 4);

            // delete
            deleteDepartment(4);

            // get
            allDepartment();

            /* commit
                conn.setAutoCommit(false); // Disable automatic commit
                conn.commit(); // Confirm transactions
            */

        } catch (SQLException e) {
            try {
                conn.rollback(); // In case of error, roll back all pending transactions
                throw new DBException(e.getMessage());
            } catch (SQLException ex) {
                throw new DBException(ex.getMessage());
            }
        } catch (ParseException e) {
            throw new RuntimeException(e);
        } finally {
            // conn.setAutoCommit(true); // Enable automatic commit again
            DB.closeConn();
            DB.closeConn();
            // DB.closeStatement(st);
            // DB.closeResultSet(rs);
        }
    }

    public static void allDepartment() throws SQLException {
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("select * from department");
        System.out.println("Table: Departments");
        while(rs.next()){
            System.out.println(rs.getInt("id") + ", " + rs.getString("name"));
        }
    }

    public static void insertSeller(String name, String email, String birthdate, Double baseSalary, int departmentID) throws SQLException, ParseException {
        PreparedStatement pst  = conn.prepareStatement(
                "INSERT INTO seller " +
                        "(Name, Email, BirthDate, BaseSalary, DepartmentId)" +
                        "VALUES (?, ?, ?, ?, ?)",
                Statement.RETURN_GENERATED_KEYS
        );
        pst.setString(1, name);
        pst.setString(2, email);
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
        pst.setDate(3,  new java.sql.Date(sdf.parse(birthdate).getTime()));
        pst.setDouble(4, baseSalary);
        pst.setInt(5, departmentID);
        printIds(pst.executeUpdate(), pst);
    }

    public static void updateBaseSalarySellerByDepartmentID(Double baseSalaryAdditional, int departmentID) throws SQLException {
        PreparedStatement pst = conn.prepareStatement(
                        "UPDATE seller " +
                                "SET BaseSalary = BaseSalary + ?" +
                                "WHERE DepartmentId = ?",
                        Statement.RETURN_GENERATED_KEYS
                );
        pst.setDouble(1, baseSalaryAdditional);
        pst.setInt(2, departmentID);
        printIds(pst.executeUpdate(), pst);
    }

    public static void deleteDepartment(int departmentID) {
        try{
            PreparedStatement pst = conn.prepareStatement(
                    "DELETE from department " +
                            "WHERE Id = ?"
            );
            pst.setInt(1, departmentID);

            int rowsAffected = pst.executeUpdate();
            System.out.println("Done! Rows Affected: " + rowsAffected);
        } catch (SQLException e){
            throw new DBIntegrityException(e.getMessage());
        }
    }
    public static void printIds(int rowsAffected, PreparedStatement pst){
        try {
            if( rowsAffected > 0 ){
                ResultSet rs = pst.getGeneratedKeys();
                System.out.println("Done! Rows Affected: " + rowsAffected);
                while(rs.next()) {
                    System.out.println("Id: " + rs.getInt(1));
                }
                System.out.println();
            }else{
                System.out.println("No rows Affected");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}