package org.example;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
import java.sql.SQLException;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        XmlToDatabaseLibrary library = new XmlToDatabaseLibrary();
        Scanner scanner = new Scanner(System.in);

        while (true) {
            System.out.println("\n=== XML to Database Library ===");
            System.out.println("1. Show table names");
            System.out.println("2. Show table DDL");
            System.out.println("3. Update all tables");
            System.out.println("4. Update specific table");
            System.out.println("5. Show column names");
            System.out.println("6. Check if column is ID");
            System.out.println("7. Show DDL changes");
            System.out.println("0. Exit");
            System.out.print("Choose option: ");

            try {
                int choice = Integer.parseInt(scanner.nextLine());

                switch (choice) {
                    case 1:
                        System.out.println("Tables: " + library.getTableNames());
                        break;

                    case 2:
                        System.out.print("Enter table name: ");
                        String tableName = scanner.nextLine();
                        System.out.println(library.getTableDDL(tableName));
                        break;

                    case 3:
                        library.update();
                        System.out.println("All tables updated successfully");
                        break;

                    case 4:
                        System.out.print("Enter table name: ");
                        tableName = scanner.nextLine();
                        library.update(tableName);
                        break;

                    case 5:
                        System.out.print("Enter table name: ");
                        tableName = scanner.nextLine();
                        System.out.println("Columns: " + library.getColumnNames(tableName));
                        break;

                    case 6:
                        System.out.print("Enter table name: ");
                        tableName = scanner.nextLine();
                        System.out.print("Enter column name: ");
                        String columnName = scanner.nextLine();
                        boolean isId = library.isColumnId(tableName, columnName);
                        System.out.println("Is ID column: " + isId);
                        break;

                    case 7:
                        System.out.print("Enter table name: ");
                        tableName = scanner.nextLine();
                        System.out.println(library.getDDLChange(tableName));
                        break;

                    case 0:
                        System.out.println("Goodbye!");
                        return;

                    default:
                        System.out.println("Invalid option");
                }
            } catch (NumberFormatException e) {
                System.out.println("Please enter a valid number");
            } catch (SQLException e) {
                System.err.println("Database error: " + e.getMessage());
                e.printStackTrace();
            } catch (Exception e) {
                System.err.println("Error: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }
}