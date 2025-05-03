package Client;

import Common.Message;
import Common.Metadata;

import java.util.List;
import java.util.Scanner;

/**
 * 客户端启动类
 */
public class ClientMain {
    
    private static final String DEFAULT_MASTER_HOST = "localhost";
    private static final int DEFAULT_MASTER_PORT = 8000;
    
    private static ClientService clientService;
    private static Scanner scanner;
    
    public static void main(String[] args) {
        try {
            scanner = new Scanner(System.in);
            
            // 创建客户端服务
            clientService = new ClientServiceImpl();
            
            // 连接到服务器
            String masterHost = DEFAULT_MASTER_HOST;
            int masterPort = DEFAULT_MASTER_PORT;
            
            if (args.length >= 1) {
                masterHost = args[0];
            }
            
            if (args.length >= 2) {
                try {
                    masterPort = Integer.parseInt(args[1]);
                } catch (NumberFormatException e) {
                    System.err.println("Invalid port number: " + args[1]);
                }
            }
            
            System.out.println("Connecting to Master at " + masterHost + ":" + masterPort + "...");
            boolean connected = clientService.connect(masterHost, masterPort);
            
            if (!connected) {
                System.err.println("Failed to connect to Master");
                return;
            }
            
            // 添加关闭钩子
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    System.out.println("Disconnecting from server...");
                    clientService.disconnect();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }));
            
            // 显示欢迎信息
            System.out.println("Welcome to Distributed MiniSQL Client");
            System.out.println("Type 'help' for help, 'exit' to quit");
            
            // 进入命令循环
            commandLoop();
            
            // 关闭资源
            scanner.close();
            clientService.disconnect();
        } catch (Exception e) {
            System.err.println("Client exception: " + e.toString());
            e.printStackTrace();
        }
    }
    
    /**
     * 命令循环
     */
    private static void commandLoop() {
        while (true) {
            System.out.print("minisql> ");
            String command = scanner.nextLine().trim();
            
            if (command.isEmpty()) {
                continue;
            }
            
            if (command.equalsIgnoreCase("exit") || command.equalsIgnoreCase("quit")) {
                break;
            }
            
            if (command.equalsIgnoreCase("help")) {
                showHelp();
                continue;
            }
            
            if (command.equalsIgnoreCase("tables")) {
                showTables();
                continue;
            }
            
            if (command.equalsIgnoreCase("servers")) {
                showServers();
                continue;
            }
            
            // 执行SQL命令
            executeCommand(command);
        }
    }
    
    /**
     * 显示帮助信息
     */
    private static void showHelp() {
        System.out.println("Available commands:");
        System.out.println("  help                  - Show this help message");
        System.out.println("  exit, quit            - Exit the client");
        System.out.println("  tables                - List all tables");
        System.out.println("  servers               - List all RegionServers");
        System.out.println("");
        System.out.println("SQL commands:");
        System.out.println("  CREATE TABLE table_name (column_name data_type [constraints], ...)");
        System.out.println("  DROP TABLE table_name");
        System.out.println("  CREATE [UNIQUE] INDEX index_name ON table_name (column_name)");
        System.out.println("  DROP INDEX index_name ON table_name");
        System.out.println("  INSERT INTO table_name (column1, column2, ...) VALUES (value1, value2, ...)");
        System.out.println("  DELETE FROM table_name [WHERE condition]");
        System.out.println("  SELECT column1, column2, ... FROM table_name [WHERE condition]");
        System.out.println("  UPDATE table_name SET column1 = value1, column2 = value2, ... [WHERE condition]");
    }
    
    /**
     * 显示所有表
     */
    private static void showTables() {
        try {
            List<Metadata.TableInfo> tables = clientService.getAllTables();
            
            if (tables.isEmpty()) {
                System.out.println("No tables found");
                return;
            }
            
            System.out.println("Tables:");
            for (Metadata.TableInfo table : tables) {
                System.out.println("  " + table.getTableName());
                
                System.out.println("    Columns:");
                for (Metadata.ColumnInfo column : table.getColumns()) {
                    String constraints = "";
                    if (column.isNotNull()) {
                        constraints += " NOT NULL";
                    }
                    if (column.isUnique()) {
                        constraints += " UNIQUE";
                    }
                    if (table.getPrimaryKey() != null && table.getPrimaryKey().equals(column.getColumnName())) {
                        constraints += " PRIMARY KEY";
                    }
                    
                    System.out.println("      " + column.getColumnName() + " " + column.getDataType() + constraints);
                }
                
                if (!table.getIndexes().isEmpty()) {
                    System.out.println("    Indexes:");
                    for (Metadata.IndexInfo index : table.getIndexes()) {
                        System.out.println("      " + index.getIndexName() + " on " + index.getColumnName() + (index.isUnique() ? " (UNIQUE)" : ""));
                    }
                }
                
                System.out.println();
            }
        } catch (Exception e) {
            System.err.println("Failed to get tables: " + e.getMessage());
        }
    }
    
    /**
     * 显示所有RegionServer
     */
    private static void showServers() {
        try {
            List<String> servers = clientService.getAllRegionServers();
            
            if (servers.isEmpty()) {
                System.out.println("No RegionServers found");
                return;
            }
            
            System.out.println("RegionServers:");
            for (String server : servers) {
                System.out.println("  " + server);
            }
        } catch (Exception e) {
            System.err.println("Failed to get RegionServers: " + e.getMessage());
        }
    }
    
    /**
     * 执行SQL命令
     */
    private static void executeCommand(String command) {
        try {
            Message response = clientService.executeSql(command);
            
            if (response.getType() == Common.Message.MessageType.RESPONSE_ERROR) {
                System.err.println("Error: " + response.getData("error"));
            } else {
                System.out.println("OK");
            }
        } catch (Exception e) {
            System.err.println("Error executing command: " + e.getMessage());
        }
    }
} 