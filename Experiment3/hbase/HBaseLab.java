import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeSet;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;

/** HBase 2.x API exercises. All demonstrations use dedicated lab3_* tables. */
public class HBaseLab implements AutoCloseable {
    private final Connection connection;

    public HBaseLab() throws IOException {
        connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
    }

    private static byte[] b(String value) { return Bytes.toBytes(value); }

    private static String[] splitColumn(String column) {
        if (column == null || column.isEmpty()) {
            throw new IllegalArgumentException("Column/family must not be empty");
        }
        String[] parts = column.split(":", 2);
        if (parts[0].isEmpty()) throw new IllegalArgumentException("Empty family");
        return parts.length == 1 ? new String[] { parts[0], "" } : parts;
    }

    // This method intentionally replaces an existing table, as required by the lab.
    public void createTable(String tableName, String[] fields) throws IOException {
        if (fields.length == 0) throw new IllegalArgumentException("At least one family is required");
        TableName name = TableName.valueOf(tableName);
        TreeSet<String> families = new TreeSet<>();
        for (String field : fields) families.add(splitColumn(field)[0]);
        try (Admin admin = connection.getAdmin()) {
            if (admin.tableExists(name)) {
                if (admin.isTableEnabled(name)) admin.disableTable(name);
                admin.deleteTable(name);
            }
            TableDescriptorBuilder descriptor = TableDescriptorBuilder.newBuilder(name);
            for (String family : families) {
                descriptor.setColumnFamily(ColumnFamilyDescriptorBuilder.of(family));
            }
            admin.createTable(descriptor.build());
        }
    }

    public void addRecord(String tableName, String row, String[] fields, String[] values)
            throws IOException {
        if (fields.length != values.length) {
            throw new IllegalArgumentException("fields and values must have equal lengths");
        }
        if (fields.length == 0) throw new IllegalArgumentException("No cells supplied");
        Put put = new Put(b(row));
        for (int i = 0; i < fields.length; i++) {
            String[] column = splitColumn(fields[i]);
            put.addColumn(b(column[0]), b(column[1]), b(values[i]));
        }
        try (Table table = connection.getTable(TableName.valueOf(tableName))) { table.put(put); }
    }

    public void listTables() throws IOException {
        try (Admin admin = connection.getAdmin()) {
            for (TableDescriptor descriptor : admin.listTableDescriptors()) {
                System.out.println(descriptor.getTableName() + " " + descriptor);
            }
        }
    }

    public void scanTable(String tableName) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(tableName));
             ResultScanner scanner = table.getScanner(new Scan())) {
            for (Result result : scanner) {
                for (Cell cell : result.rawCells()) {
                    System.out.printf("%s\t%s:%s\t%s%n", Bytes.toString(result.getRow()),
                            Bytes.toString(CellUtil.cloneFamily(cell)),
                            Bytes.toString(CellUtil.cloneQualifier(cell)),
                            Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
        }
    }

    // Scan every row first. Restricting the server scan to the requested column would
    // hide rows with a missing cell and would violate the lab's explicit null rule.
    public void scanColumn(String tableName, String column) throws IOException {
        String[] parts = splitColumn(column);
        boolean wholeFamily = !column.contains(":");
        List<Result> rows = new ArrayList<>();
        TreeSet<String> qualifiers = new TreeSet<>();
        try (Table table = connection.getTable(TableName.valueOf(tableName));
             ResultScanner scanner = table.getScanner(new Scan())) {
            for (Result row : scanner) {
                rows.add(row);
                if (wholeFamily) {
                    NavigableMap<byte[], byte[]> cells = row.getFamilyMap(b(parts[0]));
                    if (cells != null) {
                        for (byte[] qualifier : cells.keySet()) qualifiers.add(Bytes.toString(qualifier));
                    }
                }
            }
        }
        if (!wholeFamily) qualifiers.add(parts[1]);
        for (Result row : rows) {
            if (qualifiers.isEmpty()) {
                System.out.printf("%s\t%s\tnull%n", Bytes.toString(row.getRow()), parts[0]);
            }
            for (String qualifier : qualifiers) {
                byte[] value = row.getValue(b(parts[0]), b(qualifier));
                System.out.printf("%s\t%s:%s\t%s%n", Bytes.toString(row.getRow()),
                        parts[0], qualifier, value == null ? "null" : Bytes.toString(value));
            }
        }
    }

    // Required three-argument signature: obtain the missing new value interactively.
    public void modifyData(String tableName, String row, String column) throws IOException {
        System.out.print("Enter new value: ");
        String value = new BufferedReader(new InputStreamReader(System.in, "UTF-8")).readLine();
        if (value == null) throw new IOException("No new value was supplied on stdin");
        modifyData(tableName, row, column, value);
    }

    public void modifyData(String tableName, String row, String column, String value)
            throws IOException {
        addRecord(tableName, row, new String[] {column}, new String[] {value});
    }

    public void deleteRow(String tableName, String row) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            table.delete(new Delete(b(row)));
        }
    }

    public void deleteColumn(String tableName, String row, String column) throws IOException {
        String[] parts = splitColumn(column);
        Delete delete = new Delete(b(row));
        delete.addColumns(b(parts[0]), b(parts[1])); // Delete every version of this cell.
        try (Table table = connection.getTable(TableName.valueOf(tableName))) { table.delete(delete); }
    }

    public void addFamily(String tableName, String family) throws IOException {
        try (Admin admin = connection.getAdmin()) {
            admin.addColumnFamily(TableName.valueOf(tableName), ColumnFamilyDescriptorBuilder.of(family));
        }
    }

    public void deleteFamily(String tableName, String family) throws IOException {
        try (Admin admin = connection.getAdmin()) {
            admin.deleteColumnFamily(TableName.valueOf(tableName), b(family));
        }
    }

    public void clearTable(String tableName) throws IOException {
        TableName name = TableName.valueOf(tableName);
        try (Admin admin = connection.getAdmin()) {
            if (admin.isTableEnabled(name)) admin.disableTable(name);
            admin.truncateTable(name, true); // Recreates and enables the table; preserves splits.
        }
    }

    public long countRows(String tableName) throws IOException {
        long count = 0;
        Scan scan = new Scan().setFilter(new FirstKeyOnlyFilter()).setCaching(500);
        try (Table table = connection.getTable(TableName.valueOf(tableName));
             ResultScanner scanner = table.getScanner(scan)) {
            for (Result ignored : scanner) count++;
        }
        return count;
    }

    private void loadSamples() throws IOException {
        String student = "lab3_student_api";
        String course = "lab3_course_api";
        String sc = "lab3_sc_api";
        createTable(student, new String[] {"Info"});
        createTable(course, new String[] {"Info"});
        createTable(sc, new String[] {"Info", "Score"});
        String[][] students = {
            {"Zhangsan", "2015001", "male", "23"},
            {"Mary", "2015002", "female", "22"},
            {"Lisi", "2015003", "male", "24"}
        };
        for (String[] s : students) {
            addRecord(student, s[0], new String[] {"Info:S_No", "Info:S_Name", "Info:S_Sex", "Info:S_Age"},
                    new String[] {s[1], s[0], s[2], s[3]});
            addRecord(sc, s[0], new String[] {"Info:S_No"}, new String[] {s[1]});
        }
        String[][] courses = {{"123001", "Math", "2.0"}, {"123002", "Computer Science", "5.0"},
                              {"123003", "English", "3.0"}};
        for (String[] c : courses) {
            addRecord(course, c[0], new String[] {"Info:C_No", "Info:C_Name", "Info:C_Credit"}, c);
        }
        addRecord(sc, "Zhangsan", new String[] {"Score:Math", "Score:English"}, new String[] {"86", "69"});
        addRecord(sc, "Mary", new String[] {"Score:Computer Science", "Score:English"}, new String[] {"77", "99"});
        addRecord(sc, "Lisi", new String[] {"Score:Math", "Score:Computer Science"}, new String[] {"98", "95"});
        for (String name : new String[] {student, course, sc}) {
            System.out.println("=== " + name + " ===");
            scanTable(name);
            System.out.println("ROWS=" + countRows(name));
        }
        System.out.println("=== Entire Score family, including nulls ===");
        scanColumn(sc, "Score");
        System.out.println("=== Score:Math; Mary's value must be null ===");
        scanColumn(sc, "Score:Math");
    }

    private void demonstrateOperations() throws IOException {
        String name = "lab3_ops_api";
        createTable(name, new String[] {"Info"});
        addRecord(name, "old", new String[] {"Info:value"}, new String[] {"will be replaced"});
        createTable(name, new String[] {"Info"});
        System.out.println("AFTER RECREATE ROWS=" + countRows(name));
        addRecord(name, "demo", new String[] {"Info:value"}, new String[] {"before"});
        modifyData(name, "demo", "Info:value", "after");
        scanTable(name);
        addFamily(name, "Extra");
        addRecord(name, "demo", new String[] {"Extra:note"}, new String[] {"temporary"});
        scanTable(name);
        deleteColumn(name, "demo", "Extra:note");
        scanColumn(name, "Extra:note");
        deleteFamily(name, "Extra");
        addRecord(name, "delete_me", new String[] {"Info:value"}, new String[] {"temporary row"});
        deleteRow(name, "delete_me");
        System.out.println("AFTER DELETE ROWS=" + countRows(name));
        clearTable(name);
        System.out.println("AFTER TRUNCATE ROWS=" + countRows(name));
    }

    @Override public void close() throws IOException { connection.close(); }

    public static void main(String[] args) throws Exception {
        try (HBaseLab lab = new HBaseLab()) {
            if (args.length == 4 && args[0].equals("modify")) {
                lab.modifyData(args[1], args[2], args[3]);
                lab.scanColumn(args[1], args[3]);
                return;
            }
            if (args.length != 0) {
                throw new IllegalArgumentException("Usage: HBaseLab [modify table row family:qualifier]");
            }
            lab.loadSamples();
            lab.demonstrateOperations();
            System.out.println("=== All user tables and descriptors ===");
            lab.listTables();
        }
    }
}
