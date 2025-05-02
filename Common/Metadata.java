package Common;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 系统元数据类
 */
public class Metadata {
    
    /**
     * 表的元数据信息
     */
    public static class TableInfo implements Serializable {
        private String tableName;
        private List<ColumnInfo> columns;
        private String primaryKey;
        private List<IndexInfo> indexes;
        
        public TableInfo(String tableName) {
            this.tableName = tableName;
            this.columns = new ArrayList<>();
            this.indexes = new ArrayList<>();
        }
        
        public String getTableName() {
            return tableName;
        }
        
        public List<ColumnInfo> getColumns() {
            return columns;
        }
        
        public void addColumn(ColumnInfo column) {
            columns.add(column);
        }
        
        public String getPrimaryKey() {
            return primaryKey;
        }
        
        public void setPrimaryKey(String primaryKey) {
            this.primaryKey = primaryKey;
        }
        
        public List<IndexInfo> getIndexes() {
            return indexes;
        }
        
        public void addIndex(IndexInfo index) {
            indexes.add(index);
        }
    }
    
    /**
     * 列的元数据信息
     */
    public static class ColumnInfo implements Serializable {
        private String columnName;
        private DataTypes.DataType dataType;
        private boolean notNull;
        private boolean unique;
        
        public ColumnInfo(String columnName, DataTypes.DataType dataType, boolean notNull, boolean unique) {
            this.columnName = columnName;
            this.dataType = dataType;
            this.notNull = notNull;
            this.unique = unique;
        }
        
        public String getColumnName() {
            return columnName;
        }
        
        public DataTypes.DataType getDataType() {
            return dataType;
        }
        
        public boolean isNotNull() {
            return notNull;
        }
        
        public boolean isUnique() {
            return unique;
        }
    }
    
    /**
     * 索引的元数据信息
     */
    public static class IndexInfo implements Serializable {
        private String indexName;
        private String tableName;
        private String columnName;
        private boolean unique;
        
        public IndexInfo(String indexName, String tableName, String columnName, boolean unique) {
            this.indexName = indexName;
            this.tableName = tableName;
            this.columnName = columnName;
            this.unique = unique;
        }
        
        public String getIndexName() {
            return indexName;
        }
        
        public String getTableName() {
            return tableName;
        }
        
        public String getColumnName() {
            return columnName;
        }
        
        public boolean isUnique() {
            return unique;
        }
    }
    
    /**
     * 表区域信息，用于记录表在哪些RegionServer上
     */
    public static class TableRegionInfo implements Serializable {
        private String tableName;
        private List<String> regionServers;
        
        public TableRegionInfo(String tableName) {
            this.tableName = tableName;
            this.regionServers = new ArrayList<>();
        }
        
        public String getTableName() {
            return tableName;
        }
        
        public List<String> getRegionServers() {
            return regionServers;
        }
        
        public void addRegionServer(String regionServer) {
            if (!regionServers.contains(regionServer)) {
                regionServers.add(regionServer);
            }
        }
        
        public void removeRegionServer(String regionServer) {
            regionServers.remove(regionServer);
        }
    }
} 