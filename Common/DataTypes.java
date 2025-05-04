package Common;

import java.io.Serializable;

/**
 * 定义系统中使用的数据类型
 */
public class DataTypes {
    // 基本数据类型枚举
    public enum Type {
        INT,
        FLOAT,
        CHAR,
        VARCHAR
    }
    
    // 数据类型基类
    public abstract static class DataType implements Serializable {
        private static final long serialVersionUID = 1L;
        
        protected Type type;
        
        public Type getType() {
            return type;
        }
        
        public abstract int getSize();
        public abstract String toString();
    }
    
    // 整型
    public static class IntType extends DataType {
        private static final long serialVersionUID = 2L;
        
        public IntType() {
            this.type = Type.INT;
        }
        
        @Override
        public int getSize() {
            return 4;
        }
        
        @Override
        public String toString() {
            return "INT";
        }
    }
    
    // 浮点型
    public static class FloatType extends DataType {
        private static final long serialVersionUID = 3L;
        
        public FloatType() {
            this.type = Type.FLOAT;
        }
        
        @Override
        public int getSize() {
            return 4;
        }
        
        @Override
        public String toString() {
            return "FLOAT";
        }
    }
    
    // 字符型
    public static class CharType extends DataType {
        private static final long serialVersionUID = 4L;
        
        private int length;
        
        public CharType(int length) {
            this.type = Type.CHAR;
            this.length = length;
        }
        
        public int getLength() {
            return length;
        }
        
        @Override
        public int getSize() {
            return length;
        }
        
        @Override
        public String toString() {
            return "CHAR(" + length + ")";
        }
    }
    
    // 可变长字符型
    public static class VarcharType extends DataType {
        private static final long serialVersionUID = 5L;
        
        private int maxLength;
        
        public VarcharType(int maxLength) {
            this.type = Type.VARCHAR;
            this.maxLength = maxLength;
        }
        
        public int getMaxLength() {
            return maxLength;
        }
        
        @Override
        public int getSize() {
            return maxLength;
        }
        
        @Override
        public String toString() {
            return "VARCHAR(" + maxLength + ")";
        }
    }
} 