package Common;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * 系统消息类，用于组件间通信
 */
public class Message implements Serializable {
    
    public enum MessageType {
        // 客户端到Master的消息
        CREATE_TABLE,
        DROP_TABLE,
        CREATE_INDEX,
        DROP_INDEX,
        
        // Master到RegionServer的消息
        REGION_CREATE_TABLE,
        REGION_DROP_TABLE,
        REGION_CREATE_INDEX,
        REGION_DROP_INDEX,
        
        // 客户端到RegionServer的消息
        INSERT,
        DELETE,
        SELECT,
        UPDATE,
        
        // 响应消息
        RESPONSE_SUCCESS,
        RESPONSE_ERROR
    }
    
    private MessageType type;
    private String sender;
    private String receiver;
    private Map<String, Object> data;
    
    public Message(MessageType type, String sender, String receiver) {
        this.type = type;
        this.sender = sender;
        this.receiver = receiver;
        this.data = new HashMap<>();
    }
    
    public MessageType getType() {
        return type;
    }
    
    public String getSender() {
        return sender;
    }
    
    public String getReceiver() {
        return receiver;
    }
    
    public Map<String, Object> getData() {
        return data;
    }
    
    public void setData(String key, Object value) {
        data.put(key, value);
    }
    
    public Object getData(String key) {
        return data.get(key);
    }
    
    /**
     * 创建成功响应消息
     */
    public static Message createSuccessResponse(String sender, String receiver) {
        Message response = new Message(MessageType.RESPONSE_SUCCESS, sender, receiver);
        return response;
    }
    
    /**
     * 创建带消息内容的成功响应消息
     */
    public static Message createSuccessResponse(String sender, String receiver, String message) {
        Message response = new Message(MessageType.RESPONSE_SUCCESS, sender, receiver);
        response.setData("message", message);
        return response;
    }
    
    /**
     * 创建错误响应消息
     */
    public static Message createErrorResponse(String sender, String receiver, String errorMessage) {
        Message response = new Message(MessageType.RESPONSE_ERROR, sender, receiver);
        response.setData("error", errorMessage);
        return response;
    }
} 