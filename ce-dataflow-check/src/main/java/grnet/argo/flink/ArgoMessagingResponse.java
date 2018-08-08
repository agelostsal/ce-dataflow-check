package grnet.argo.flink;

import java.util.ArrayList;

public class ArgoMessagingResponse {
    private ReceivedMessages[] receivedMessages;

    public ReceivedMessages[] getReceivedMessages() {
        return this.receivedMessages;
    }

    public void setReceivedMessages(ReceivedMessages[] receivedMessages) {
        this.receivedMessages = receivedMessages;
    }

}

class ReceivedMessages {
    private String ackId;
    private Message message;

    public String getAckId() {
        return ackId;
    }

    public void setAckId(String ackId) {
        this.ackId = ackId;
    }

    public Message getMessage() {
        return message;
    }

    public void setMessage(Message message) {
        this.message = message;
    }

}

class Message{
    private int messageId;
    private String data;
    private String publishTime;

    public int getMessageId() {
        return messageId;
    }

    public void setMessageId(int messageId) {
        this.messageId = messageId;
    }

    public void setData(String data) {
        this.data = data;
    }

    public String getPublishTime() {
        return publishTime;
    }

    public void setPublishTime(String publishTime) {
        this.publishTime = publishTime;
    }

    public String getData() {
        return data;
    }

}
class MessageData implements Cloneable{
    private String message;
    private ArrayList<DataflowError> errors;

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public ArrayList<DataflowError> getErrors() {
        return errors;
    }

    public void setErrors(ArrayList<DataflowError> errors) {
        this.errors = errors;
    }

    public void setError(DataflowError err) { this.errors.add(err); }

    @Override
    public String toString() {
        return "MessageData{" +
                "message='" + message + '\'' +
                ", errors=" + errors +
                '}';
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}
class DataflowError {
    private String service;
    private String errormessage;

    public DataflowError(String service, String errormessage) {
        this.service = service;
        this.errormessage = errormessage;
    }

    public void setService(String service) {
        this.service = service;
    }

    public void setErrormessage(String errormessage) {
        this.errormessage = errormessage;
    }

    @Override
    public String toString() {
        return "DataflowError{" +
                "service='" + service + '\'' +
                ", errormessage='" + errormessage + '\'' +
                '}';
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}