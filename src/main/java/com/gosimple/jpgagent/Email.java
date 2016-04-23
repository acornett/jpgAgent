package com.gosimple.jpgagent;

import java.io.Serializable;

public class Email implements Serializable {
    Integer queueID;
    String[] to;
    String subject;
    String body;

    public Email(Integer queueID, String[] to, String subject, String body) {
        this.queueID = queueID;
        this.to = to;
        this.subject = subject;
        this.body = body;
    }

    public Integer getQueueID() {
        return queueID;
    }

    public String[] getTo() {
        return to;
    }

    public String getSubject() {
        return subject;
    }

    public String getBody() {
        return body;
    }
}
