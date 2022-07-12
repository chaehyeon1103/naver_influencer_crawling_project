package com.influencer.vo;

import lombok.*;

import java.sql.Date;

@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class LogVO {
    private int seq;
    private String date;
    private String hh;
    private int keywordSeq;
    private String channelType;
    private int taskType;
    private int taskStep;
    private String taskName;
    private int taskStatus;
    private String taskMessage;
    private Date taskStartdate;
    private Date taskEnddate;
    private String taskFileName;
    private int status;
    private int retryCount;
    private int mailCount;
    private int countingTarget;
    private int countingEnd;
    private Date regdate;

//    private String taskTable;
//    private String summaryTaskName;
//    private int summaryStatus;
}
