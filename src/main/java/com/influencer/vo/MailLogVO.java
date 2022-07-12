package com.influencer.vo;

import lombok.*;

@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class MailLogVO {
    private String keyword;
    private String channelType;
    private int taskType;
    private int taskStep;
    private int retryCount;
    private String errMsg;
    private String ip;
}
