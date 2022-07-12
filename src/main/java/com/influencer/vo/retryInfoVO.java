package com.influencer.vo;

import lombok.*;

@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class retryInfoVO {
    private int keywordSeq;
    private String channelType;
    private int taskType;
    private int taskStep;
    private String date;
    private String hh;
}
