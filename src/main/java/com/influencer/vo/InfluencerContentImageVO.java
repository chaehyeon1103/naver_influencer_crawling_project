package com.influencer.vo;

import lombok.*;

import java.util.Date;

@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class InfluencerContentImageVO {
    private int seq;
    private int logSeq;
    private int keywordSeq;
    private String contentNum;
    private String contentImageUrl;
    private Date regdate;
}
