package com.influencer.vo;

import lombok.*;

import java.sql.Date;

@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class RecommendedInfluencerVO {
    private int seq;
    private int logSeq;
    private int keywordSeq;
    private String contentNum;
    private String influencerImg;
    private String influencerName;
    private String influencerType;
    private String influencerTypeDetail;
    private String fanCount;
    private String contentTitle;
    private String contentDesc;
    private String contentImg;
    private String contentLink;
    private String contentPostDate;
    private Date regdate;
}
