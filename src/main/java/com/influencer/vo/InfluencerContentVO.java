package com.influencer.vo;

import lombok.*;

import java.sql.Date;

@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class InfluencerContentVO {
    private int seq;
    private int logSeq;
    private int keywordSeq;
    private String channelType;
    private String contentNum;
    private String influencerImg;
    private String influencerImgLink;
    private String influencerName;
    private String influencerType;
    private String influencerTypeDetail;
    private String fanCount;
    private String snsFollowerTypeCount;
    private String influencerInfo1Title;
    private String influencerInfo2Title;
    private String influencerInfo3Title;
    private String influencerInfo1Desc;
    private String influencerInfo2Desc;
    private String influencerInfo3Desc;
    private String contentTitle;
    private String contentDesc;
    private String contentLink;
    private String contentPostDate;
    private Date regdate;
}
