package com.influencer.vo;

import lombok.*;

@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class ContentAnalysisIdsVO {
    private int keywordSeq;
    private String contentId;
    private String insertId;
    private String downloadUrl;
    private String apiStatus;
    private String apiSuccessDate;
    private String insertStatus;
    private String insertSuccessDate;
    private String regdate;
}
