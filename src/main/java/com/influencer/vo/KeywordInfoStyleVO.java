package com.influencer.vo;

import lombok.*;

import java.sql.Date;

@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class KeywordInfoStyleVO {
    private int seq;
    private int logSeq;
    private int keywordSeq;
    private String keywordInfoTitle;
    private String keywordInfoType;
    private Date regdate;
}
