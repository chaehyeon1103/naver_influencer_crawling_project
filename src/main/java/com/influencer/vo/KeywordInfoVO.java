package com.influencer.vo;

import lombok.*;

import java.sql.Date;

@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class KeywordInfoVO {
    private int seq;
    private int logSeq;
    private int keywordSeq;
    private String type;
    private int totalCount;
    private Date regdate;
}
