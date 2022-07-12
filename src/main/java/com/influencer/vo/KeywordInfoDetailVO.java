package com.influencer.vo;

import lombok.*;

import java.sql.Date;

@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class KeywordInfoDetailVO {
    private int seq;
    private int logSeq;
    private int keywordSeq;
    private String type;
    private int count;
    private Date regdate;
}
