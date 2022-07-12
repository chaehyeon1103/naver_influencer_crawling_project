package com.influencer.vo;

import lombok.*;

import java.sql.Date;

@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class PopularKeywordVO {
    private int seq;
    private int logSeq;
    private int keywordSeq;
    private String image;
    private String keyword;
    private int count;
    private Date regdate;
}
