package com.influencer.vo;

import lombok.*;

import java.sql.Date;

@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class KeywordVO {
    private int seq;
    private String keyword;
    private String status;
    private Date regdate;
}
