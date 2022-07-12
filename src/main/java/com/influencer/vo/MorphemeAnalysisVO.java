package com.influencer.vo;

import lombok.*;

@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class MorphemeAnalysisVO {
    private int keywordSeq;
    private String contentId;
    private String contentWord;
    private String wordClass;
    private int count;
    private String regdate;
}
