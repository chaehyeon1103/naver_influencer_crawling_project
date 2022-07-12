package com.influencer.service;

import com.influencer.vo.ContentAnalysisIdsVO;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface MorphemeService {

    //content 제목, 내용 get & txt파일 생성
    public List<ContentAnalysisIdsVO> getContent();

    //형태소 분석 시작 api 사용
    public List<ContentAnalysisIdsVO> analysisStart(List<ContentAnalysisIdsVO> contentList, String formatedNow) throws IOException;

    //형태소 분석 상태 확인 및 입력
    public void analysisStatus(List<ContentAnalysisIdsVO> contentList, String formatedNow);
}
