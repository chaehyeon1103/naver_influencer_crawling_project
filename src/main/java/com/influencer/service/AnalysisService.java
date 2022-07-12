package com.influencer.service;

import com.influencer.vo.ContentAnalysisIdsVO;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.util.List;

public interface AnalysisService {

    //형태소 분석 시작
    public void analysisStart(String nowdate);

    //status 정기 체크
    public void statusCheck();

    //분석 형태소 데이터 정기 DB 입력
    public void analysisDataInsert(String nowdate) throws IOException, ParseException;
}
