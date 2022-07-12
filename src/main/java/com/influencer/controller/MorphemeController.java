package com.influencer.controller;

import com.influencer.service.AnalysisService;
import com.influencer.service.InfluencerService;
import com.influencer.service.MorphemeService;
import com.influencer.vo.ContentAnalysisIdsVO;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Controller;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

@Component
@Controller
@RequestMapping("/influencer")
@RequiredArgsConstructor
public class MorphemeController {

    private final Logger logger = LoggerFactory.getLogger(MorphemeController.class);

    @Setter(onMethod_=@Autowired)
    private AnalysisService service;

    //    @Scheduled(cron = "0 0/30 * * * *")
    @GetMapping("/morphologyAnalysis")
    public void morphology() throws IOException, ParseException {

        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd_HHmm");
        String nowdate = now.format(formatter);

        //형태소 분석 시작
        service.analysisStart(nowdate);
        //sleep (1~5초) => 바로 실행하면 아직 대기중이라 작동 안됨
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //status 정기 체크
        service.statusCheck();
        //분석 형태소 데이터 정기 DB 입력
        service.analysisDataInsert(nowdate);
    }
}
