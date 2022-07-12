package com.influencer.controller;

import com.influencer.service.InfluencerService;
import com.influencer.vo.KeywordVO;
import com.influencer.vo.timeVO;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

@Component
@Controller
@RequestMapping("/influencer")
@RequiredArgsConstructor
public class InfluencerController {

    private final Logger logger = LoggerFactory.getLogger(InfluencerController.class);

    @Setter(onMethod_=@Autowired)
    private InfluencerService service;

    @Scheduled(cron = "0 0/30 * * * *")
    @GetMapping("/crawling")
    public void get() throws IOException {

        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd_HHmm");
        String formatedNow = now.format(formatter);

        DateTimeFormatter formatter2 = DateTimeFormatter.ofPattern("yyyyMMdd");
        String date = now.format(formatter2);

        DateTimeFormatter formatter3 = DateTimeFormatter.ofPattern("HH");
        String hours = now.format(formatter3);

        DateTimeFormatter formatter4 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH");
        String datetime = now.format(formatter4);

        timeVO time = service.getTime();

        // LOG테이블에 데이터가 있는지 없는지 체크 (3시간에 한번 수집)
        // 정기첫수집 조건 -> hh % 3 == 0 && 현재시간 hh 기준 log 테이블 rows 수 == 0
        List<KeywordVO> keywordList;
        int logCount = service.confirmFirst(date, hours);
        boolean retryFlag;

        retryFlag = false;
        keywordList = service.getKeyword();

        logger.info("hour는 : " + hours);
        logger.info("log 수는 : " + logCount);

        if(Integer.parseInt(hours) % 3 == 0 && logCount == 0) {
            logger.info("수집 start");
            retryFlag = false;
        } else {
            logger.info("재수집 retry start");
            retryFlag = true;
        }

        logger.info("retryFlag는 : " + retryFlag);

        //keyword별로 정보 수집
        for (KeywordVO keywordVO : keywordList) {
            try {
                String keyword = keywordVO.getKeyword();
                int keywordSeq = keywordVO.getSeq();

                //html 파일 저장 -----------------------------------------------------------------------
                String filePath = service.crawling(keywordSeq, keyword, formatedNow, retryFlag, time);
                List<String> channelFilePathList = service.channelCrawling(keywordSeq, keyword, formatedNow, retryFlag, time);

                //html 파싱 및 csv 저장 -----------------------------------------------------------------------
                List<String> contentFilePathList = new ArrayList<>();
                List<String> contentImageFilePathList = new ArrayList<>();
                List<String> relatedContentFilePathList = new ArrayList<>();

                String influencerInfoFilePath = service.parsingKeywordInfluencerInfo(keywordSeq, keyword, filePath, formatedNow, retryFlag, time);
                String influencerDetailFilePath = service.parsingKeywordInfluencerDetail(keywordSeq, keyword, filePath, formatedNow, retryFlag, time);
                String infoStyleFilePath = service.parsingKeywordInfoStyle(keywordSeq, keyword, filePath, formatedNow, retryFlag, time);
                String recInfluencerFilePath = service.parsingRecommandedInfluencer(keywordSeq, keyword, filePath, formatedNow, retryFlag, time);
                String popularKeywordFilePath = service.parsingPopularKeyword(keywordSeq, keyword, filePath, formatedNow, retryFlag, time);

                for (String channelFilePath : channelFilePathList) {
                    //channel type 구분
                    String channelType = channelFilePath.substring(channelFilePath.length()-6, channelFilePath.length()-5);

                    String influencerContentFilePath = service.parsingInfluencerContent(keywordSeq, keyword, channelFilePath, formatedNow, channelType, retryFlag, time);
                    String contentImageFilePath = service.parsingInfluencerContentImage(keywordSeq, keyword, channelFilePath, formatedNow, channelType, retryFlag, time);
                    String relatedContentFilePath = service.parsingRelatedContent(keywordSeq, keyword, channelFilePath, formatedNow, channelType, retryFlag, time);

                    contentFilePathList.add(influencerContentFilePath);
                    contentImageFilePathList.add(contentImageFilePath);
                    relatedContentFilePathList.add(relatedContentFilePath);
                }

                //csv 파일 db 저장 -----------------------------------------------------------------------
                service.insertKeywordInfo(keywordSeq, influencerInfoFilePath, retryFlag, time);
                service.insertKeywordInfoDetail(keywordSeq, influencerDetailFilePath, retryFlag, time);
                service.insertKeywordFashionStyle(keywordSeq, infoStyleFilePath, retryFlag, time);
                service.insertRecommendedInfluencer(keywordSeq, recInfluencerFilePath, retryFlag, time);
                service.insertPopularKeyword(keywordSeq, popularKeywordFilePath, retryFlag, time);

                for (String contentFilePath : contentFilePathList) {
                    //channel type 구분
                    String channelType = contentFilePath.substring(38, 39);
                    service.insertInfluencerContent(keywordSeq, contentFilePath, channelType, retryFlag, time);
                }
                for (String contentImageFilePath : contentImageFilePathList) {
                    //channel type 구분
                    String channelType = contentImageFilePath.substring(38, 39);
                    service.insertInfluencerContentImage(keywordSeq, contentImageFilePath, channelType, retryFlag, time);
                }
                for (String relatedContentFilePath : relatedContentFilePathList) {
                    //channel type 구분
                    String channelType = relatedContentFilePath.substring(38, 39);
                    service.insertRelatedContent(keywordSeq, relatedContentFilePath, channelType, retryFlag, time);
                }

                //sleep (1~5초)
                try {
                    Thread.sleep((int) (Math.random() * 5000) + 1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } catch (Exception e) {
                logger.info(e.getMessage());

                //sleep (1~5초)
                try {
                    Thread.sleep((int) (Math.random() * 5000) + 1);
                } catch (InterruptedException ee) {
                    ee.printStackTrace();
                }

                continue;
            }
        }
        logger.info("수집 end");

        //집계수집여부(counting_target) 판별
        //오늘 날짜의 집계대상여부 0으로 초기화
        service.todayTarget0(date);
        //오늘 날짜의 가장 최근 데이터들만 집계대상여부 1로 변경
        service.todayTarget1(date);

        //오류 알림톡 발송
        Date datee = new Date();

        Calendar cal = Calendar.getInstance();
        cal.setTime(datee);
        SimpleDateFormat sdformat = new SimpleDateFormat("HH");
        cal.add(Calendar.MINUTE, 30);
        String hh3 = sdformat.format(cal.getTime());

        timeVO time2 = service.getTime();
        String hh2 = time2.getHh();

        if(!hh3.equals(hh2) && Integer.parseInt(hh3) % 3 == 0) {
            service.failAlimTalk(time, datetime);
        }
    }
}
