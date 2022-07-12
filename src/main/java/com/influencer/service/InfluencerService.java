package com.influencer.service;

import com.influencer.vo.KeywordVO;
import com.influencer.vo.timeVO;

import java.io.IOException;
import java.util.List;

public interface InfluencerService {
    public List<KeywordVO> getKeyword();

    public String crawling(int keywordSeq, String keyword, String formatedNow, boolean retryFlag, timeVO time) throws Exception;
    public List<String> channelCrawling(int keywordSeq, String keyword, String formatedNow, boolean retryFlag, timeVO time) throws Exception;

    public String parsingKeywordInfluencerInfo(int keywordSeq, String keyword, String filePath, String formatedNow, boolean retryFlag, timeVO time) throws Exception;
    public String parsingKeywordInfluencerDetail(int keywordSeq, String keyword, String filePath, String formatedNow, boolean retryFlag, timeVO time) throws Exception;
    public String parsingKeywordInfoStyle(int keywordSeq, String keyword, String filePath, String formatedNow, boolean retryFlag, timeVO time) throws Exception;
    public String parsingRecommandedInfluencer(int keywordSeq, String keyword, String filePath, String formatedNow, boolean retryFlag, timeVO time) throws Exception;
    public String parsingPopularKeyword(int keywordSeq, String keyword, String filePath, String formatedNow, boolean retryFlag, timeVO time) throws Exception;
    public String parsingInfluencerContent(int keywordSeq, String keyword, String filePath, String formatedNow, String channelType, boolean retryFlag, timeVO time) throws Exception;
    public String parsingInfluencerContentImage(int keywordSeq, String keyword, String filePath, String formatedNow, String channelType, boolean retryFlag, timeVO time) throws Exception;
    public String parsingRelatedContent(int keywordSeq, String keyword, String filePath, String formatedNow, String channelType, boolean retryFlag, timeVO time) throws Exception;

    public boolean insertKeywordInfo(int keywordSeq, String filePath, boolean retryFlag, timeVO time) throws Exception;
    public boolean insertKeywordInfoDetail(int keywordSeq, String filePath, boolean retryFlag, timeVO time) throws Exception;
    public boolean insertKeywordFashionStyle(int keywordSeq, String filePath, boolean retryFlag, timeVO time) throws Exception;
    public boolean insertRecommendedInfluencer(int keywordSeq, String filePath, boolean retryFlag, timeVO time) throws Exception;
    public boolean insertPopularKeyword(int keywordSeq, String filePath, boolean retryFlag, timeVO time) throws Exception;
    public boolean insertInfluencerContent(int keywordSeq, String filePath, String channelType, boolean retryFlag, timeVO time) throws Exception;
    public boolean insertInfluencerContentImage(int keywordSeq, String filePath, String channelType, boolean retryFlag, timeVO time) throws Exception;
    public boolean insertRelatedContent(int keywordSeq, String filePath, String channelType, boolean retryFlag, timeVO time) throws Exception;

    //재수집 관련 service
    public int confirmFirst(String date, String hh);
    public timeVO getTime();

    //집계수집여부(counting_target) 관련 service
    public void todayTarget0(String date);
    public void todayTarget1(String date);

    //알림톡 관련 service
    public void failAlimTalk(timeVO time, String datetime);
}
