package com.influencer.mapper;

import com.influencer.vo.*;
import org.apache.ibatis.annotations.Mapper;

import java.util.List;

@Mapper
public interface InfluencerMapper {
    public List<KeywordVO> getKeyword();

    public boolean insertKeywordInfo(String filePath);
    public boolean insertKeywordInfoDetail(String filePath);
    public boolean insertRecommendedInfluencer(String filePath);
    public boolean insertInfluencerContent(String filePath);
    public boolean insertInfluencerContentImage(String filePath);
    public boolean insertRelatedContent(String filePath);
    public boolean insertPopularKeyword(String filePath);
    public boolean insertKeywordFashionStyle(String filePath);

    public void insertLog(LogVO log);
    public void updateLog(LogVO log);
    public LogVO getLog(retryInfoVO retryInfo);

    //재수집 관련 mapper
    public int confirmFirst(String date, String hh);
    public timeVO getTime();

    //메일 관련 mapper
    public MailLogVO getMailLog(int seq);

    //log_seq 관련 mapper
    public int getSeq(String tableName);
    public void updateLogSeq(int logSeq, int minSeq, int maxSeq, String tableName);

    //집계수집여부(counting_target) 관련 mapper
    public void todayTarget0(String date);
    public void todayTarget1(String date);

    //알림톡 관련 mapper
    public List<Integer> failKeyword(timeVO time);
}
