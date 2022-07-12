package com.influencer.service;

import au.com.bytecode.opencsv.CSVWriter;
import com.influencer.HttpPostMultipart;
import com.influencer.mapper.InfluencerMapper;
import com.influencer.vo.*;
import com.influencer.vo.KeywordVO;
import lombok.Setter;
import org.json.JSONArray;
import org.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.mail.Message;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@Service
public class InfluencerServiceImpl implements InfluencerService {

    private final Logger logger = LoggerFactory.getLogger(InfluencerServiceImpl.class);

    //ip주소 가져오는 function
    public String getIp(){
        String result = null;
        try {
            result = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            result = "";
        }
        return result;
    }

    //오류 메일 보내는 function
    private void sendMail(MailLogVO mailLog) throws Exception {


        String SUBJECT = "[CURADAR] 네이버 인플루언서 수집 오류";

        String channelType = null;
        switch (mailLog.getChannelType()) {
            case "A":
                channelType = "전체 HTML";
                break;
            case "T":
                channelType = "전체채널";
                break;
            case "B":
                channelType = "블로그";
                break;
            case "P":
                channelType = "포스트";
                break;
            case "N":
                channelType = "NTV";
                break;
            case "Y":
                channelType = "유튜브";
                break;
        }

        String BODY = String.join(
            System.getProperty("line.separator"),
            "<p>[CURADAR]</p>",
            "<p>수집 중 오류가 발생했습니다.</p><br/>",
            "<p>키워드 : " + mailLog.getKeyword() + "</p>",
            "<p>실패 단계 : " + channelType + "의 "+ mailLog.getTaskType() + " - " + mailLog.getTaskStep() + "단계" + "</p>",
            "<p>재시도 횟수 : " + mailLog.getRetryCount() + "</p>",
            "<p>오류 메세지 : " + mailLog.getErrMsg() + "</p>",
            "<p>서버 IP : " + getIp() + "</p>"
        );

        Properties props = new Properties();
        props.put("mail.transport.protocol", "smtp");
        props.put("mail.smtp.ssl.trust","smtp.mailplug.co.kr");
        props.put("mail.smtp.port", PORT);
        props.put("mail.smtp.starttls.enable", "true");
        props.put("mail.smtp.ssl.enable", "true");
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.host", HOST);
        props.put("mail.smtp.socketFactory.fallback", "false");
        props.put("mail.smtp.debug", "true");

        Session session = Session.getDefaultInstance(props);

        MimeMessage msg = new MimeMessage(session);
        msg.setFrom(new InternetAddress(FROM, FROMNAME));
        msg.setRecipient(Message.RecipientType.TO, new InternetAddress(TO));
        msg.setSubject(SUBJECT);
        msg.setContent(BODY, "text/html;charset=UTF-8");

        Transport transport = session.getTransport();
        try {
            logger.info("Sending...");

            transport.connect(HOST, SMTP_USERNAME, SMTP_PASSWORD);
            transport.sendMessage(msg, msg.getAllRecipients());

            logger.info("Email sent!");
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            transport.close();
        }
    }

    //오류 알림톡 발송하는 function
    public void sendTalk(String datetime, List<KeywordVO> keywordList, List<String> failList) {
        try {
            HttpPostMultipart multipart = new HttpPostMultipart("https://api.artistchai.co.kr/alimtalk/regist", "utf-8");

            //오류내용 지정
            String content =
                    "플랫폼 : [CURADAR] 네이버 인플루언서\n" +
                    "오류내용 : \n" +
                    "수집 일시 : " + datetime + "시" + "\n" +
                    "대상 키워드 수 : " + keywordList.size() + "\n" +
                    "수집 실패 키워드 수 : " + failList.size() + "\n" +
                    "수집 실패 키워드 : " + failList.toString() + "\n" +
                    "자세한 내용은 메일을 확인해주세요.";

            //string data 객체 저장


            //send post
            multipart.finish();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Setter(onMethod_=@Autowired)
    private InfluencerMapper mapper;

    @Override
    public List<KeywordVO> getKeyword() {
        return mapper.getKeyword();
    }

    @Override
    public String crawling(int keywordSeq, String keyword, String formatedNow, boolean retryFlag, timeVO time) throws Exception {

        logger.info(keyword + " crawling 시작");

        LogVO retryLog = new LogVO();
        LogVO log = new LogVO();
        //재수집 and 이전 수집때의 status가 성공이라면
        if(retryFlag) {
            // channelType = 'A' 수집의 성공 여부 체크
            // taskStatus = 1 일때는 return "PASS"
            retryInfoVO retryInfo = new retryInfoVO();
            retryInfo.setKeywordSeq(keywordSeq);
            retryInfo.setChannelType("A");
            retryInfo.setTaskType(1);
            retryInfo.setTaskStep(0);
            retryInfo.setDate(time.getDate());
            retryInfo.setHh(time.getHh());

            //row가 있고 성공해서 continue 되는 경우
            if(mapper.getLog(retryInfo) != null) {
                retryLog = mapper.getLog(retryInfo);

                if(retryLog.getTaskStatus() == 1) {
                    return "D:/influencerHtml/" + retryLog.getTaskFileName();
                }
            }
        }

        //logVO 생성
        log.setKeywordSeq(keywordSeq);
        log.setChannelType("A");
        log.setTaskType(1);
        log.setTaskStep(0);
        log.setTaskName("CRAWLING_HTML_ALL");
        mapper.insertLog(log);

        String urlStr = "https://search.naver.com/search.naver?where=influencer&sm=tab_jum&query=%23"+keyword;

        try {
            URL url = new URL(urlStr);

            String file = "D:/influencerHtml/" + formatedNow + "_" + keywordSeq + ".html";

            InputStream is = url.openStream();
            BufferedReader br = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
            OutputStreamWriter fw = new OutputStreamWriter(new FileOutputStream(file));

            String line;
            while ((line = br.readLine()) != null) {
                fw.write(line + "\r\n");
                fw.flush();
            }

            br.close();
            is.close();
            fw.close();

            logger.info("crawling 끝");

            //log 성공 update
            if(retryFlag) {
                log.setRetryCount(retryLog.getRetryCount() + 1);
            }
            log.setTaskStatus(1);
            log.setStatus(1);
            log.setTaskFileName(formatedNow + "_" + keywordSeq + ".html");
            mapper.updateLog(log);

            return file;
        } catch (Exception e) {
            //log 실패 update
            if(retryFlag) {
                if (retryLog.getDate() != null) {
                    log.setRetryCount(retryLog.getRetryCount() + 1);

                    //3번 재시도 실패일 시 mailCount + 1
                    if(log.getRetryCount() == 3) {
                        log.setMailCount(1);
                    } else if(log.getRetryCount() > 3) {
                        log.setMailCount(retryLog.getMailCount());
                    }
                }
            }
            log.setTaskStatus(9);
            log.setStatus(9);
            log.setTaskMessage(e.getMessage());
            mapper.updateLog(log);

            //3번 재시도 실패일 시 메일 발송
            if(log.getRetryCount() == 3) {
                MailLogVO mailLog = mapper.getMailLog(log.getSeq());
                mailLog.setErrMsg(e.getMessage());
                sendMail(mailLog);
            }

            throw new RuntimeException(e);
        }
    }

    //json html crawling
    @Override
    public List<String> channelCrawling(int keywordSeq, String keyword, String formatedNow, boolean retryFlag, timeVO time) throws Exception {

        logger.info(keyword + " channel crawling 시작");

        ArrayList<String> channelFilePathList = new ArrayList<>();

        int[] channelNumList = {0, 1, 2, 3, 6};
        for (int channelNum : channelNumList) {

            String channelType;
            String taskName;
            if(channelNum == 0) {
                channelType = "T";
                taskName = "CRAWLING_HTML_CHANNEL_ALL";
            } else if(channelNum == 1) {
                channelType = "B";
                taskName = "CRAWLING_HTML_CHANNEL_BLOG";
            } else if(channelNum == 2) {
                channelType = "P";
                taskName = "CRAWLING_HTML_CHANNEL_POST";
            } else if(channelNum == 3) {
                channelType = "N";
                taskName = "CRAWLING_HTML_CHANNEL_NTV";
            } else {
                channelType = "Y";
                taskName = "CRAWLING_HTML_CHANNEL_YOUTUBE";
            }

            LogVO retryLog = new LogVO();
            LogVO log = new LogVO();

            //재수집인 경우
            if(retryFlag) {
                // channelType별 수집의 성공 여부 체크
                // taskStatus = 1 일때는 성공했던 fileName 가져와 add list
                retryInfoVO retryInfo = new retryInfoVO();
                retryInfo.setKeywordSeq(keywordSeq);
                retryInfo.setChannelType(channelType);
                retryInfo.setTaskType(1);
                retryInfo.setTaskStep(0);
                retryInfo.setDate(time.getDate());
                retryInfo.setHh(time.getHh());

                //row가 있고 성공해서 continue 되는 경우
                if(mapper.getLog(retryInfo) != null) {
                    retryLog = mapper.getLog(retryInfo);

                    if(retryLog.getTaskStatus() == 1) {
                        String retryFilePath = "D:/influencerHtml/" + retryLog.getTaskFileName();
                        channelFilePathList.add(retryFilePath);
                        continue;
                    }
                }
            }

            //logVO 생성
            log.setKeywordSeq(keywordSeq);
            log.setChannelType(channelType);
            log.setTaskType(1);
            log.setTaskStep(0);
            log.setTaskName(taskName);
            mapper.insertLog(log);

            String urlStr = "https://s.search.naver.com/p/influencer/api/v1/collection?query=%23"+keyword;

            if(channelNum == 0) {
                urlStr += "&display=50&where=influencer_api";
            } else {
                urlStr += "&display=50&where=influencer_api&channel=" + channelNum;
            }

            try {
                URL url = new URL(urlStr);

                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET"); // http 메서드
                conn.setRequestProperty("Content-Type", " application/javascript"); // header Content-Type 정보
                conn.setRequestProperty("auth", "myAuth"); // header의 auth 정보
                conn.setDoOutput(true);

                BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                StringBuilder sb = new StringBuilder();
                String line;

                while ((line = br.readLine()) != null) {
                    sb.append(line);
                }

                String result = sb.toString();
                result = result.replaceAll("\\\\t", "");
                result = result.replaceAll("\\\\n", "");
                result = result.replaceAll("\\\\\"","'");

                JSONObject obj = new JSONObject(result);
                JSONArray itemList_arr = obj.getJSONObject("result").getJSONObject("docs").getJSONArray("itemList");


                String filePath = "D:/influencerHtml/" + formatedNow + "_" + keywordSeq + channelType + ".html";

                BufferedWriter bw;
                String newLine = System.lineSeparator();

                try {
                    //File file = new File(filePath);
                    bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filePath), StandardCharsets.UTF_8));

                    for (int i = 0; i < itemList_arr.length(); i++) {
                        bw.write(itemList_arr.getJSONObject(i).getString("html"));
                        bw.write(newLine);
                        bw.flush();
                    }
                } catch(Exception e){
                    //log 실패 update
                    if(retryFlag) {
                        if (retryLog.getDate() != null) {
                            log.setRetryCount(retryLog.getRetryCount() + 1);

                            //3번 재시도 실패일 시 mailCount + 1
                            if(log.getRetryCount() == 3) {
                                log.setMailCount(1);
                            } else if(log.getRetryCount() > 3) {
                                log.setMailCount(retryLog.getMailCount());
                            }
                        }
                    }
                    log.setTaskStatus(9);
                    log.setStatus(9);
                    log.setTaskMessage(e.getMessage());
                    mapper.updateLog(log);

                    //3번 재시도 실패일 시 메일 발송
                    if(log.getRetryCount() == 3) {
                        MailLogVO mailLog = mapper.getMailLog(log.getSeq());
                        mailLog.setErrMsg(e.getMessage());
                        sendMail(mailLog);
                    }

                    throw new RuntimeException(e);
                }

                //log 성공 update
                if(retryFlag) {
                    if (retryLog.getDate() != null) {
                        log.setRetryCount(retryLog.getRetryCount() + 1);
                    }
                }
                log.setTaskStatus(1);
                log.setStatus(1);
                log.setTaskFileName(formatedNow + "_" + keywordSeq + channelType + ".html");
                mapper.updateLog(log);

                channelFilePathList.add(filePath);

            } catch(Exception e){
                //log 실패 update
                if(retryFlag) {
                    if (retryLog.getDate() != null) {
                        log.setRetryCount(retryLog.getRetryCount() + 1);

                        //3번 재시도 실패일 시 mailCount + 1
                        if(log.getRetryCount() == 3) {
                            log.setMailCount(1);
                        } else if(log.getRetryCount() > 3) {
                            log.setMailCount(retryLog.getMailCount());
                        }
                    }
                }
                log.setTaskStatus(9);
                log.setStatus(9);
                log.setTaskMessage(e.getMessage());
                mapper.updateLog(log);

                //3번 재시도 실패일 시 메일 발송
                if(log.getRetryCount() == 3) {
                    MailLogVO mailLog = mapper.getMailLog(log.getSeq());
                    mailLog.setErrMsg(e.getMessage());
                    sendMail(mailLog);
                }

                throw new RuntimeException(e);
            }
        }
        logger.info("channel crawling 끝");
        return channelFilePathList;
    }

    @Override
    public String parsingKeywordInfluencerInfo(int keywordSeq, String keyword, String filePath, String formatedNow, boolean retryFlag, timeVO time) throws Exception {
        logger.info(keyword + " keywordInfluencerInfo parsing 시작");

        LogVO retryLog = new LogVO();
        LogVO log = new LogVO();

        //재수집 and 이전 수집때의 status가 성공이라면
        if(retryFlag) {
            // channelType별 수집의 성공 여부 체크
            // taskStatus = 1 일때는 성공했던 return "PASS"
            retryInfoVO retryInfo = new retryInfoVO();
            retryInfo.setKeywordSeq(keywordSeq);
            retryInfo.setChannelType("A");
            retryInfo.setTaskType(2);
            retryInfo.setTaskStep(1);
            retryInfo.setDate(time.getDate());
            retryInfo.setHh(time.getHh());

            //row가 있고 성공해서 continue 되는 경우
            if(mapper.getLog(retryInfo) != null) {
                retryLog = mapper.getLog(retryInfo);

                if(retryLog.getTaskStatus() == 1) {
                    return "D:/uploadInfluencerCsv/" + retryLog.getTaskFileName();
                }
            }
        }

        //logVO 생성
        log.setKeywordSeq(keywordSeq);
        log.setChannelType("A");
        log.setTaskType(2);
        log.setTaskStep(1);
        log.setTaskName("PARSING_CSV_ALL_STEP1");
        mapper.insertLog(log);

        KeywordInfoVO keywordInfo = new KeywordInfoVO();

        try {
            File file = new File(filePath);
            Document doc = Jsoup.parse(file, "UTF-8");

            //totalCount
            StringBuilder num = new StringBuilder();
            Elements counts = doc.getElementsByClass("num_bx _creator_counts");
            for (Element count : counts) {
                Elements counts2 = count.getElementsByClass("num_inner _num");
                for (Element count2 : counts2) {
                    String count3 = count2.getElementsByClass("num").text();
                    num.append(count3);
                }
            }

            //type
            String type = doc.getElementsByClass("_count_title").text();
            type = type.substring(0, type.length() - 16);

            keywordInfo.setKeywordSeq(keywordSeq);
            keywordInfo.setTotalCount(Integer.parseInt(num.toString()));
            keywordInfo.setType(type);
        } catch (Exception e) {
            //log 실패 update
            if(retryFlag) {
                if (retryLog.getDate() != null) {
                    log.setRetryCount(retryLog.getRetryCount() + 1);

                    //3번 재시도 실패일 시 mailCount + 1
                    if(log.getRetryCount() == 3) {
                        log.setMailCount(1);
                    } else if(log.getRetryCount() > 3) {
                        log.setMailCount(retryLog.getMailCount());
                    }
                }
            }
            log.setTaskStatus(9);
            log.setStatus(9);
            log.setTaskMessage(e.getMessage());
            mapper.updateLog(log);

            //3번 재시도 실패일 시 메일 발송
            if(log.getRetryCount() == 3) {
                MailLogVO mailLog = mapper.getMailLog(log.getSeq());
                mailLog.setErrMsg(e.getMessage());
                sendMail(mailLog);
            }

            throw new RuntimeException(e);
        }

        //csv 파일 변환 --------------------------------------------------------------------
        String csvFilePath = "D:/uploadInfluencerCsv/"+formatedNow+"_"+keywordSeq+"_keyword_influencer_info.csv";

        try {
            CSVWriter cw = new CSVWriter(new OutputStreamWriter(new FileOutputStream(csvFilePath), StandardCharsets.UTF_8),',', CSVWriter.NO_QUOTE_CHARACTER);
            try (cw) {
                cw.writeNext(new String[]{"\ufeff"});
                cw.writeNext(new String[]{"keyword_seq", "type", "total_count"});
                cw.writeNext(new String[]{String.valueOf(keywordInfo.getKeywordSeq()), keywordInfo.getType(), String.valueOf(keywordInfo.getTotalCount())});
            } catch (Exception e) {
                //log 실패 update
                if(retryFlag) {
                    if (retryLog.getDate() != null) {
                        log.setRetryCount(retryLog.getRetryCount() + 1);

                        //3번 재시도 실패일 시 mailCount + 1
                        if(log.getRetryCount() == 3) {
                            log.setMailCount(1);
                        } else if(log.getRetryCount() > 3) {
                            log.setMailCount(retryLog.getMailCount());
                        }
                    }
                }
                log.setTaskStatus(9);
                log.setStatus(9);
                log.setTaskMessage(e.getMessage());
                mapper.updateLog(log);

                //3번 재시도 실패일 시 메일 발송
                if(log.getRetryCount() == 3) {
                    MailLogVO mailLog = mapper.getMailLog(log.getSeq());
                    mailLog.setErrMsg(e.getMessage());
                    sendMail(mailLog);
                }

                throw new RuntimeException(e);
            }
        } catch (Exception e) {
            //log 실패 update
            if(retryFlag) {
                if (retryLog.getDate() != null) {
                    log.setRetryCount(retryLog.getRetryCount() + 1);

                    //3번 재시도 실패일 시 mailCount + 1
                    if(log.getRetryCount() == 3) {
                        log.setMailCount(1);
                    } else if(log.getRetryCount() > 3) {
                        log.setMailCount(retryLog.getMailCount());
                    }
                }
            }
            log.setTaskStatus(9);
            log.setStatus(9);
            log.setTaskMessage(e.getMessage());
            mapper.updateLog(log);

            //3번 재시도 실패일 시 메일 발송
            if(log.getRetryCount() == 3) {
                MailLogVO mailLog = mapper.getMailLog(log.getSeq());
                mailLog.setErrMsg(e.getMessage());
                sendMail(mailLog);
            }

            throw new RuntimeException(e);
        }
        //log 성공 update
        if(retryFlag) {
            if(retryLog.getDate() != null) {
                log.setRetryCount(retryLog.getRetryCount() + 1);
            }
        }
        log.setTaskStatus(1);
        log.setStatus(1);
        log.setTaskFileName(formatedNow+"_"+keywordSeq+"_keyword_influencer_info.csv");
        mapper.updateLog(log);

        logger.info("parsing 끝");
        return csvFilePath;
    }

    //---------------------------------------------------------------------------------------------------------------------
    @Override
    public String parsingKeywordInfluencerDetail(int keywordSeq, String keyword, String filePath, String formatedNow, boolean retryFlag, timeVO time) throws Exception {
        logger.info(keyword + " keywordInfluencerDetail parsing 시작");

        LogVO retryLog = new LogVO();
        LogVO log = new LogVO();

        //재수집 and 이전 수집때의 status가 성공이라면
        if(retryFlag) {
            // channelType별 수집의 성공 여부 체크
            // taskStatus = 1 일때는 성공했던 return "PASS"
            retryInfoVO retryInfo = new retryInfoVO();
            retryInfo.setKeywordSeq(keywordSeq);
            retryInfo.setChannelType("A");
            retryInfo.setTaskType(2);
            retryInfo.setTaskStep(2);
            retryInfo.setDate(time.getDate());
            retryInfo.setHh(time.getHh());

            //row가 있고 성공해서 continue 되는 경우
            if(mapper.getLog(retryInfo) != null) {
                retryLog = mapper.getLog(retryInfo);

                if(retryLog.getTaskStatus() == 1) {
                    return "D:/uploadInfluencerCsv/" + retryLog.getTaskFileName();
                }
            }
        }

        //logVO 생성
        log.setKeywordSeq(keywordSeq);
        log.setChannelType("A");
        log.setTaskType(2);
        log.setTaskStep(2);
        log.setTaskName("PARSING_CSV_ALL_STEP2");
        mapper.insertLog(log);

        List<KeywordInfoDetailVO> keywordInfoDetailList = new ArrayList<>();

        try {
            File file = new File(filePath);
            Document doc = Jsoup.parse(file, "UTF-8");

            Elements infoDetails = doc.getElementsByClass("api_flicking_wrap my_filter _major_subjects _scroll_target");
            for (Element infoDetail : infoDetails) {
                Elements infoDetails2 = infoDetail.getElementsByClass("flick_bx");
                for (Element infoDetail2 : infoDetails2) {
                    KeywordInfoDetailVO keywordInfoDetail = new KeywordInfoDetailVO();

                    //type
                    keywordInfoDetail.setType(infoDetail2.getElementsByClass("name _name_target").text());
                    //count
                    String detailCount = infoDetail2.getElementsByClass("num").text();
                    detailCount = detailCount.substring(0, detailCount.length() - 1);
                    keywordInfoDetail.setCount(Integer.parseInt(detailCount));
                    //keywordSeq
                    keywordInfoDetail.setKeywordSeq(keywordSeq);

                    keywordInfoDetailList.add(keywordInfoDetail);
                }
            }
        } catch (Exception e) {
            //log 실패 update
            if(retryFlag) {
                if (retryLog.getDate() != null) {
                    log.setRetryCount(retryLog.getRetryCount() + 1);

                    //3번 재시도 실패일 시 mailCount + 1
                    if(log.getRetryCount() == 3) {
                        log.setMailCount(1);
                    } else if(log.getRetryCount() > 3) {
                        log.setMailCount(retryLog.getMailCount());
                    }
                }
            }
            log.setTaskStatus(9);
            log.setStatus(9);
            log.setTaskMessage(e.getMessage());
            mapper.updateLog(log);

            //3번 재시도 실패일 시 메일 발송
            if(log.getRetryCount() == 3) {
                MailLogVO mailLog = mapper.getMailLog(log.getSeq());
                mailLog.setErrMsg(e.getMessage());
                sendMail(mailLog);
            }

            throw new RuntimeException(e);
        }

        //csv 파일 변환 --------------------------------------------------------------------
        String csvFilePath = "D:/uploadInfluencerCsv/"+formatedNow+"_"+keywordSeq+"_keyword_influencer_detail.csv";

        try {
            CSVWriter cw = new CSVWriter(new OutputStreamWriter(new FileOutputStream(csvFilePath), StandardCharsets.UTF_8),',', CSVWriter.NO_QUOTE_CHARACTER);
            try (cw) {
                cw.writeNext(new String[]{"\ufeff"});
                cw.writeNext(new String[]{"keyword_seq", "type", "count"});
                for (KeywordInfoDetailVO keywordInfoDetail : keywordInfoDetailList) {
                    cw.writeNext(new String[]{String.valueOf(keywordInfoDetail.getKeywordSeq()), keywordInfoDetail.getType(), String.valueOf(keywordInfoDetail.getCount())});
                }
            } catch (Exception e) {
                //log 실패 update
                if(retryFlag) {
                    if (retryLog.getDate() != null) {
                        log.setRetryCount(retryLog.getRetryCount() + 1);

                        //3번 재시도 실패일 시 mailCount + 1
                        if(log.getRetryCount() == 3) {
                            log.setMailCount(1);
                        } else if(log.getRetryCount() > 3) {
                            log.setMailCount(retryLog.getMailCount());
                        }
                    }
                }
                log.setTaskStatus(9);
                log.setStatus(9);
                log.setTaskMessage(e.getMessage());
                mapper.updateLog(log);

                //3번 재시도 실패일 시 메일 발송
                if(log.getRetryCount() == 3) {
                    MailLogVO mailLog = mapper.getMailLog(log.getSeq());
                    mailLog.setErrMsg(e.getMessage());
                    sendMail(mailLog);
                }

                throw new RuntimeException(e);
            }
        } catch (Exception e) {
            //log 실패 update
            if(retryFlag) {
                if (retryLog.getDate() != null) {
                    log.setRetryCount(retryLog.getRetryCount() + 1);

                    //3번 재시도 실패일 시 mailCount + 1
                    if(log.getRetryCount() == 3) {
                        log.setMailCount(1);
                    } else if(log.getRetryCount() > 3) {
                        log.setMailCount(retryLog.getMailCount());
                    }
                }
            }
            log.setTaskStatus(9);
            log.setStatus(9);
            log.setTaskMessage(e.getMessage());
            mapper.updateLog(log);

            //3번 재시도 실패일 시 메일 발송
            if(log.getRetryCount() == 3) {
                MailLogVO mailLog = mapper.getMailLog(log.getSeq());
                mailLog.setErrMsg(e.getMessage());
                sendMail(mailLog);
            }

            throw new RuntimeException(e);
        }

        //log 성공 update
        if(retryFlag) {
            if(retryLog.getDate() != null) {
                log.setRetryCount(retryLog.getRetryCount() + 1);
            }
        }
        log.setTaskStatus(1);
        log.setStatus(1);
        log.setTaskFileName(formatedNow+"_"+keywordSeq+"_keyword_influencer_detail.csv");
        mapper.updateLog(log);

        logger.info("parsing 끝");
        return csvFilePath;
    }

    @Override
    public String parsingKeywordInfoStyle(int keywordSeq, String keyword, String filePath, String formatedNow, boolean retryFlag, timeVO time) throws Exception {
        logger.info(keyword + " keywordInfoStyle parsing 시작");

        LogVO retryLog = new LogVO();
        LogVO log = new LogVO();

        //재수집 and 이전 수집때의 status가 성공이라면
        if(retryFlag) {
            // channelType별 수집의 성공 여부 체크
            // taskStatus = 1 일때는 성공했던 return "PASS"
            retryInfoVO retryInfo = new retryInfoVO();
            retryInfo.setKeywordSeq(keywordSeq);
            retryInfo.setChannelType("A");
            retryInfo.setTaskType(2);
            retryInfo.setTaskStep(3);
            retryInfo.setDate(time.getDate());
            retryInfo.setHh(time.getHh());

            //row가 있고 성공해서 continue 되는 경우
            if(mapper.getLog(retryInfo) != null) {
                retryLog = mapper.getLog(retryInfo);

                if(retryLog.getTaskStatus() == 1) {
                    return "D:/uploadInfluencerCsv/" + retryLog.getTaskFileName();
                }
            }
        }

        //logVO 생성
        log.setKeywordSeq(keywordSeq);
        log.setChannelType("A");
        log.setTaskType(2);
        log.setTaskStep(3);
        log.setTaskName("PARSING_CSV_ALL_STEP3");
        mapper.insertLog(log);

        List<KeywordInfoStyleVO> keywordInfoStyleList = new ArrayList<>();

        try {
            File file = new File(filePath);
            Document doc = Jsoup.parse(file, "UTF-8");

            Elements styleTitle = doc.getElementsByClass("tab_title_area");
            String infoTile = styleTitle.get(0).getElementsByClass("tit").text();

            Elements styleTypes = doc.getElementsByClass("api_flicking_wrap primary_tablist _list _scroll_target");
            for (Element styleType : styleTypes) {
                Elements styleTypes2 = styleType.getElementsByClass("flick_bx");
                for (Element styleType2 : styleTypes2) {
                    KeywordInfoStyleVO keywordStyle = new KeywordInfoStyleVO();
                    keywordStyle.setKeywordSeq(keywordSeq);

                    //keywordInfoType
                    keywordStyle.setKeywordInfoType(styleType2.getElementsByClass("btn").text());
                    //keywordInfoTitle
                    keywordStyle.setKeywordInfoTitle(infoTile);

                    keywordInfoStyleList.add(keywordStyle);
                }
            }
        } catch (Exception e) {
            //log 실패 update
            if(retryFlag) {
                if (retryLog.getDate() != null) {
                    log.setRetryCount(retryLog.getRetryCount() + 1);

                    //3번 재시도 실패일 시 mailCount + 1
                    if(log.getRetryCount() == 3) {
                        log.setMailCount(1);
                    } else if(log.getRetryCount() > 3) {
                        log.setMailCount(retryLog.getMailCount());
                    }
                }
            }
            log.setTaskStatus(9);
            log.setStatus(9);
            log.setTaskMessage(e.getMessage());
            mapper.updateLog(log);

            //3번 재시도 실패일 시 메일 발송
            if(log.getRetryCount() == 3) {
                MailLogVO mailLog = mapper.getMailLog(log.getSeq());
                mailLog.setErrMsg(e.getMessage());
                sendMail(mailLog);
            }

            throw new RuntimeException(e);
        }

        //csv 파일 변환 --------------------------------------------------------------------
        String csvFilePath = "D:/uploadInfluencerCsv/"+formatedNow+"_"+keywordSeq+"_keyword_info_style.csv";

        try {
            CSVWriter cw = new CSVWriter(new OutputStreamWriter(new FileOutputStream(csvFilePath), StandardCharsets.UTF_8),',', CSVWriter.NO_QUOTE_CHARACTER);
            try (cw) {
                cw.writeNext(new String[]{"\ufeff"});
                cw.writeNext(new String[]{"keyword_seq", "keyword_info_title", "keyword_info_type"});
                for (KeywordInfoStyleVO KeywordInfoStyle : keywordInfoStyleList) {
                    cw.writeNext(new String[]{String.valueOf(KeywordInfoStyle.getKeywordSeq()), KeywordInfoStyle.getKeywordInfoTitle(), String.valueOf(KeywordInfoStyle.getKeywordInfoType())});
                }
            } catch (Exception e) {
                //log 실패 update
                if(retryFlag) {
                    if (retryLog.getDate() != null) {
                        log.setRetryCount(retryLog.getRetryCount() + 1);

                        //3번 재시도 실패일 시 mailCount + 1
                        if(log.getRetryCount() == 3) {
                            log.setMailCount(1);
                        } else if(log.getRetryCount() > 3) {
                            log.setMailCount(retryLog.getMailCount());
                        }
                    }
                }
                log.setTaskStatus(9);
                log.setStatus(9);
                log.setTaskMessage(e.getMessage());
                mapper.updateLog(log);

                //3번 재시도 실패일 시 메일 발송
                if(log.getRetryCount() == 3) {
                    MailLogVO mailLog = mapper.getMailLog(log.getSeq());
                    mailLog.setErrMsg(e.getMessage());
                    sendMail(mailLog);
                }

                throw new RuntimeException(e);
            }
        } catch (Exception e) {
            //log 실패 update
            if(retryFlag) {
                if (retryLog.getDate() != null) {
                    log.setRetryCount(retryLog.getRetryCount() + 1);

                    //3번 재시도 실패일 시 mailCount + 1
                    if(log.getRetryCount() == 3) {
                        log.setMailCount(1);
                    } else if(log.getRetryCount() > 3) {
                        log.setMailCount(retryLog.getMailCount());
                    }
                }
            }
            log.setTaskStatus(9);
            log.setStatus(9);
            log.setTaskMessage(e.getMessage());
            mapper.updateLog(log);

            //3번 재시도 실패일 시 메일 발송
            if(log.getRetryCount() == 3) {
                MailLogVO mailLog = mapper.getMailLog(log.getSeq());
                mailLog.setErrMsg(e.getMessage());
                sendMail(mailLog);
            }

            throw new RuntimeException(e);
        }

        //log 성공 update
        if(retryFlag) {
            if(retryLog.getDate() != null) {
                log.setRetryCount(retryLog.getRetryCount() + 1);
            }
        }
        log.setTaskStatus(1);
        log.setStatus(1);
        log.setTaskFileName(formatedNow+"_"+keywordSeq+"_keyword_info_style.csv");
        mapper.updateLog(log);

        logger.info("parsing 끝");
        return csvFilePath;
    }

    @Override
    public String parsingRecommandedInfluencer(int keywordSeq, String keyword, String filePath, String formatedNow, boolean retryFlag, timeVO time) throws Exception {
        logger.info(keyword + " recommandedInfluencer parsing 시작");

        LogVO retryLog = new LogVO();
        LogVO log = new LogVO();

        //재수집 and 이전 수집때의 status가 성공이라면
        if(retryFlag) {
            // channelType별 수집의 성공 여부 체크
            // taskStatus = 1 일때는 성공했던 return "PASS"
            retryInfoVO retryInfo = new retryInfoVO();
            retryInfo.setKeywordSeq(keywordSeq);
            retryInfo.setChannelType("A");
            retryInfo.setTaskType(2);
            retryInfo.setTaskStep(4);
            retryInfo.setDate(time.getDate());
            retryInfo.setHh(time.getHh());

            //row가 있고 성공해서 continue 되는 경우
            if(mapper.getLog(retryInfo) != null) {
                retryLog = mapper.getLog(retryInfo);

                if(retryLog.getTaskStatus() == 1) {
                    return "D:/uploadInfluencerCsv/" + retryLog.getTaskFileName();
                }
            }
        }

        //logVO 생성
        log.setKeywordSeq(keywordSeq);
        log.setChannelType("A");
        log.setTaskType(2);
        log.setTaskStep(4);
        log.setTaskName("PARSING_CSV_ALL_STEP4");
        mapper.insertLog(log);

        List<RecommendedInfluencerVO> recommendedInfluencerList = new ArrayList<>();

        try {
            File file = new File(filePath);
            Document doc = Jsoup.parse(file, "UTF-8");

            Elements rInfluencerImgs = doc.getElementsByClass("api_flicking_wrap creator_flick_wrap _profile_list");
            for (Element rInfluencerImg : rInfluencerImgs) {
                //추천 인플루언서 이미지
                Elements rInfluencerImgs2 = rInfluencerImg.getElementsByClass("flick_bx");
                //추천 인플루언서 게시글
                Elements rInfluencerContents = doc.getElementsByClass("creator_info");

                //추천 인플루언서 수 구하기
                int recInfluSize = rInfluencerImgs2.size();

                for (int i=0; i<recInfluSize; i++) {
                    RecommendedInfluencerVO recInfluencer = new RecommendedInfluencerVO();
                    recInfluencer.setKeywordSeq(keywordSeq);

                    //influencerImg
                    recInfluencer.setInfluencerImg(rInfluencerImgs2.get(i).select("img").attr("src"));

                    //influencerContent
                    Element rInfluencerContent = rInfluencerContents.get(i);

                    //userInfo
                    Elements userBoxes = rInfluencerContent.getElementsByClass("user_box");
                    for (Element userBox : userBoxes) {
                        //influencerName
                        recInfluencer.setInfluencerName(userBox.getElementsByClass("txt").text());
                        //influencerType
                        recInfluencer.setInfluencerType(userBox.getElementsByClass("etc highlight").text());

                        if(userBox.getElementsByClass("etc").size() == 3) {
                            //influencerDetail
                            recInfluencer.setInfluencerTypeDetail(userBox.getElementsByClass("etc").get(1).text());
                            //fanCount
                            recInfluencer.setFanCount(userBox.getElementsByClass("_fan_count").text());
                        } else if(userBox.getElementsByClass("etc").size() == 2) {
                            //fanCount
                            recInfluencer.setFanCount(userBox.getElementsByClass("_fan_count").text());
                        }
                    }

                    //contentInfo
                    Elements detailBoxes = rInfluencerContent.getElementsByClass("detail_box");
                    for (Element detailBox : detailBoxes) {
                        //contentTitle
                        recInfluencer.setContentTitle(detailBox.getElementsByClass("name_link").text());
                        //contentDesc
                        recInfluencer.setContentDesc(detailBox.getElementsByClass("dsc").text());
                        //contentImg
                        recInfluencer.setContentImg(detailBox.select("img").attr("data-lazysrc"));
                        //contentLink
                        String contentLink = detailBox.getElementsByClass("name_link").attr("href");
                        recInfluencer.setContentLink(contentLink);

                        //contentNum
                        String[] strArr = contentLink.split("/");
                        String[] strArr2 = strArr[6].split("\\?");
                        String contentNum = strArr2[0];
                        recInfluencer.setContentNum(contentNum);

                        //contentPostDate
                        String dateStr = detailBox.getElementsByClass("date").text();
                        LocalDateTime nowDate = LocalDateTime.now();

                        if(dateStr.length() > 6) {
                            //일반 날짜일 때
                            recInfluencer.setContentPostDate(dateStr);
                        } else if(dateStr.length() > 2){
                            if(dateStr.endsWith("시간 전")) {
                                //~시간 전일 때
                                int beforeHours = Integer.parseInt(dateStr.substring(0, dateStr.length()-4));
                                nowDate = nowDate.minusHours(beforeHours);
                            } else if(dateStr.endsWith("일 전")) {
                                //~일 전일 때
                                int beforeDate = Integer.parseInt(dateStr.substring(0, dateStr.length()-3));
                                nowDate = nowDate.minusDays(beforeDate);
                            }
                            recInfluencer.setContentPostDate(String.valueOf(nowDate));
                        } else {
                            //'어제'일 때
                            nowDate = nowDate.minusDays(1);
                            recInfluencer.setContentPostDate(String.valueOf(nowDate));
                        }
                    }
                    recommendedInfluencerList.add(recInfluencer);
                }
            }
        } catch (Exception e) {
            //log 실패 update
            if(retryFlag) {
                if (retryLog.getDate() != null) {
                    log.setRetryCount(retryLog.getRetryCount() + 1);

                    //3번 재시도 실패일 시 mailCount + 1
                    if(log.getRetryCount() == 3) {
                        log.setMailCount(1);
                    } else if(log.getRetryCount() > 3) {
                        log.setMailCount(retryLog.getMailCount());
                    }
                }
            }
            log.setTaskStatus(9);
            log.setStatus(9);
            log.setTaskMessage(e.getMessage());
            mapper.updateLog(log);

            //3번 재시도 실패일 시 메일 발송
            if(log.getRetryCount() == 3) {
                MailLogVO mailLog = mapper.getMailLog(log.getSeq());
                mailLog.setErrMsg(e.getMessage());
                sendMail(mailLog);
            }

            throw new RuntimeException(e);
        }

        //csv 파일 변환 --------------------------------------------------------------------
        String csvFilePath = "D:/uploadInfluencerCsv/"+formatedNow+"_"+keywordSeq+"_recommanded_influencer.csv";

        try {
            CSVWriter cw = new CSVWriter(new OutputStreamWriter(new FileOutputStream(csvFilePath), StandardCharsets.UTF_8),',', CSVWriter.NO_QUOTE_CHARACTER);
            try (cw) {
                cw.writeNext(new String[]{"\ufeff"});
                cw.writeNext(new String[]{"keyword_seq", "content_num", "influencer_img", "influencer_name", "influencer_type", "influencer_type_detail", "fan_count",
                        "content_title", "content_desc", "content_img", "content_link", "content_post_date"});
                for (RecommendedInfluencerVO recommendedInfluencer : recommendedInfluencerList) {
                    recommendedInfluencer.setFanCount(recommendedInfluencer.getFanCount().replace(",", ""));
                    recommendedInfluencer.setContentTitle(recommendedInfluencer.getContentTitle().replace(",", " "));
                    recommendedInfluencer.setContentDesc(recommendedInfluencer.getContentDesc().replace(",", " "));

                    cw.writeNext(new String[]{String.valueOf(recommendedInfluencer.getKeywordSeq()), recommendedInfluencer.getContentNum(), recommendedInfluencer.getInfluencerImg(), recommendedInfluencer.getInfluencerName(),
                            recommendedInfluencer.getInfluencerType(), recommendedInfluencer.getInfluencerTypeDetail(), recommendedInfluencer.getFanCount(), recommendedInfluencer.getContentTitle(),
                            recommendedInfluencer.getContentDesc(), recommendedInfluencer.getContentImg(), recommendedInfluencer.getContentLink(), recommendedInfluencer.getContentPostDate()});
                }
            } catch (Exception e) {
                //log 실패 update
                if(retryFlag) {
                    if (retryLog.getDate() != null) {
                        log.setRetryCount(retryLog.getRetryCount() + 1);

                        //3번 재시도 실패일 시 mailCount + 1
                        if(log.getRetryCount() == 3) {
                            log.setMailCount(1);
                        } else if(log.getRetryCount() > 3) {
                            log.setMailCount(retryLog.getMailCount());
                        }
                    }
                }
                log.setTaskStatus(9);
                log.setStatus(9);
                log.setTaskMessage(e.getMessage());
                mapper.updateLog(log);

                //3번 재시도 실패일 시 메일 발송
                if(log.getRetryCount() == 3) {
                    MailLogVO mailLog = mapper.getMailLog(log.getSeq());
                    mailLog.setErrMsg(e.getMessage());
                    sendMail(mailLog);
                }

                throw new RuntimeException(e);
            }
        } catch (Exception e) {
            //log 실패 update
            if(retryFlag) {
                if (retryLog.getDate() != null) {
                    log.setRetryCount(retryLog.getRetryCount() + 1);

                    //3번 재시도 실패일 시 mailCount + 1
                    if(log.getRetryCount() == 3) {
                        log.setMailCount(1);
                    } else if(log.getRetryCount() > 3) {
                        log.setMailCount(retryLog.getMailCount());
                    }
                }
            }
            log.setTaskStatus(9);
            log.setStatus(9);
            log.setTaskMessage(e.getMessage());
            mapper.updateLog(log);

            //3번 재시도 실패일 시 메일 발송
            if(log.getRetryCount() == 3) {
                MailLogVO mailLog = mapper.getMailLog(log.getSeq());
                mailLog.setErrMsg(e.getMessage());
                sendMail(mailLog);
            }

            throw new RuntimeException(e);
        }

        //log 성공 update
        if(retryFlag) {
            if(retryLog.getDate() != null) {
                log.setRetryCount(retryLog.getRetryCount() + 1);
            }
        }
        log.setTaskStatus(1);
        log.setStatus(1);
        log.setTaskFileName(formatedNow+"_"+keywordSeq+"_recommanded_influencer.csv");
        mapper.updateLog(log);

        logger.info("parsing 끝");
        return csvFilePath;
    }

    @Override
    public String parsingPopularKeyword(int keywordSeq, String keyword, String filePath, String formatedNow, boolean retryFlag, timeVO time) throws Exception {
        logger.info(keyword + " popularKeyword parsing 시작");

        LogVO retryLog = new LogVO();
        LogVO log = new LogVO();

        //재수집 and 이전 수집때의 status가 성공이라면
        if(retryFlag) {
            // channelType별 수집의 성공 여부 체크
            // taskStatus = 1 일때는 성공했던 return "PASS"
            retryInfoVO retryInfo = new retryInfoVO();
            retryInfo.setKeywordSeq(keywordSeq);
            retryInfo.setChannelType("A");
            retryInfo.setTaskType(2);
            retryInfo.setTaskStep(5);
            retryInfo.setDate(time.getDate());
            retryInfo.setHh(time.getHh());

            //row가 있고 성공해서 continue 되는 경우
            if(mapper.getLog(retryInfo) != null) {
                retryLog = mapper.getLog(retryInfo);

                if(retryLog.getTaskStatus() == 1) {
                    return "D:/uploadInfluencerCsv/" + retryLog.getTaskFileName();
                }
            }
        }

        //logVO 생성
        log.setKeywordSeq(keywordSeq);
        log.setChannelType("A");
        log.setTaskType(2);
        log.setTaskStep(5);
        log.setTaskName("PARSING_CSV_ALL_STEP5");
        mapper.insertLog(log);

        List<PopularKeywordVO> popularKeywordList = new ArrayList<>();

        try {
            File file = new File(filePath);
            Document doc = Jsoup.parse(file, "UTF-8");

            Elements popularKs = doc.getElementsByClass("item_link_area");
            for (Element popularK : popularKs) {
                Elements popularKs1 = popularK.getElementsByClass("item_link");
                for (Element popularK1 : popularKs1) {
                    PopularKeywordVO popularKeyword = new PopularKeywordVO();

                    popularKeyword.setKeywordSeq(keywordSeq);
                    popularKeyword.setImage(popularK1.select("img").attr("data-lazysrc"));

                    String popularKeywordKeyword = popularK1.getElementsByClass("hash_tag").text();
                    popularKeyword.setKeyword(popularKeywordKeyword.substring(1));

                    String countStr = popularK1.getElementsByClass("count").text();
                    popularKeyword.setCount(Integer.parseInt(countStr.substring(0, countStr.length() - 5)));

                    popularKeywordList.add(popularKeyword);
                }
            }
        } catch (Exception e) {
            //log 실패 update
            if(retryFlag) {
                if (retryLog.getDate() != null) {
                    log.setRetryCount(retryLog.getRetryCount() + 1);

                    //3번 재시도 실패일 시 mailCount + 1
                    if(log.getRetryCount() == 3) {
                        log.setMailCount(1);
                    } else if(log.getRetryCount() > 3) {
                        log.setMailCount(retryLog.getMailCount());
                    }
                }
            }
            log.setTaskStatus(9);
            log.setStatus(9);
            log.setTaskMessage(e.getMessage());
            mapper.updateLog(log);

            //3번 재시도 실패일 시 메일 발송
            if(log.getRetryCount() == 3) {
                MailLogVO mailLog = mapper.getMailLog(log.getSeq());
                mailLog.setErrMsg(e.getMessage());
                sendMail(mailLog);
            }

            throw new RuntimeException(e);
        }

        //csv 파일 변환 --------------------------------------------------------------------
        String csvFilePath = "D:/uploadInfluencerCsv/"+formatedNow+"_"+keywordSeq+"_popular_keyword.csv";

        try {
            CSVWriter cw = new CSVWriter(new OutputStreamWriter(new FileOutputStream(csvFilePath), StandardCharsets.UTF_8),',', CSVWriter.NO_QUOTE_CHARACTER);
            try (cw) {
                cw.writeNext(new String[]{"\ufeff"});
                cw.writeNext(new String[]{"keyword_seq", "image", "keyword", "count"});
                for (PopularKeywordVO popularKeyword : popularKeywordList) {
                    cw.writeNext(new String[]{String.valueOf(popularKeyword.getKeywordSeq()), popularKeyword.getImage(), popularKeyword.getKeyword(), String.valueOf(popularKeyword.getCount())});
                }
            } catch (Exception e) {
                //log 실패 update
                if(retryFlag) {
                    if (retryLog.getDate() != null) {
                        log.setRetryCount(retryLog.getRetryCount() + 1);

                        //3번 재시도 실패일 시 mailCount + 1
                        if(log.getRetryCount() == 3) {
                            log.setMailCount(1);
                        } else if(log.getRetryCount() > 3) {
                            log.setMailCount(retryLog.getMailCount());
                        }
                    }
                }
                log.setTaskStatus(9);
                log.setStatus(9);
                log.setTaskMessage(e.getMessage());
                mapper.updateLog(log);

                //3번 재시도 실패일 시 메일 발송
                if(log.getRetryCount() == 3) {
                    MailLogVO mailLog = mapper.getMailLog(log.getSeq());
                    mailLog.setErrMsg(e.getMessage());
                    sendMail(mailLog);
                }

                throw new RuntimeException(e);
            }
        } catch (Exception e) {
            //log 실패 update
            if(retryFlag) {
                if (retryLog.getDate() != null) {
                    log.setRetryCount(retryLog.getRetryCount() + 1);

                    //3번 재시도 실패일 시 mailCount + 1
                    if(log.getRetryCount() == 3) {
                        log.setMailCount(1);
                    } else if(log.getRetryCount() > 3) {
                        log.setMailCount(retryLog.getMailCount());
                    }
                }
            }
            log.setTaskStatus(9);
            log.setStatus(9);
            log.setTaskMessage(e.getMessage());
            mapper.updateLog(log);

            //3번 재시도 실패일 시 메일 발송
            if(log.getRetryCount() == 3) {
                MailLogVO mailLog = mapper.getMailLog(log.getSeq());
                mailLog.setErrMsg(e.getMessage());
                sendMail(mailLog);
            }

            throw new RuntimeException(e);
        }

        //log 성공 update
        if(retryFlag) {
            if(retryLog.getDate() != null) {
                log.setRetryCount(retryLog.getRetryCount() + 1);
            }
        }
        log.setTaskStatus(1);
        log.setStatus(1);
        log.setTaskFileName(formatedNow+"_"+keywordSeq+"_popular_keyword.csv");
        mapper.updateLog(log);

        logger.info("parsing 끝");
        return csvFilePath;
    }

    @Override
    public String parsingInfluencerContent(int keywordSeq, String keyword, String filePath, String formatedNow, String channelType, boolean retryFlag, timeVO time) throws Exception {
        logger.info(keyword + " influencerContent parsing 시작");

        String taskName = null;
        switch (channelType) {
            case "T":
                taskName = "PARSING_CSV_CHANNEL_ALL_STEP6";
                break;
            case "B":
                taskName = "PARSING_CSV_CHANNEL_BLOG_STEP6";
                break;
            case "P":
                taskName = "PARSING_CSV_CHANNEL_POST_STEP6";
                break;
            case "N":
                taskName = "PARSING_CSV_CHANNEL_NTV_STEP6";
                break;
            case "Y":
                taskName = "PARSING_CSV_CHANNEL_YOUTUBE_STEP6";
                break;
        }

        LogVO retryLog = new LogVO();
        LogVO log = new LogVO();

        //재수집 and 이전 수집때의 status가 성공이라면
        if(retryFlag) {
            // channelType별 수집의 성공 여부 체크
            // taskStatus = 1 일때는 성공했던 return "PASS"
            retryInfoVO retryInfo = new retryInfoVO();
            retryInfo.setKeywordSeq(keywordSeq);
            retryInfo.setChannelType(channelType);
            retryInfo.setTaskType(2);
            retryInfo.setTaskStep(6);
            retryInfo.setDate(time.getDate());
            retryInfo.setHh(time.getHh());

            //row가 있고 성공해서 continue 되는 경우
            if(mapper.getLog(retryInfo) != null) {
                retryLog = mapper.getLog(retryInfo);

                if(retryLog.getTaskStatus() == 1) {
                    return "D:/uploadInfluencerCsv/" + retryLog.getTaskFileName();
                }
            }
        }

        //logVO 생성
        log.setKeywordSeq(keywordSeq);
        log.setChannelType(channelType);
        log.setTaskType(2);
        log.setTaskStep(6);
        log.setTaskName(taskName);
        mapper.insertLog(log);

        List<InfluencerContentVO> influencerContentList = new ArrayList<>();

        try {
            File file = new File(filePath);
            Document doc = Jsoup.parse(file, "UTF-8");

            Elements contents = doc.getElementsByClass("keyword_bx _item _check_visible");
            logger.info("갯수는 : " + contents.size());

            for (Element content : contents) {
                InfluencerContentVO influencerContent = new InfluencerContentVO();
                influencerContent.setKeywordSeq(keywordSeq);

                //userInfo
                Elements userBoxes = content.getElementsByClass("user_box");
                for (Element userBox : userBoxes) {
                    //influencerImg
                    influencerContent.setInfluencerImg(userBox.select("img").attr("data-lazysrc"));

                    //influencerImgLink
                    influencerContent.setInfluencerImgLink(userBox.getElementsByClass("user_thumb_wrap").attr("href"));

                    //influencerName
                    influencerContent.setInfluencerName(userBox.getElementsByClass("name elss").text());

                    //influencerType
                    influencerContent.setInfluencerType(userBox.getElementsByClass("etc highlight").text());

                    if(userBox.getElementsByClass("etc").size() == 3) {
                        //influencerDetail
                        influencerContent.setInfluencerTypeDetail(userBox.getElementsByClass("etc").get(1).text());
                        //fanCount
                        influencerContent.setFanCount(userBox.getElementsByClass("_fan_count").text());
                    } else if(userBox.getElementsByClass("etc").size() == 2) {
                        //fanCount
                        influencerContent.setFanCount(userBox.getElementsByClass("_fan_count").text());
                    }

                    //snsFollowerTypeCount
                    if(userBox.getElementsByClass("keyword type_catch").size() > 0) {
                        influencerContent.setSnsFollowerTypeCount(userBox.getElementsByClass("keyword type_catch").text());
                    }

                    //influencerInfo
                    if(userBox.getElementsByClass("dsc_inner").size() > 0) {
                        Elements info = userBox.getElementsByClass("dsc_inner");
                        influencerContent.setInfluencerInfo1Title(info.get(0).getElementsByClass("tit").text());
                        influencerContent.setInfluencerInfo2Title(info.get(1).getElementsByClass("tit").text());
                        influencerContent.setInfluencerInfo3Title(info.get(2).getElementsByClass("tit").text());
                        influencerContent.setInfluencerInfo1Desc(info.get(0).getElementsByClass("txt").text());
                        influencerContent.setInfluencerInfo2Desc(info.get(1).getElementsByClass("txt").text());
                        influencerContent.setInfluencerInfo3Desc(info.get(2).getElementsByClass("txt").text());
                    }
                }

                String contentNum;

                //contentInfo
                Elements detailBoxes = content.getElementsByClass("detail_box");
                for (Element detailBox : detailBoxes) {
                    //contentTitle
                    influencerContent.setContentTitle(detailBox.getElementsByClass("name_link").text());

                    //contentDesc
                    influencerContent.setContentDesc(detailBox.getElementsByClass("dsc_link").text());

                    //contentLink
                    String contentLink = detailBox.getElementsByClass("name_link").attr("href");
                    influencerContent.setContentLink(contentLink);

                    //contentNum
                    String[] strArr = contentLink.split("/");
                    String[] strArr2 = strArr[6].split("\\?");
                    contentNum = strArr2[0];
                    influencerContent.setContentNum(contentNum);

                    //contentPostDate
//                    String dateStr = detailBox.getElementsByClass("date").text();
//                    LocalDateTime nowDate = LocalDateTime.now();
//                    DateTimeFormatter formatterDate = DateTimeFormatter.ofPattern("yyyy.MM.dd");
//
//                    if(dateStr.length() > 6) {
//                        influencerContent.setContentPostDate(dateStr);
//                    } else if(dateStr.length() > 2){
//                        if(dateStr.endsWith("시간 전")) {
//                            int beforeHours = Integer.parseInt(dateStr.substring(0, dateStr.length()-4));
//                            nowDate = nowDate.minusHours(beforeHours);
//                        } else if(dateStr.endsWith("일 전")) {
//                            int beforeDate = Integer.parseInt(dateStr.substring(0, dateStr.length()-3));
//                            nowDate = nowDate.minusDays(beforeDate);
//                        }
//                        influencerContent.setContentPostDate(nowDate.format(formatterDate));
//                    } else {
//                        nowDate = nowDate.minusDays(1);
//                        influencerContent.setContentPostDate(nowDate.format(formatterDate));
//                    }

                    //contentPostDate
//
                    //contentPostDate
                    String dateStr = detailBox.getElementsByClass("date").text();
                    LocalDateTime nowDate = LocalDateTime.now();
                    DateTimeFormatter formatterDate = DateTimeFormatter.ofPattern("yyyy.MM.dd");

                    if(dateStr.length() > 6) {
                        influencerContent.setContentPostDate(dateStr);
                    } else if(dateStr.length() > 2){
                        if(dateStr.endsWith("시간 전")) {
                            int beforeHours = Integer.parseInt(dateStr.substring(0, dateStr.length()-4));
                            nowDate = nowDate.minusHours(beforeHours);
                        } else if(dateStr.endsWith("일 전")) {
                            int beforeDate = Integer.parseInt(dateStr.substring(0, dateStr.length()-3));
                            nowDate = nowDate.minusDays(beforeDate);
                        }
                        influencerContent.setContentPostDate(nowDate.format(formatterDate).toString());
                    } else {
                        nowDate = nowDate.minusDays(1);
                        influencerContent.setContentPostDate(nowDate.format(formatterDate).toString());
                    }
                }
                influencerContentList.add(influencerContent);
            }
        } catch (Exception e) {
            //log 실패 update
            if(retryFlag) {
                if (retryLog.getDate() != null) {
                    log.setRetryCount(retryLog.getRetryCount() + 1);

                    //3번 재시도 실패일 시 mailCount + 1
                    if(log.getRetryCount() == 3) {
                        log.setMailCount(1);
                    } else if(log.getRetryCount() > 3) {
                        log.setMailCount(retryLog.getMailCount());
                    }
                }
            }
            log.setTaskStatus(9);
            log.setStatus(9);
            log.setTaskMessage(e.getMessage());
            mapper.updateLog(log);

            //3번 재시도 실패일 시 메일 발송
            if(log.getRetryCount() == 3) {
                MailLogVO mailLog = mapper.getMailLog(log.getSeq());
                mailLog.setErrMsg(e.getMessage());
                sendMail(mailLog);
            }

            throw new RuntimeException(e);
        }

        //csv 파일 변환 --------------------------------------------------------------------
        String csvFilePath = "D:\\uploadInfluencerCsv/"+formatedNow+"_"+keywordSeq+channelType+"_influencer_content.csv";

        try {
            CSVWriter cw = new CSVWriter(new OutputStreamWriter(new FileOutputStream(csvFilePath), StandardCharsets.UTF_8),',', CSVWriter.NO_QUOTE_CHARACTER);
            try (cw) {
                cw.writeNext(new String[]{"\ufeff"});
                cw.writeNext(new String[]{"keyword_seq", "channel_type", "content_num", "influencer_image", "influencer_image_link", "influencer_name", "influencer_type",
                        "influencer_type_detail", "fan_count", "sns_follower_type_count", "influencer_info1_title", "influencer_info2_title", "influencer_info3_title",
                        "influencer_info1_desc", "influencer_info2_desc", "influencer_info3_desc", "content_title", "content_desc", "content_link", "content_post_date"});
                for (InfluencerContentVO influencerContent : influencerContentList) {
                    influencerContent.setInfluencerName(influencerContent.getInfluencerName().replace(",", ""));
                    influencerContent.setFanCount(influencerContent.getFanCount().replace(",", ""));
                    influencerContent.setContentTitle(influencerContent.getContentTitle().replace(",", " "));
                    influencerContent.setContentDesc(influencerContent.getContentDesc().replace(",", " "));
                    if (influencerContent.getSnsFollowerTypeCount() != null) {
                        influencerContent.setSnsFollowerTypeCount(influencerContent.getSnsFollowerTypeCount().replace(",", ""));
                    }

                    cw.writeNext(new String[]{String.valueOf(influencerContent.getKeywordSeq()), channelType, influencerContent.getContentNum(), influencerContent.getInfluencerImg(), influencerContent.getInfluencerImgLink(),
                            influencerContent.getInfluencerName(), influencerContent.getInfluencerType(), influencerContent.getInfluencerTypeDetail(), influencerContent.getFanCount(),
                            influencerContent.getSnsFollowerTypeCount(), influencerContent.getInfluencerInfo1Title(), influencerContent.getInfluencerInfo2Title(), influencerContent.getInfluencerInfo3Title(),
                            influencerContent.getInfluencerInfo1Desc(), influencerContent.getInfluencerInfo2Desc(), influencerContent.getInfluencerInfo3Desc(), influencerContent.getContentTitle(),
                            influencerContent.getContentDesc(), influencerContent.getContentLink(), influencerContent.getContentPostDate()});
                }
            } catch (Exception e) {
                //log 실패 update
                if(retryFlag) {
                    if (retryLog.getDate() != null) {
                        log.setRetryCount(retryLog.getRetryCount() + 1);

                        //3번 재시도 실패일 시 mailCount + 1
                        if(log.getRetryCount() == 3) {
                            log.setMailCount(1);
                        } else if(log.getRetryCount() > 3) {
                            log.setMailCount(retryLog.getMailCount());
                        }
                    }
                }
                log.setTaskStatus(9);
                log.setStatus(9);
                log.setTaskMessage(e.getMessage());
                mapper.updateLog(log);

                //3번 재시도 실패일 시 메일 발송
                if(log.getRetryCount() == 3) {
                    MailLogVO mailLog = mapper.getMailLog(log.getSeq());
                    mailLog.setErrMsg(e.getMessage());
                    sendMail(mailLog);
                }

                throw new RuntimeException(e);
            }
        } catch (Exception e) {
            //log 실패 update
            if(retryFlag) {
                if (retryLog.getDate() != null) {
                    log.setRetryCount(retryLog.getRetryCount() + 1);

                    //3번 재시도 실패일 시 mailCount + 1
                    if(log.getRetryCount() == 3) {
                        log.setMailCount(1);
                    } else if(log.getRetryCount() > 3) {
                        log.setMailCount(retryLog.getMailCount());
                    }
                }
            }
            log.setTaskStatus(9);
            log.setStatus(9);
            log.setTaskMessage(e.getMessage());
            mapper.updateLog(log);

            //3번 재시도 실패일 시 메일 발송
            if(log.getRetryCount() == 3) {
                MailLogVO mailLog = mapper.getMailLog(log.getSeq());
                mailLog.setErrMsg(e.getMessage());
                sendMail(mailLog);
            }

            throw new RuntimeException(e);
        }

        //log 성공 update
        if(retryFlag) {
            if(retryLog.getDate() != null) {
                log.setRetryCount(retryLog.getRetryCount() + 1);
            }
        }
        log.setTaskStatus(1);
        log.setStatus(1);
        log.setTaskFileName(formatedNow+"_"+keywordSeq+channelType+"_influencer_content.csv");
        mapper.updateLog(log);

        logger.info("parsing 끝");
        return csvFilePath;
    }

    @Override
    public String parsingInfluencerContentImage(int keywordSeq, String keyword, String filePath, String formatedNow, String channelType, boolean retryFlag, timeVO time) throws Exception {
        logger.info(keyword + " influencerContentImage parsing 시작");

        String taskName = null;
        switch (channelType) {
            case "T":
                taskName = "PARSING_CSV_CHANNEL_ALL_STEP7";
                break;
            case "B":
                taskName = "PARSING_CSV_CHANNEL_BLOG_STEP7";
                break;
            case "P":
                taskName = "PARSING_CSV_CHANNEL_POST_STEP7";
                break;
            case "N":
                taskName = "PARSING_CSV_CHANNEL_NTV_STEP7";
                break;
            case "Y":
                taskName = "PARSING_CSV_CHANNEL_YOUTUBE_STEP7";
                break;
        }

        LogVO retryLog = new LogVO();
        LogVO log = new LogVO();

        //재수집 and 이전 수집때의 status가 성공이라면
        if(retryFlag) {
            // channelType별 수집의 성공 여부 체크
            // taskStatus = 1 일때는 성공했던 return "PASS"
            retryInfoVO retryInfo = new retryInfoVO();
            retryInfo.setKeywordSeq(keywordSeq);
            retryInfo.setChannelType(channelType);
            retryInfo.setTaskType(2);
            retryInfo.setTaskStep(7);
            retryInfo.setDate(time.getDate());
            retryInfo.setHh(time.getHh());

            //row가 있고 성공해서 continue 되는 경우
            if(mapper.getLog(retryInfo) != null) {
                retryLog = mapper.getLog(retryInfo);

                if(retryLog.getTaskStatus() == 1) {
                    return "D:/uploadInfluencerCsv/" + retryLog.getTaskFileName();
                }
            }
        }

        //logVO 생성
        log.setKeywordSeq(keywordSeq);
        log.setChannelType(channelType);
        log.setTaskType(2);
        log.setTaskStep(7);
        log.setTaskName(taskName);
        mapper.insertLog(log);

        List<InfluencerContentImageVO> contentImageList = new ArrayList<>();

        try {
            File file = new File(filePath);
            Document doc = Jsoup.parse(file, "UTF-8");

            Elements contents = doc.getElementsByClass("keyword_bx _item _check_visible");
            for (Element content : contents) {

                String contentNum;

                Elements detailBoxes = content.getElementsByClass("detail_box");
                for (Element detailBox : detailBoxes) {
                    //contentLink
                    String contentLink = detailBox.getElementsByClass("name_link").attr("href");

                    //contentNum
                    String[] strArr = contentLink.split("/");
                    String[] strArr2 = strArr[6].split("\\?");
                    contentNum = strArr2[0];

                    //contentImage ----------------------------------------------------
                    Elements images;
                    if(detailBox.getElementsByClass("flick_bx").size() > 0) {
                        images = detailBox.getElementsByClass("flick_bx");
                        for (Element image : images) {
                            InfluencerContentImageVO contentImage = new InfluencerContentImageVO();

                            contentImage.setKeywordSeq(keywordSeq);
                            contentImage.setContentNum(contentNum);
                            contentImage.setContentImageUrl(image.select("img").attr("src"));

                            contentImageList.add(contentImage);
                        }
                    } else {
                        images = detailBox.getElementsByClass("img bg_nimg api_get _foryou_image_source");
                        for (Element image : images) {
                            InfluencerContentImageVO contentImage = new InfluencerContentImageVO();

                            contentImage.setKeywordSeq(keywordSeq);
                            contentImage.setContentNum(contentNum);
                            contentImage.setContentImageUrl(image.select("img").attr("data-lazysrc"));

                            contentImageList.add(contentImage);
                        }
                    }

                }
            }
        } catch (Exception e) {
            //log 실패 update
            if(retryFlag) {
                if (retryLog.getDate() != null) {
                    log.setRetryCount(retryLog.getRetryCount() + 1);

                    //3번 재시도 실패일 시 mailCount + 1
                    if(log.getRetryCount() == 3) {
                        log.setMailCount(1);
                    } else if(log.getRetryCount() > 3) {
                        log.setMailCount(retryLog.getMailCount());
                    }
                }
            }
            log.setTaskStatus(9);
            log.setStatus(9);
            log.setTaskMessage(e.getMessage());
            mapper.updateLog(log);

            //3번 재시도 실패일 시 메일 발송
            if(log.getRetryCount() == 3) {
                MailLogVO mailLog = mapper.getMailLog(log.getSeq());
                mailLog.setErrMsg(e.getMessage());
                sendMail(mailLog);
            }

            throw new RuntimeException(e);
        }

        //csv 파일 변환 --------------------------------------------------------------------
        String csvFilePath = "D:\\uploadInfluencerCsv/"+formatedNow+"_"+keywordSeq+channelType+"_influencer_content_image.csv";

        try {
            CSVWriter cw = new CSVWriter(new OutputStreamWriter(new FileOutputStream(csvFilePath), StandardCharsets.UTF_8),',', CSVWriter.NO_QUOTE_CHARACTER);
            try (cw) {
                cw.writeNext(new String[]{"\ufeff"});
                cw.writeNext(new String[]{"keyword_seq", "content_num", "content_image_url"});
                for (InfluencerContentImageVO influencerContentImage : contentImageList) {
                    cw.writeNext(new String[]{String.valueOf(influencerContentImage.getKeywordSeq()), influencerContentImage.getContentNum(), influencerContentImage.getContentImageUrl()});
                }
            } catch (Exception e) {
                //log 실패 update
                if(retryFlag) {
                    if (retryLog.getDate() != null) {
                        log.setRetryCount(retryLog.getRetryCount() + 1);

                        //3번 재시도 실패일 시 mailCount + 1
                        if(log.getRetryCount() == 3) {
                            log.setMailCount(1);
                        } else if(log.getRetryCount() > 3) {
                            log.setMailCount(retryLog.getMailCount());
                        }
                    }
                }
                log.setTaskStatus(9);
                log.setStatus(9);
                log.setTaskMessage(e.getMessage());
                mapper.updateLog(log);

                //3번 재시도 실패일 시 메일 발송
                if(log.getRetryCount() == 3) {
                    MailLogVO mailLog = mapper.getMailLog(log.getSeq());
                    mailLog.setErrMsg(e.getMessage());
                    sendMail(mailLog);
                }

                throw new RuntimeException(e);
            }
        } catch (Exception e) {
            //log 실패 update
            if(retryFlag) {
                if (retryLog.getDate() != null) {
                    log.setRetryCount(retryLog.getRetryCount() + 1);

                    //3번 재시도 실패일 시 mailCount + 1
                    if(log.getRetryCount() == 3) {
                        log.setMailCount(1);
                    } else if(log.getRetryCount() > 3) {
                        log.setMailCount(retryLog.getMailCount());
                    }
                }
            }
            log.setTaskStatus(9);
            log.setStatus(9);
            log.setTaskMessage(e.getMessage());
            mapper.updateLog(log);

            //3번 재시도 실패일 시 메일 발송
            if(log.getRetryCount() == 3) {
                MailLogVO mailLog = mapper.getMailLog(log.getSeq());
                mailLog.setErrMsg(e.getMessage());
                sendMail(mailLog);
            }

            throw new RuntimeException(e);
        }

        //log 성공 update
        if(retryFlag) {
            if(retryLog.getDate() != null) {
                log.setRetryCount(retryLog.getRetryCount() + 1);
            }
        }
        log.setTaskStatus(1);
        log.setStatus(1);
        log.setTaskFileName(formatedNow+"_"+keywordSeq+channelType+"_influencer_content_image.csv");
        mapper.updateLog(log);

        logger.info("parsing 끝");
        return csvFilePath;
    }

    @Override
    public String parsingRelatedContent(int keywordSeq, String keyword, String filePath, String formatedNow, String channelType, boolean retryFlag, timeVO time) throws Exception {
        logger.info(keyword + " relatedContent parsing 시작");

        String taskName = null;
        switch (channelType) {
            case "T":
                taskName = "PARSING_CSV_CHANNEL_ALL_STEP8";
                break;
            case "B":
                taskName = "PARSING_CSV_CHANNEL_BLOG_STEP8";
                break;
            case "P":
                taskName = "PARSING_CSV_CHANNEL_POST_STEP8";
                break;
            case "N":
                taskName = "PARSING_CSV_CHANNEL_NTV_STEP8";
                break;
            case "Y":
                taskName = "PARSING_CSV_CHANNEL_YOUTUBE_STEP8";
                break;
        }

        LogVO retryLog = new LogVO();
        LogVO log = new LogVO();

        //재수집 and 이전 수집때의 status가 성공이라면
        if(retryFlag) {
            // channelType별 수집의 성공 여부 체크
            // taskStatus = 1 일때는 성공했던 return "PASS"
            retryInfoVO retryInfo = new retryInfoVO();
            retryInfo.setKeywordSeq(keywordSeq);
            retryInfo.setChannelType(channelType);
            retryInfo.setTaskType(2);
            retryInfo.setTaskStep(8);
            retryInfo.setDate(time.getDate());
            retryInfo.setHh(time.getHh());

            //row가 있고 성공해서 continue 되는 경우
            if(mapper.getLog(retryInfo) != null) {
                retryLog = mapper.getLog(retryInfo);

                if(retryLog.getTaskStatus() == 1) {
                    return "D:/uploadInfluencerCsv/" + retryLog.getTaskFileName();
                }
            }
        }

        //logVO 생성
        log.setKeywordSeq(keywordSeq);
        log.setChannelType(channelType);
        log.setTaskType(2);
        log.setTaskStep(8);
        log.setTaskName(taskName);
        mapper.insertLog(log);

        List<RelatedContentVO> relatedContentList = new ArrayList<>();

        try {
            File file = new File(filePath);
            Document doc = Jsoup.parse(file, "UTF-8");

            Elements contents = doc.getElementsByClass("keyword_bx _item _check_visible");
            for (Element content : contents) {

                String contentNum = null;

                //contentInfo
                Elements detailBoxes = content.getElementsByClass("detail_box");
                for (Element detailBox : detailBoxes) {
                    //contentLink
                    String contentLink = detailBox.getElementsByClass("name_link").attr("href");

                    //contentNum
                    String[] strArr = contentLink.split("/");
                    String[] strArr2 = strArr[6].split("\\?");
                    contentNum = strArr2[0];
                }

                //relatedContent
                if(content.getElementsByClass("link_box").size() > 0) {
                    Elements linkBoxes = content.getElementsByClass("link_box");
                    for (Element linkBox : linkBoxes) {
                        Elements relatedLinks = linkBox.getElementsByClass("link");
                        for(Element relatedLink : relatedLinks) {
                            RelatedContentVO relatedContent = new RelatedContentVO();

                            relatedContent.setKeywordSeq(keywordSeq);
                            relatedContent.setContentNum(contentNum);
                            relatedContent.setContentTitle(relatedLink.getElementsByClass("name").text());
                            relatedContent.setContentUrl(relatedLink.attr("href"));

                            relatedContentList.add(relatedContent);
                        }
                    }
                }
            }
        } catch (Exception e) {
            //log 실패 update
            if(retryFlag) {
                if (retryLog.getDate() != null) {
                    log.setRetryCount(retryLog.getRetryCount() + 1);

                    //3번 재시도 실패일 시 mailCount + 1
                    if(log.getRetryCount() == 3) {
                        log.setMailCount(1);
                    } else if(log.getRetryCount() > 3) {
                        log.setMailCount(retryLog.getMailCount());
                    }
                }
            }
            log.setTaskStatus(9);
            log.setStatus(9);
            log.setTaskMessage(e.getMessage());
            mapper.updateLog(log);

            //3번 재시도 실패일 시 메일 발송
            if(log.getRetryCount() == 3) {
                MailLogVO mailLog = mapper.getMailLog(log.getSeq());
                mailLog.setErrMsg(e.getMessage());
                sendMail(mailLog);
            }

            throw new RuntimeException(e);
        }

        //csv 파일 변환 --------------------------------------------------------------------
        String csvFilePath = "D:\\uploadInfluencerCsv/"+formatedNow+"_"+keywordSeq+channelType+"_related_content.csv";

        try {
            CSVWriter cw = new CSVWriter(new OutputStreamWriter(new FileOutputStream(csvFilePath), StandardCharsets.UTF_8),',', CSVWriter.NO_QUOTE_CHARACTER);
            try (cw) {
                cw.writeNext(new String[]{"\ufeff"});
                cw.writeNext(new String[]{"keyword_seq", "content_num", "content_title", "content_url"});
                for (RelatedContentVO relatedContent : relatedContentList) {
                    relatedContent.setContentTitle(relatedContent.getContentTitle().replace(",", " "));

                    cw.writeNext(new String[]{String.valueOf(relatedContent.getKeywordSeq()), relatedContent.getContentNum(), relatedContent.getContentTitle(), relatedContent.getContentUrl()});
                }
            } catch (Exception e) {
                //log 실패 update
                if(retryFlag) {
                    if (retryLog.getDate() != null) {
                        log.setRetryCount(retryLog.getRetryCount() + 1);

                        //3번 재시도 실패일 시 mailCount + 1
                        if(log.getRetryCount() == 3) {
                            log.setMailCount(1);
                        } else if(log.getRetryCount() > 3) {
                            log.setMailCount(retryLog.getMailCount());
                        }
                    }
                }
                log.setTaskStatus(9);
                log.setStatus(9);
                log.setTaskMessage(e.getMessage());
                mapper.updateLog(log);

                //3번 재시도 실패일 시 메일 발송
                if(log.getRetryCount() == 3) {
                    MailLogVO mailLog = mapper.getMailLog(log.getSeq());
                    mailLog.setErrMsg(e.getMessage());
                    sendMail(mailLog);
                }

                throw new RuntimeException(e);
            }
        } catch (Exception e) {
            //log 실패 update
            if(retryFlag) {
                if (retryLog.getDate() != null) {
                    log.setRetryCount(retryLog.getRetryCount() + 1);

                    //3번 재시도 실패일 시 mailCount + 1
                    if(log.getRetryCount() == 3) {
                        log.setMailCount(1);
                    } else if(log.getRetryCount() > 3) {
                        log.setMailCount(retryLog.getMailCount());
                    }
                }
            }
            log.setTaskStatus(9);
            log.setStatus(9);
            log.setTaskMessage(e.getMessage());
            mapper.updateLog(log);

            //3번 재시도 실패일 시 메일 발송
            if(log.getRetryCount() == 3) {
                MailLogVO mailLog = mapper.getMailLog(log.getSeq());
                mailLog.setErrMsg(e.getMessage());
                sendMail(mailLog);
            }

            throw new RuntimeException(e);
        }

        //log 성공 update
        if(retryFlag) {
            if(retryLog.getDate() != null) {
                log.setRetryCount(retryLog.getRetryCount() + 1);
            }
        }
        log.setTaskStatus(1);
        log.setStatus(1);
        log.setTaskFileName(formatedNow+"_"+keywordSeq+channelType+"_related_content.csv");
        mapper.updateLog(log);

        logger.info("parsing 끝");
        return csvFilePath;
    }

    @Override
    public boolean insertKeywordInfo(int keywordSeq, String filePath, boolean retryFlag, timeVO time) throws Exception {

        LogVO retryLog = new LogVO();
        LogVO log = new LogVO();

        //재수집 and 이전 수집때의 status가 성공이라면
        if(retryFlag) {
            // channelType별 수집의 성공 여부 체크
            // taskStatus = 1 일때는 성공했던 return "PASS"
            retryInfoVO retryInfo = new retryInfoVO();
            retryInfo.setKeywordSeq(keywordSeq);
            retryInfo.setChannelType("A");
            retryInfo.setTaskType(3);
            retryInfo.setTaskStep(1);
            retryInfo.setDate(time.getDate());
            retryInfo.setHh(time.getHh());

            //row가 있고 성공해서 continue 되는 경우
            if(mapper.getLog(retryInfo) != null) {
                retryLog = mapper.getLog(retryInfo);

                if(retryLog.getTaskStatus() == 1) {
                    return false;
                }
            }
        }

        //logVO 생성
        log.setKeywordSeq(keywordSeq);
        log.setChannelType("A");
        log.setTaskType(3);
        log.setTaskStep(1);
        log.setTaskName("INSERT_TO_MYSQL_ALL_STEP1");
        mapper.insertLog(log);

        int minSeq = mapper.getSeq("keyword_influencer_info") + 1;
        try {
            //db 저장
            if(mapper.insertKeywordInfo(filePath)) {
                int maxSeq = mapper.getSeq("keyword_influencer_info");
                mapper.updateLogSeq(log.getSeq(), minSeq, maxSeq, "keyword_influencer_info");
            }

            //log 성공 update
            if(retryFlag) {
                if(retryLog.getDate() != null) {
                    log.setRetryCount(retryLog.getRetryCount() + 1);
                }
            }
            log.setTaskStatus(1);
            log.setStatus(1);
            mapper.updateLog(log);

        } catch (Exception e) {
            //log 실패 update
            if(retryFlag) {
                if (retryLog.getDate() != null) {
                    log.setRetryCount(retryLog.getRetryCount() + 1);

                    //3번 재시도 실패일 시 mailCount + 1
                    if(log.getRetryCount() == 3) {
                        log.setMailCount(1);
                    } else if(log.getRetryCount() > 3) {
                        log.setMailCount(retryLog.getMailCount());
                    }
                }
            }
            log.setTaskStatus(9);
            log.setStatus(9);
            log.setTaskMessage(e.getMessage());
            mapper.updateLog(log);

            //3번 재시도 실패일 시 메일 발송
            if(log.getRetryCount() == 3) {
                MailLogVO mailLog = mapper.getMailLog(log.getSeq());
                mailLog.setErrMsg(e.getMessage());
                sendMail(mailLog);
            }

            throw new RuntimeException(e);
        }
        return true;
    }

    @Override
    public boolean insertKeywordInfoDetail(int keywordSeq, String filePath, boolean retryFlag, timeVO time) throws Exception {

        LogVO retryLog = new LogVO();
        LogVO log = new LogVO();

        //재수집 and 이전 수집때의 status가 성공이라면
        if(retryFlag) {
            // channelType별 수집의 성공 여부 체크
            // taskStatus = 1 일때는 성공했던 return "PASS"
            retryInfoVO retryInfo = new retryInfoVO();
            retryInfo.setKeywordSeq(keywordSeq);
            retryInfo.setChannelType("A");
            retryInfo.setTaskType(3);
            retryInfo.setTaskStep(2);
            retryInfo.setDate(time.getDate());
            retryInfo.setHh(time.getHh());

            //row가 있고 성공해서 continue 되는 경우
            if(mapper.getLog(retryInfo) != null) {
                retryLog = mapper.getLog(retryInfo);

                if(retryLog.getTaskStatus() == 1) {
                    return false;
                }
            }
        }

        //logVO 생성
        log.setKeywordSeq(keywordSeq);
        log.setChannelType("A");
        log.setTaskType(3);
        log.setTaskStep(2);
        log.setTaskName("INSERT_TO_MYSQL_ALL_STEP2");
        mapper.insertLog(log);

        int minSeq = mapper.getSeq("keyword_influencer_detail") + 1;
        try {
            //db 저장
            if(mapper.insertKeywordInfoDetail(filePath)) {
                int maxSeq = mapper.getSeq("keyword_influencer_detail");
                mapper.updateLogSeq(log.getSeq(), minSeq, maxSeq, "keyword_influencer_detail");
            }

            //log 성공 update
            if(retryFlag) {
                if(retryLog.getDate() != null) {
                    log.setRetryCount(retryLog.getRetryCount() + 1);
                }
            }
            log.setTaskStatus(1);
            log.setStatus(1);
            mapper.updateLog(log);

        } catch (Exception e) {
            //log 실패 update
            if(retryFlag) {
                if (retryLog.getDate() != null) {
                    log.setRetryCount(retryLog.getRetryCount() + 1);

                    //3번 재시도 실패일 시 mailCount + 1
                    if(log.getRetryCount() == 3) {
                        log.setMailCount(1);
                    } else if(log.getRetryCount() > 3) {
                        log.setMailCount(retryLog.getMailCount());
                    }
                }
            }
            log.setTaskStatus(9);
            log.setStatus(9);
            log.setTaskMessage(e.getMessage());
            mapper.updateLog(log);

            //3번 재시도 실패일 시 메일 발송
            if(log.getRetryCount() == 3) {
                MailLogVO mailLog = mapper.getMailLog(log.getSeq());
                mailLog.setErrMsg(e.getMessage());
                sendMail(mailLog);
            }

            throw new RuntimeException(e);
        }
        return true;
    }

    @Override
    public boolean insertKeywordFashionStyle(int keywordSeq, String filePath, boolean retryFlag, timeVO time) throws Exception {

        LogVO retryLog = new LogVO();
        LogVO log = new LogVO();

        //재수집 and 이전 수집때의 status가 성공이라면
        if(retryFlag) {
            // channelType별 수집의 성공 여부 체크
            // taskStatus = 1 일때는 성공했던 return "PASS"
            retryInfoVO retryInfo = new retryInfoVO();
            retryInfo.setKeywordSeq(keywordSeq);
            retryInfo.setChannelType("A");
            retryInfo.setTaskType(3);
            retryInfo.setTaskStep(3);
            retryInfo.setDate(time.getDate());
            retryInfo.setHh(time.getHh());

            //row가 있고 성공해서 continue 되는 경우
            if(mapper.getLog(retryInfo) != null) {
                retryLog = mapper.getLog(retryInfo);

                if(retryLog.getTaskStatus() == 1) {
                    return false;
                }
            }
        }

        //logVO 생성
        log.setKeywordSeq(keywordSeq);
        log.setChannelType("A");
        log.setTaskType(3);
        log.setTaskStep(3);
        log.setTaskName("INSERT_TO_MYSQL_ALL_STEP3");
        mapper.insertLog(log);

        int minSeq = mapper.getSeq("keyword_info_style") + 1;
        try {
            //db 저장
            if(mapper.insertKeywordFashionStyle(filePath)) {
                int maxSeq = mapper.getSeq("keyword_info_style");
                mapper.updateLogSeq(log.getSeq(), minSeq, maxSeq, "keyword_info_style");
            }

            //log 성공 update
            if(retryFlag) {
                if(retryLog.getDate() != null) {
                    log.setRetryCount(retryLog.getRetryCount() + 1);
                }
            }
            log.setTaskStatus(1);
            log.setStatus(1);
            log.setTaskFileName(null);
            mapper.updateLog(log);

        } catch (Exception e) {
            //log 실패 update
            if(retryFlag) {
                if (retryLog.getDate() != null) {
                    log.setRetryCount(retryLog.getRetryCount() + 1);

                    //3번 재시도 실패일 시 mailCount + 1
                    if(log.getRetryCount() == 3) {
                        log.setMailCount(1);
                    } else if(log.getRetryCount() > 3) {
                        log.setMailCount(retryLog.getMailCount());
                    }
                }
            }
            log.setTaskStatus(9);
            log.setStatus(9);
            log.setTaskMessage(e.getMessage());
            mapper.updateLog(log);

            //3번 재시도 실패일 시 메일 발송
            if(log.getRetryCount() == 3) {
                MailLogVO mailLog = mapper.getMailLog(log.getSeq());
                mailLog.setErrMsg(e.getMessage());
                sendMail(mailLog);
            }

            throw new RuntimeException(e);
        }
        return true;
    }

    @Override
    public boolean insertRecommendedInfluencer(int keywordSeq, String filePath, boolean retryFlag, timeVO time) throws Exception {

        LogVO retryLog = new LogVO();
        LogVO log = new LogVO();

        //재수집 and 이전 수집때의 status가 성공이라면
        if(retryFlag) {
            // channelType별 수집의 성공 여부 체크
            // taskStatus = 1 일때는 성공했던 return "PASS"
            retryInfoVO retryInfo = new retryInfoVO();
            retryInfo.setKeywordSeq(keywordSeq);
            retryInfo.setChannelType("A");
            retryInfo.setTaskType(3);
            retryInfo.setTaskStep(4);
            retryInfo.setDate(time.getDate());
            retryInfo.setHh(time.getHh());

            //row가 있고 성공해서 continue 되는 경우
            if(mapper.getLog(retryInfo) != null) {
                retryLog = mapper.getLog(retryInfo);

                if(retryLog.getTaskStatus() == 1) {
                    return false;
                }
            }
        }

        //logVO 생성
        log.setKeywordSeq(keywordSeq);
        log.setChannelType("A");
        log.setTaskType(3);
        log.setTaskStep(4);
        log.setTaskName("INSERT_TO_MYSQL_ALL_STEP4");
        mapper.insertLog(log);

        int minSeq = mapper.getSeq("recommanded_influencer") + 1;
        try {
            //db 저장
            if(mapper.insertRecommendedInfluencer(filePath)) {
                int maxSeq = mapper.getSeq("recommanded_influencer");
                mapper.updateLogSeq(log.getSeq(), minSeq, maxSeq, "recommanded_influencer");
            }

            //log 성공 update
            if(retryFlag) {
                if(retryLog.getDate() != null) {
                    log.setRetryCount(retryLog.getRetryCount() + 1);
                }
            }
            log.setTaskStatus(1);
            log.setStatus(1);
            mapper.updateLog(log);

        } catch (Exception e) {
            //log 실패 update
            if(retryFlag) {
                if (retryLog.getDate() != null) {
                    log.setRetryCount(retryLog.getRetryCount() + 1);

                    //3번 재시도 실패일 시 mailCount + 1
                    if(log.getRetryCount() == 3) {
                        log.setMailCount(1);
                    } else if(log.getRetryCount() > 3) {
                        log.setMailCount(retryLog.getMailCount());
                    }
                }
            }
            log.setTaskStatus(9);
            log.setStatus(9);
            log.setTaskMessage(e.getMessage());
            mapper.updateLog(log);

            //3번 재시도 실패일 시 메일 발송
            if(log.getRetryCount() == 3) {
                MailLogVO mailLog = mapper.getMailLog(log.getSeq());
                mailLog.setErrMsg(e.getMessage());
                sendMail(mailLog);
            }

            throw new RuntimeException(e);
        }
        return true;
    }

    @Override
    public boolean insertPopularKeyword(int keywordSeq, String filePath, boolean retryFlag, timeVO time) throws Exception {

        LogVO retryLog = new LogVO();
        LogVO log = new LogVO();

        //재수집 and 이전 수집때의 status가 성공이라면
        if(retryFlag) {
            // channelType별 수집의 성공 여부 체크
            // taskStatus = 1 일때는 성공했던 return "PASS"
            retryInfoVO retryInfo = new retryInfoVO();
            retryInfo.setKeywordSeq(keywordSeq);
            retryInfo.setChannelType("A");
            retryInfo.setTaskType(3);
            retryInfo.setTaskStep(5);
            retryInfo.setDate(time.getDate());
            retryInfo.setHh(time.getHh());

            //row가 있고 성공해서 continue 되는 경우
            if(mapper.getLog(retryInfo) != null) {
                retryLog = mapper.getLog(retryInfo);

                if(retryLog.getTaskStatus() == 1) {
                    return false;
                }
            }
        }

        //logVO 생성
        log.setKeywordSeq(keywordSeq);
        log.setChannelType("A");
        log.setTaskType(3);
        log.setTaskStep(5);
        log.setTaskName("INSERT_TO_MYSQL_ALL_STEP5");
        mapper.insertLog(log);

        int minSeq = mapper.getSeq("popular_keyword") + 1;
        try {
            //db 저장
            if(mapper.insertPopularKeyword(filePath)) {
                int maxSeq = mapper.getSeq("popular_keyword");
                mapper.updateLogSeq(log.getSeq(), minSeq, maxSeq, "popular_keyword");
            }

            //log 성공 update
            if(retryFlag) {
                if(retryLog.getDate() != null) {
                    log.setRetryCount(retryLog.getRetryCount() + 1);
                }
            }
            log.setTaskStatus(1);
            log.setStatus(1);
            mapper.updateLog(log);

        } catch (Exception e) {
            //log 실패 update
            if(retryFlag) {
                if (retryLog.getDate() != null) {
                    log.setRetryCount(retryLog.getRetryCount() + 1);

                    //3번 재시도 실패일 시 mailCount + 1
                    if(log.getRetryCount() == 3) {
                        log.setMailCount(1);
                    } else if(log.getRetryCount() > 3) {
                        log.setMailCount(retryLog.getMailCount());
                    }
                }
            }
            log.setTaskStatus(9);
            log.setStatus(9);
            log.setTaskMessage(e.getMessage());
            mapper.updateLog(log);

            //3번 재시도 실패일 시 메일 발송
            if(log.getRetryCount() == 3) {
                MailLogVO mailLog = mapper.getMailLog(log.getSeq());
                mailLog.setErrMsg(e.getMessage());
                sendMail(mailLog);
            }

            throw new RuntimeException(e);
        }
        return true;
    }

    @Override
    public boolean insertInfluencerContent(int keywordSeq, String filePath, String channelType, boolean retryFlag, timeVO time) throws Exception {

        String taskName = null;
        switch (channelType) {
            case "T":
                taskName = "INSERT_TO_MYSQL_CHANNEL_ALL_STEP6";
                break;
            case "B":
                taskName = "INSERT_TO_MYSQL_CHANNEL_BLOG_STEP6";
                break;
            case "P":
                taskName = "INSERT_TO_MYSQL_CHANNEL_POST_STEP6";
                break;
            case "N":
                taskName = "INSERT_TO_MYSQL_CHANNEL_NTV_STEP6";
                break;
            case "Y":
                taskName = "INSERT_TO_MYSQL_CHANNEL_YOUTUBE_STEP6";
                break;
        }

        LogVO retryLog = new LogVO();
        LogVO log = new LogVO();

        //재수집 and 이전 수집때의 status가 성공이라면
        if(retryFlag) {
            // channelType별 수집의 성공 여부 체크
            // taskStatus = 1 일때는 성공했던 return "PASS"
            retryInfoVO retryInfo = new retryInfoVO();
            retryInfo.setKeywordSeq(keywordSeq);
            retryInfo.setChannelType(channelType);
            retryInfo.setTaskType(3);
            retryInfo.setTaskStep(6);
            retryInfo.setDate(time.getDate());
            retryInfo.setHh(time.getHh());

            //row가 있고 성공해서 continue 되는 경우
            if(mapper.getLog(retryInfo) != null) {
                retryLog = mapper.getLog(retryInfo);

                if(retryLog.getTaskStatus() == 1) {
                    return false;
                }
            }
        }

        //logVO 생성
        log.setKeywordSeq(keywordSeq);
        log.setChannelType(channelType);
        log.setTaskType(3);
        log.setTaskStep(6);
        log.setTaskName(taskName);
        mapper.insertLog(log);

        int minSeq = mapper.getSeq("influencer_content") + 1;
        try {
            //db 저장
            if(mapper.insertInfluencerContent(filePath)) {
                int maxSeq = mapper.getSeq("influencer_content");
                mapper.updateLogSeq(log.getSeq(), minSeq, maxSeq, "influencer_content");
            }

            //log 성공 update
            if(retryFlag) {
                if(retryLog.getDate() != null) {
                    log.setRetryCount(retryLog.getRetryCount() + 1);
                }
            }
            log.setTaskStatus(1);
            log.setStatus(1);
            mapper.updateLog(log);

        } catch (Exception e) {
            //log 실패 update
            if(retryFlag) {
                if (retryLog.getDate() != null) {
                    log.setRetryCount(retryLog.getRetryCount() + 1);

                    //3번 재시도 실패일 시 mailCount + 1
                    if(log.getRetryCount() == 3) {
                        log.setMailCount(1);
                    } else if(log.getRetryCount() > 3) {
                        log.setMailCount(retryLog.getMailCount());
                    }
                }
            }
            log.setTaskStatus(9);
            log.setStatus(9);
            log.setTaskMessage(e.getMessage());
            mapper.updateLog(log);

            //3번 재시도 실패일 시 메일 발송
            if(log.getRetryCount() == 3) {
                MailLogVO mailLog = mapper.getMailLog(log.getSeq());
                mailLog.setErrMsg(e.getMessage());
                sendMail(mailLog);
            }

            throw new RuntimeException(e);
        }
        return true;
    }

    @Override
    public boolean insertInfluencerContentImage(int keywordSeq, String filePath, String channelType, boolean retryFlag, timeVO time) throws Exception {
        String taskName = null;
        switch (channelType) {
            case "T":
                taskName = "INSERT_TO_MYSQL_CHANNEL_ALL_STEP7";
                break;
            case "B":
                taskName = "INSERT_TO_MYSQL_CHANNEL_BLOG_STEP7";
                break;
            case "P":
                taskName = "INSERT_TO_MYSQL_CHANNEL_POST_STEP7";
                break;
            case "N":
                taskName = "INSERT_TO_MYSQL_CHANNEL_NTV_STEP7";
                break;
            case "Y":
                taskName = "INSERT_TO_MYSQL_CHANNEL_YOUTUBE_STEP7";
                break;
        }

        LogVO retryLog = new LogVO();
        LogVO log = new LogVO();

        //재수집 and 이전 수집때의 status가 성공이라면
        if(retryFlag) {
            // channelType별 수집의 성공 여부 체크
            // taskStatus = 1 일때는 성공했던 return "PASS"
            retryInfoVO retryInfo = new retryInfoVO();
            retryInfo.setKeywordSeq(keywordSeq);
            retryInfo.setChannelType(channelType);
            retryInfo.setTaskType(3);
            retryInfo.setTaskStep(7);
            retryInfo.setDate(time.getDate());
            retryInfo.setHh(time.getHh());

            //row가 있고 성공해서 continue 되는 경우
            if(mapper.getLog(retryInfo) != null) {
                retryLog = mapper.getLog(retryInfo);

                if(retryLog.getTaskStatus() == 1) {
                    return false;
                }
            }
        }

        //logVO 생성
        log.setKeywordSeq(keywordSeq);
        log.setChannelType(channelType);
        log.setTaskType(3);
        log.setTaskStep(7);
        log.setTaskName(taskName);
        mapper.insertLog(log);

        int minSeq = mapper.getSeq("influencer_content_image") + 1;
        try {
            //db 저장
            if(mapper.insertInfluencerContentImage(filePath)) {
                int maxSeq = mapper.getSeq("influencer_content_image");
                mapper.updateLogSeq(log.getSeq(), minSeq, maxSeq, "influencer_content_image");
            }

            //log 성공 update
            if(retryFlag) {
                if(retryLog.getDate() != null) {
                    log.setRetryCount(retryLog.getRetryCount() + 1);
                }
            }
            log.setTaskStatus(1);
            log.setStatus(1);
            mapper.updateLog(log);

        } catch (Exception e) {
            //log 실패 update
            if(retryFlag) {
                if (retryLog.getDate() != null) {
                    log.setRetryCount(retryLog.getRetryCount() + 1);

                    //3번 재시도 실패일 시 mailCount + 1
                    if(log.getRetryCount() == 3) {
                        log.setMailCount(1);
                    } else if(log.getRetryCount() > 3) {
                        log.setMailCount(retryLog.getMailCount());
                    }
                }
            }
            log.setTaskStatus(9);
            log.setStatus(9);
            log.setTaskMessage(e.getMessage());
            mapper.updateLog(log);

            //3번 재시도 실패일 시 메일 발송
            if(log.getRetryCount() == 3) {
                MailLogVO mailLog = mapper.getMailLog(log.getSeq());
                mailLog.setErrMsg(e.getMessage());
                sendMail(mailLog);
            }

            throw new RuntimeException(e);
        }
        return true;
    }

    @Override
    public boolean insertRelatedContent(int keywordSeq, String filePath, String channelType, boolean retryFlag, timeVO time) throws Exception {
        String taskName = null;
        switch (channelType) {
            case "T":
                taskName = "INSERT_TO_MYSQL_CHANNEL_ALL_STEP8";
                break;
            case "B":
                taskName = "INSERT_TO_MYSQL_CHANNEL_BLOG_STEP8";
                break;
            case "P":
                taskName = "INSERT_TO_MYSQL_CHANNEL_POST_STEP8";
                break;
            case "N":
                taskName = "INSERT_TO_MYSQL_CHANNEL_NTV_STEP8";
                break;
            case "Y":
                taskName = "INSERT_TO_MYSQL_CHANNEL_YOUTUBE_STEP8";
                break;
        }

        LogVO retryLog = new LogVO();
        LogVO log = new LogVO();

        //재수집 and 이전 수집때의 status가 성공이라면
        if(retryFlag) {
            // channelType별 수집의 성공 여부 체크
            // taskStatus = 1 일때는 성공했던 return "PASS"
            retryInfoVO retryInfo = new retryInfoVO();
            retryInfo.setKeywordSeq(keywordSeq);
            retryInfo.setChannelType(channelType);
            retryInfo.setTaskType(3);
            retryInfo.setTaskStep(8);
            retryInfo.setDate(time.getDate());
            retryInfo.setHh(time.getHh());

            //row가 있고 성공해서 continue 되는 경우
            if(mapper.getLog(retryInfo) != null) {
                retryLog = mapper.getLog(retryInfo);

                if (retryLog.getTaskStatus() == 1) {
                    return false;
                }
            }
        }

        //logVO 생성
        log.setKeywordSeq(keywordSeq);
        log.setChannelType(channelType);
        log.setTaskType(3);
        log.setTaskStep(8);
        log.setTaskName(taskName);
        mapper.insertLog(log);

        int minSeq = mapper.getSeq("related_content") + 1;
        try {
            //db 저장
            if(mapper.insertRelatedContent(filePath)) {
                int maxSeq = mapper.getSeq("related_content");
                mapper.updateLogSeq(log.getSeq(), minSeq, maxSeq, "related_content");
            }

            //log 성공 update
            if(retryFlag) {
                if(retryLog.getDate() != null) {
                    log.setRetryCount(retryLog.getRetryCount() + 1);
                }
            }
            log.setTaskStatus(1);
            log.setStatus(1);
            mapper.updateLog(log);

        } catch (Exception e) {
            //log 실패 update
            if(retryFlag) {
                if (retryLog.getDate() != null) {
                    log.setRetryCount(retryLog.getRetryCount() + 1);

                    //3번 재시도 실패일 시 mailCount + 1
                    if(log.getRetryCount() == 3) {
                        log.setMailCount(1);
                    } else if(log.getRetryCount() > 3) {
                        log.setMailCount(retryLog.getMailCount());
                    }
                }
            }
            log.setTaskStatus(9);
            log.setStatus(9);
            log.setTaskMessage(e.getMessage());
            mapper.updateLog(log);

            //3번 재시도 실패일 시 메일 발송
            if(log.getRetryCount() == 3) {
                MailLogVO mailLog = mapper.getMailLog(log.getSeq());
                mailLog.setErrMsg(e.getMessage());
                sendMail(mailLog);
            }

            throw new RuntimeException(e);
        }
        return true;
    }

    @Override
    public int confirmFirst(String date, String hh) {
        return mapper.confirmFirst(date, hh);
    }

    @Override
    public timeVO getTime() {
        return mapper.getTime();
    }

    @Override
    public void todayTarget0(String date) {
        mapper.todayTarget0(date);
    }

    @Override
    public void todayTarget1(String date) {
        mapper.todayTarget1(date);
    }

    @Override
    public void failAlimTalk(timeVO time, String datetime) {
//        if(mapper.failKeyword(time) != null) {
//
//        }
        List<Integer> successList = mapper.failKeyword(time);
        List<KeywordVO> keywordList = mapper.getKeyword();
        List<String> failList = new ArrayList<>();
        for (KeywordVO keyword : keywordList) {
            if(!successList.contains(keyword.getSeq())) {
                failList.add(keyword.getKeyword());
            }
        }

        if(successList.size() != keywordList.size()) {
            logger.info("성공한 수 " + successList.size());
            logger.info("대상 수 " + keywordList.size());
            logger.info("수집 실패 있다");
            sendTalk(datetime, keywordList, failList);
        } else {
            logger.info("수집 실패 없다");
        }
    }
}
