package com.influencer.service;

import au.com.bytecode.opencsv.CSVWriter;
import com.influencer.HttpPostMultipart;
import com.influencer.vo.ContentAnalysisIdsVO;
import com.influencer.vo.MorphemeAnalysisVO;
import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.util.FileCopyUtils;
import org.springframework.web.client.RestTemplate;

import java.io.*;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

@Service
public class AnalysisServiceImpl implements AnalysisService{

    private final Logger logger = LoggerFactory.getLogger(AnalysisServiceImpl.class);

    public static String URL = "https://nlp.artistchai.co.kr/";
    public static String FILEPATHTXT = "D:/influencerMorphemeTxt/";
    public static String TXT = ".txt";

    public static String FILEPATHCSV = "D:/influencerMorphemeCsv/";
    public static String TABLEIDS = "influencer_content_analysis_ids";
    public static String TABLEINFO = "influencer_content_analysis_info";
    public static String TABLECONTENT = "influencer_content_info";
    public static String CSV = ".csv";

    public static String MONGOIMPORT = "C:/Program Files/MongoDB/Tools/100/bin/mongoimport.exe --host ";
    public static String HOST = "61.97.191.52";
    public static String PORT = "27017";
    public static String DB = "influencer";
    public static String USERNAME = "curadar_influencer";
    public static String PASSWORD = "ckdl070%";

    //????????? ?????? ??????
    @Override
    public void analysisStart(String nowdate) {
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String regdate = now.format(formatter);

        List<ContentAnalysisIdsVO> idsList = new ArrayList<>();

        //MONGO DB ??????
        MongoClient mongo = MongoDBUtil.createMongo;
        //DATABASE GET
        MongoDatabase db = mongo.getDatabase("influencer");
        //COLLECTION GET
        MongoCollection<Document> conCollection = db.getCollection(TABLECONTENT);
        MongoCollection<Document> conAnaCollection = db.getCollection(TABLEIDS);

        //?????? ????????? content??? ??????------------------------------------------------
        //influencer_content_info?????? content_id get
        List<String> conNum = new ArrayList<>();
        List<String> realConNum = new ArrayList<>();
        MongoCursor<Document> conCursor = conCollection.find().cursor();
        while(conCursor.hasNext()) {
            Document doc = conCursor.next();
            conNum.add(doc.get("content_id").toString());
            realConNum.add(doc.get("content_id").toString());
        }

        //influencer_content_analysis_ids ?????? content_id get
        List<String> conAnaNum = new ArrayList<>();
        MongoCursor<Document> conAnaCursor = conAnaCollection.find().cursor();
        while(conAnaCursor.hasNext()) {
            Document doc = conAnaCursor.next();
            conAnaNum.add(doc.get("content_id").toString());
        }

        //?????? ????????? content_id??? list?????? ??????
        for (String id : conNum) {
            if(conAnaNum.contains(id)) {
                realConNum.remove(id);
            }
        }

        //txt ?????? ??????-----------------------------------------------------------
        //????????? content??? id??? title & desc get
        BasicDBObject query = new BasicDBObject();
        query.put("content_id", new BasicDBObject("$in", realConNum));
        MongoCursor<Document> realConCursor = conCollection.find(query).cursor();

        //title??? desc ????????? txt ?????? ??????
        while(realConCursor.hasNext()) {
            ContentAnalysisIdsVO ids = new ContentAnalysisIdsVO();

            Document doc = realConCursor.next();
            String keywordSeq = doc.get("keyword_seq").toString();
            String contentId = doc.get("content_id").toString();
            String text = doc.get("content_title") + " " + doc.get("content_desc");

            ids.setKeywordSeq(Integer.parseInt(keywordSeq));
            ids.setContentId(contentId);
            ids.setRegdate(regdate);

            try {
                String filePath = FILEPATHTXT + contentId + TXT;
                File file = new File(filePath);

                if(!file.exists()) {
                    file.createNewFile();
                }

                FileWriter fw = new FileWriter(file);
                BufferedWriter writer = new BufferedWriter(fw);

                writer.write(text);
                writer.close();

            } catch (IOException e) {
                logger.error(e.getMessage());
                continue;
            }

            try {
                HttpPostMultipart multipart = new HttpPostMultipart(URL + "file", "utf-8");

                //string data ?????? ??????
                multipart.addFormField("method", "1");
                multipart.addFormField("site", "curadar");
                multipart.addFormField("id", contentId);

                //file data ?????? ??????
                multipart.addFilePart("userfile", new File(FILEPATHTXT + contentId + TXT));

                //response??? ??????
                String response = multipart.finish();

                //json ??????
                JSONParser parser = new JSONParser();
                JSONObject obj = (JSONObject) parser.parse(response);

                //insertId ??????
                String insertId = obj.get("insertId").toString();
                ids.setInsertId(insertId);

                //status ??????
                ids.setApiStatus("2");

                idsList.add(ids);

            } catch (Exception e) {
                logger.error(e.getMessage());
                continue;
            }
        }

        //influencer_content_analysis_ids??? ???????????? ?????? csv ?????? ?????? ---------------------------------
        String csvFilePath = FILEPATHCSV + nowdate + "_" + TABLEIDS + CSV;
        if (idsList.size() > 0) {
            try {
                CSVWriter cw = new CSVWriter(new OutputStreamWriter(new FileOutputStream(csvFilePath), StandardCharsets.UTF_8),',', CSVWriter.NO_QUOTE_CHARACTER);
                try (cw) {
                    cw.writeNext(new String[]{"\ufeff"});
                    cw.writeNext(new String[]{"keyword_seq.int32()", "content_id.string()", "insert_id.string()", "api_status.string()", "regdate.string()"});
                    for (ContentAnalysisIdsVO ids : idsList) {
                        cw.writeNext(new String[]{String.valueOf(ids.getKeywordSeq()), ids.getContentId(), ids.getInsertId(), ids.getApiStatus(), ids.getRegdate()});
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            //command ????????? ?????? ??? csv import
            String command = MONGOIMPORT + HOST + " --port " +  PORT  + " --db " + DB + " --collection " + TABLEIDS
                    + " --username " + USERNAME + " --password " + PASSWORD
                    + " --columnsHaveTypes --headerline --type csv --file "+ csvFilePath;
            try {
                Process process = Runtime.getRuntime().exec(command);
                logger.info("ids DB ?????? ??????");
            } catch (Exception e) {
                logger.error("ids DB ?????? ??????");
                e.printStackTrace();
            }
        } else {
            logger.info("????????? ids ???????????? ????????????.");
        }
    }

    //status ?????? ??????
    @Override
    public void statusCheck() {
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String successDate = now.format(formatter);

        List<MorphemeAnalysisVO> list = new ArrayList<>();

        //MONGO DB ??????
        MongoClient mongo = MongoDBUtil.createMongo;
        //DATABASE GET
        MongoDatabase db = mongo.getDatabase("influencer");
        //COLLECTION GET
        MongoCollection<Document> conAnaCollection = db.getCollection(TABLEIDS);

        //api_status??? 2(?????? ?????????)??? ??????????????? ??????
        BasicDBObject query = new BasicDBObject();
        query.put("api_status", "2");
        MongoCursor<Document> conCursor = conAnaCollection.find(query).cursor();
        while(conCursor.hasNext()) {
            Document doc = conCursor.next();

            String insertId = doc.get("insert_id").toString();
            String line = null;
            String uri = URL + "status/" + insertId;

            try {
                //uri ??????
                java.net.URL url = new URL(uri);
                URLConnection conn = url.openConnection();

                BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8));

                line = br.readLine();
                JSONParser parser = new JSONParser();
                JSONArray array = (JSONArray) parser.parse(line);
                JSONObject obj = (JSONObject) array.get(0);

                //status ??? ?????? ??? 0(?????? ???), 1(?????? ?????????)?????? ??????
                //2(?????? ??????)?????? ????????? ??????
                String status = obj.get("status").toString();
                if(status.equals("2")) {
                    //summary link get
                    String link = obj.get("summary").toString();

                    //????????? ?????? ??????
                    Document query2 = new Document();
                    query2.append("insert_id", insertId);
                    //update??? ??????
                    Document setData = new Document();
                    setData.append("download_Url", link).append("api_status", "1")
                            .append("api_success_date", successDate).append("insert_status", "2");
                    //update
                    Document update = new Document();
                    update.append("$set", setData);

                    conAnaCollection.updateOne(query2, update);
                }
            } catch (Exception e) {
                logger.error(e.getMessage());
                continue;
            }
        }
    }

    //?????? ????????? ????????? ?????? DB ??????
    @Override
    public void analysisDataInsert(String nowdate) throws IOException, ParseException {
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String successDate = now.format(formatter);

        List<MorphemeAnalysisVO> list = new ArrayList<>();

        //MONGO DB ??????
        MongoClient mongo = MongoDBUtil.createMongo;
        //DATABASE GET
        MongoDatabase db = mongo.getDatabase("influencer");
        //COLLECTION GET
        MongoCollection<Document> conAnaCollection = db.getCollection(TABLEIDS);

        //insert_status??? 2(?????? ???????????? ??? ?????????)??? ??????????????? ??????
        BasicDBObject query = new BasicDBObject();
        query.put("insert_status", "2");
        MongoCursor<Document> conCursor = conAnaCollection.find(query).cursor();
        while (conCursor.hasNext()) {
            try {
                Document doc = conCursor.next();

                int keywordSeq = Integer.parseInt(doc.get("keyword_seq").toString());
                String contentId = doc.get("content_id").toString();
                String insertId = doc.get("insert_id").toString();
                String downloadUrl = doc.get("download_Url").toString();

                //???????????? URL
                URI url = URI.create(downloadUrl);
                RestTemplate rt = new RestTemplate();
                ResponseEntity<byte[]> res = rt.getForEntity(url, byte[].class);
                byte[] buffer = res.getBody();

                //???????????? path
                String fileName = FILEPATHTXT + contentId + "_" + insertId + "_summary" + TXT;
                Path target = Paths.get(fileName);
                try {
                    assert buffer != null;
                    FileCopyUtils.copy(buffer, target.toFile());
                } catch (Exception e) {
                    e.printStackTrace();
                    continue;
                }

                //??????????????? txt?????? ??????
                String morphemeAnalysis = "";
                BufferedReader reader = new BufferedReader(new FileReader(fileName, StandardCharsets.UTF_8));
                String str;
                while ((str = reader.readLine()) != null) {
                    morphemeAnalysis += str;
                }
                reader.close();

                //json ??????
                JSONParser parser = new JSONParser();
                JSONArray array = (JSONArray) parser.parse(morphemeAnalysis);
                for (int i=0; i< array.size(); i++) {
                    MorphemeAnalysisVO analysis = new MorphemeAnalysisVO();

                    JSONObject object = (JSONObject) array.get(i);
                    analysis.setKeywordSeq(keywordSeq);
                    analysis.setContentId(contentId);
                    analysis.setContentWord(object.get("result").toString());
                    analysis.setWordClass(object.get("word_class").toString());
                    analysis.setCount(Integer.parseInt(object.get("cnt").toString()));
                    analysis.setRegdate(successDate);

                    list.add(analysis);
                }
            } catch (Exception e) {
                logger.error(e.getMessage());
                continue;
            }
        }

        //influencer_content_analysis_info??? ???????????? ?????? csv ?????? ??????
        String csvFilePath = FILEPATHCSV + nowdate + "_" + TABLEINFO + CSV;
        if(list.size() > 0) {
            try {
                CSVWriter cw = new CSVWriter(new OutputStreamWriter(new FileOutputStream(csvFilePath), StandardCharsets.UTF_8),',', CSVWriter.NO_QUOTE_CHARACTER);
                try (cw) {
                    cw.writeNext(new String[]{"\ufeff"});
                    cw.writeNext(new String[]{"keyword_seq.int32()", "content_id.string()", "content_word.string()", "word_class.string()", "count.int32()", "regdate.string()"});
                    for (MorphemeAnalysisVO analysis : list) {
                        cw.writeNext(new String[]{String.valueOf(analysis.getKeywordSeq()), analysis.getContentId(), analysis.getContentWord(),
                                analysis.getWordClass(), String.valueOf(analysis.getCount()), String.valueOf(analysis.getRegdate())});
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            //command ????????? ?????? ??? csv import
            String command = MONGOIMPORT + HOST + " --port " +  PORT  + " --db " + DB + " --collection " + TABLEINFO
                    + " --username " + USERNAME + " --password " + PASSWORD
                    + " --columnsHaveTypes --headerline --type csv --file "+ csvFilePath;
            try {
                Process process = Runtime.getRuntime().exec(command);
                logger.info("analysis DB ?????? ??????");

                for (MorphemeAnalysisVO analysis : list) {
                    //????????? ?????? ??????
                    Document query2 = new Document();
                    query2.append("content_id", analysis.getContentId());
                    //update??? ??????
                    Document setData = new Document();
                    setData.append("insert_status", "1").append("insert_success_date", successDate);
                    //update
                    Document update = new Document();
                    update.append("$set", setData);

                    conAnaCollection.updateOne(query2, update);
                }
            } catch (Exception e) {
                logger.error("analysis DB ?????? ??????");
                e.printStackTrace();
            }
        } else {
            logger.info("????????? analysis_info ???????????? ????????????.");
        }
    }
}
