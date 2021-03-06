package com.influencer.service;

import au.com.bytecode.opencsv.CSVWriter;
import com.influencer.HttpPostMultipart;
import com.influencer.vo.ContentAnalysisIdsVO;
import com.influencer.vo.KeywordInfoDetailVO;
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
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

@Service
public class MorphemeServiceImpl implements MorphemeService{

    private final Logger logger = LoggerFactory.getLogger(MorphemeServiceImpl.class);

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

    //content ??????, ?????? get
    @Override
    public List<ContentAnalysisIdsVO> getContent() {
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter2 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String formatedNow2 = now.format(formatter2);

        List<ContentAnalysisIdsVO> contentList = new ArrayList<>();

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
        conNum.forEach(i -> {
            if(conAnaNum.contains(i)) {
                realConNum.remove(i);
            }
        });

        //????????? content??? id??? title & desc get
        BasicDBObject query = new BasicDBObject();
        query.put("content_id", new BasicDBObject("$in", realConNum));
        MongoCursor<Document> realConCursor = conCollection.find(query).cursor();

        //title??? desc ????????? txt ?????? ??????
        while(realConCursor.hasNext()) {
            ContentAnalysisIdsVO content = new ContentAnalysisIdsVO();

            Document doc = realConCursor.next();
            String keywordSeq = doc.get("keyword_seq").toString();
            String contentId = doc.get("content_id").toString();
            String text = doc.get("content_title") + " " + doc.get("content_desc");

            content.setKeywordSeq(Integer.parseInt(keywordSeq));
            content.setContentId(contentId);
            content.setRegdate(formatedNow2);

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

                contentList.add(content);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        //MONGODB ?????? ??????
//        mongo.close();

        //contentIdList return
        return contentList;
    }

    //????????? ?????? ?????? api ??????
    @Override
    public List<ContentAnalysisIdsVO> analysisStart(List<ContentAnalysisIdsVO> contentList, String formatedNow) throws IOException {
        List<ContentAnalysisIdsVO> idsList = new ArrayList<>();

        //?????? txt ????????? ???????????? ????????? ?????? api ??????
        for (ContentAnalysisIdsVO content : contentList) {

            int keywordSeq = content.getKeywordSeq();
            String contentId = content.getContentId();

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
                content.setInsertId(insertId);

                idsList.add(content);

            } catch (Exception e) {
                logger.error(e.getMessage());
                continue;
            }
        }

        //influencer_content_analysis_ids??? ???????????? ?????? csv ?????? ??????
        String csvFilePath = FILEPATHCSV + formatedNow + "_" + TABLEIDS + CSV;
        try {
            CSVWriter cw = new CSVWriter(new OutputStreamWriter(new FileOutputStream(csvFilePath), StandardCharsets.UTF_8),',', CSVWriter.NO_QUOTE_CHARACTER);
            try (cw) {
                cw.writeNext(new String[]{"\ufeff"});
                cw.writeNext(new String[]{"keyword_seq.int32()", "content_id.string()", "insert_id.string()", "regdate.string()"});
                for (ContentAnalysisIdsVO ids : idsList) {
                    cw.writeNext(new String[]{String.valueOf(ids.getKeywordSeq()), ids.getContentId(), ids.getInsertId(), String.valueOf(ids.getRegdate())});
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

        return idsList;
    }

    //????????? ?????? ?????? ??????
    @Override
    public void analysisStatus(List<ContentAnalysisIdsVO> contentList, String formatedNow) {
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter2 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String formatedNow2 = now.format(formatter2);

        List<MorphemeAnalysisVO> list = new ArrayList<>();

        for (ContentAnalysisIdsVO content : contentList) {
            String insertId = content.getInsertId();
            String contentId = content.getContentId();
            int keywordSeq = content.getKeywordSeq();

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

                //summary link get
                String link = obj.get("summary").toString();

                //???????????? URL
                URI url2 = URI.create(link);
                RestTemplate rt = new RestTemplate();
                ResponseEntity<byte[]> res = rt.getForEntity(url2, byte[].class);
                byte[] buffer = res.getBody();

                //???????????? path
                String fileName = FILEPATHTXT + contentId + "_" + insertId + "_summary" + TXT;
                Path target = Paths.get(fileName);
                try {
                    assert buffer != null;
                    FileCopyUtils.copy(buffer, target.toFile());
                } catch (Exception e) {
                    e.printStackTrace();
                }

                //??????????????? txt?????? ??????
                String morphemeAnalysis = "";
                BufferedReader reader = new BufferedReader(new FileReader(fileName, StandardCharsets.UTF_8));
                String str;
                while ((str = reader.readLine()) != null) {
                    morphemeAnalysis += str;
                }
                reader.close();

                System.out.println(morphemeAnalysis);

                //json ??????
                JSONArray array1 = (JSONArray) parser.parse(morphemeAnalysis);
                for (int i=0; i< array1.size(); i++) {
                    MorphemeAnalysisVO analysis = new MorphemeAnalysisVO();

                    JSONObject object = (JSONObject) array1.get(i);
                    analysis.setKeywordSeq(keywordSeq);
                    analysis.setContentId(contentId);
                    analysis.setContentWord(object.get("result").toString());
                    analysis.setWordClass(object.get("word_class").toString());
                    analysis.setCount(Integer.parseInt(object.get("cnt").toString()));
                    analysis.setRegdate(formatedNow2);

                    list.add(analysis);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        //influencer_content_analysis_info??? ???????????? ?????? csv ?????? ??????
        String csvFilePath = FILEPATHCSV + formatedNow + "_" + TABLEINFO + CSV;
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
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        //command ????????? ?????? ??? csv import
        String command = MONGOIMPORT + HOST + " --port " +  PORT  + " --db " + DB + " --collection " + TABLEINFO
                + " --username " + USERNAME + " --password " + PASSWORD
                + " --columnsHaveTypes --headerline --type csv --file "+ csvFilePath;
        try {
            Process process = Runtime.getRuntime().exec(command);
            logger.info("analysis DB ?????? ??????");
        } catch (Exception e) {
            logger.error("analysis DB ?????? ??????");
            e.printStackTrace();
        }
    }
}
