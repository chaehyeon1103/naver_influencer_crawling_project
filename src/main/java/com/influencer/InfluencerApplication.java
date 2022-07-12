package com.influencer;

import com.influencer.service.MongoDBUtil;
import com.influencer.vo.ContentAnalysisIdsVO;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

@EnableScheduling
@SpringBootApplication
public class InfluencerApplication implements CommandLineRunner {

	public static void main(String[] args) {
		SpringApplication.run(InfluencerApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {

	}
}
