package com.lkl.poc;

import com.amazonaws.services.lambda.runtime.Context;
import com.lkl.poc.services.PollyService;
import com.lkl.poc.services.S3Service;
import javazoom.jl.decoder.JavaLayerException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.IOException;

public class App {
    public static void main(String[] args) {
        ApplicationContext appContext = new ClassPathXmlApplicationContext("bean-common.xml");

        TextEditor textEditor = (TextEditor) appContext.getBean("textEditor");
        textEditor.spellCheck();

        appContext = null;
    }

    public void textToSpeech(String text, Context context) {
        ApplicationContext appContext = new ClassPathXmlApplicationContext("bean-common.xml");

        S3Service s3Service = (S3Service) appContext.getBean("s3Service");
        s3Service.uploadText("kelintest", "hello_spring.txt", text);

        appContext = null;
    }

    public void textToVoice(String text, Context context) throws IOException, JavaLayerException {
        ApplicationContext appContext = new ClassPathXmlApplicationContext("bean-common.xml");

        PollyService pollyService = (PollyService) appContext.getBean("pollyService");

        S3Service s3Service = (S3Service) appContext.getBean("s3Service");
        s3Service.uploadStream("kelintest", "hello_spring.mp3", pollyService.toVoice(text));

        appContext = null;
    }
}
