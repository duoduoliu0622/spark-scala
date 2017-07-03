package com.lkl.poc;

import com.amazonaws.services.lambda.runtime.Context;
import com.lkl.poc.services.S3Service;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class App
{
    public static void main( String[] args )
    {
        ApplicationContext appContext = new ClassPathXmlApplicationContext("bean-common.xml");

        TextEditor textEditor = (TextEditor) appContext.getBean("textEditor");
        textEditor.spellCheck();

        appContext = null;
    }

    public void textToSpeech(String text, Context context)
    {
        ApplicationContext appContext = new ClassPathXmlApplicationContext("bean-common.xml");

        S3Service s3Service = (S3Service) appContext.getBean("s3Service");
        s3Service.upload("kelintest", "hello_spring.txt", text);

        appContext = null;
    }
}
