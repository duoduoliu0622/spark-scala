package com.lkl.poc;

/**
 * Created by kelinliu on 2017-06-29.
 */
public class HelloWorld {
    private String message;

    public void setMessage(String message){
        this.message  = message;
    }
    public void getMessage(){
        System.out.println("Your Message : " + message);
    }
}