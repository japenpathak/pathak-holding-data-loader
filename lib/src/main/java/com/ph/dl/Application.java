package com.ph.dl;

import com.ph.dl.service.IncomeLoader;
import com.ph.dl.config.AppConfig;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class Application {
    public static void main(String[] args) throws Exception {
        AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext(AppConfig.class);
        IncomeLoader loader = ctx.getBean(IncomeLoader.class);
        loader.loadOnStartup();
        ctx.close();
        System.out.println("Income CSV loaded successfully.");
    }
}