package com.ph.dl;

import com.ph.dl.service.BankTransactionsLoader;
import com.ph.dl.service.IncomeLoader;
import com.ph.dl.service.VacancyLossService;
import com.ph.dl.service.InternetUtilityProcessor;
import com.ph.dl.config.AppConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class Application {
	
	static Logger logger = LoggerFactory.getLogger(Application.class);
	
    public static void main(String[] args) throws Exception {
        AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext(AppConfig.class);
        try {
            IncomeLoader loader = ctx.getBean(IncomeLoader.class);
            loader.loadOnStartup();
            logger.info("Income CSV loaded successfully.");

            BankTransactionsLoader bankLoader = ctx.getBean(BankTransactionsLoader.class);
            //bankLoader.loadOnStartup();
            logger.info("Bank transactions CSV loaded successfully.");

            InternetUtilityProcessor internetUtilityProcessor = ctx.getBean(InternetUtilityProcessor.class);
            internetUtilityProcessor.loadOnStartup();
            logger.info("Internet utility expenses processed successfully.");

            VacancyLossService vacancyService = ctx.getBean(VacancyLossService.class);
            Integer totalVacancies = vacancyService.generateForYear();
            logger.info("Total vacancy loss rows generated: {}", totalVacancies);
        } finally {
            ctx.close();
        }
    }
}