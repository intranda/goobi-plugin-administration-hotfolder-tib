package de.intranda.goobi.plugins.tibhotfolder;

import java.util.Calendar;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;

import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.Trigger;
import org.quartz.TriggerUtils;
import org.quartz.impl.StdSchedulerFactory;

import lombok.extern.log4j.Log4j;

@WebListener
@Log4j
public class QuartzListener implements ServletContextListener {

    @Override
    public void contextDestroyed(ServletContextEvent arg0) {
        // stop the catalogue poller job
        try {
            SchedulerFactory schedFact = new StdSchedulerFactory();
            Scheduler sched = schedFact.getScheduler();
            sched.deleteJob("tib-hotfolder", "Goobi Admin Plugin");
            log.debug("Job 'tib-hotfolder' stopped");
        } catch (SchedulerException e) {
            log.error("Error while stopping the job", e);
        }
    }

    @Override
    public void contextInitialized(ServletContextEvent arg0) {
        System.out.println("registering hotfolder job for table of contents");
        log.info("registering hotfolder job for table of contents");
        try {
            // get default scheduler
            SchedulerFactory schedFact = new StdSchedulerFactory();
            Scheduler sched = schedFact.getScheduler();

            // configure time to start 
            java.util.Calendar startTime = java.util.Calendar.getInstance();
            startTime.add(Calendar.MINUTE, 1);

            // create new job 
            JobDetail jobDetail = new JobDetail("tib-hotfolder", "Goobi Admin Plugin", QuartzHotfolderJob.class);
            Trigger trigger = TriggerUtils.makeMinutelyTrigger(1);
            trigger.setName("tib-hotfolder");
            trigger.setStartTime(startTime.getTime());

            // register job and trigger at scheduler
            sched.scheduleJob(jobDetail, trigger);
        } catch (SchedulerException e) {
            log.error("Error while executing the scheduler", e);
        }

    }

}
