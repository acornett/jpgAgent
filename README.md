#jpgAgent
========

jpgAgent is a job scheduler for PostgreSQL written in Java. It is low overhead, and aims to be fully
compatible with pgAgent.

The reason for writing a pgAgent compatible job scheduler is to be able to utilize the tools already in place
for pgAgent in the pgAdmin UI, minimizing the pain of switching for existing pgAgent users (uses the same database schema),
and provide a more stable and feature rich implementation of the agent.

##Additional features:
Kill a running job:
    NOTIFY jpgagent_kill_job, 'job_id_here';"# jpgAgent" 
