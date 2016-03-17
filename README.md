# jpgAgent
jpgAgent is a job scheduler for PostgreSQL written in Java. It is low overhead, and aims to be fully
compatible with pgAgent.

The reason for writing a pgAgent compatible job scheduler is to be able to utilize the tools already in place
for pgAgent in the pgAdmin UI, minimizing the pain of switching for existing pgAgent users (uses the same database schema),
and provide a more stable and feature rich implementation of the agent.


## Requires:
jpgAgent requires Java 8+ and PostgreSQL 9.2+

## Additional features:
Kill a running job:

    NOTIFY jpgagent_kill_job, 'job_id_here';

## Config options:
     --port N : Database host port. (default: 5432)
     -d VAL   : jpgAgent database.
     -h VAL   : Database host address.
     -p VAL   : Database password.
     -r N     : Connection retry interval (ms). (default: 30000)
     -t N     : Job poll interval (ms). (default: 10000)
     -u VAL   : Database user.

Example run:

        java -server -jar /path/to/jar/jpgagent-1.0-SNAPSHOT-jar-with-dependencies.jar -h 127.0.0.1 -u test -p password -d postgres