# jpgAgent
jpgAgent is a job scheduler for PostgreSQL written in Java. It is low overhead, and aims to be fully
compatible with pgAgent.

The reason for writing a pgAgent compatible job scheduler is to be able to utilize the tools already in place
for pgAgent in the pgAdmin UI, minimizing the pain of switching for existing pgAgent users (uses the same database schema),
and provide a more stable and feature rich implementation of the agent.


## Requires:
jpgAgent requires Java 8+ and PostgreSQL 9.2+

## Additional features:
### Kill a running job:
We support killing a job through Listen/Notify channels in Postgres.  It was implemented this way to be the easiest
to use without extending the UI to support it.

    NOTIFY jpgagent_kill_job, 'job_id_here';

### Annotations:
Annotations can be added at the job, and job step level.  There is currently one supported at the job level, and 
two at the job step level.

Annotations are added in the job comment field, or job step description field, must be on their own line, and in the correct format.

Annotations that take a time measurement support different suffixes for the value (ms, s, m, h).

#### Job:

    @JOB_TIMEOUT=30 s;

Definitions:
    
    @JOB_TIMEOUT If the job takes longer than specified to complete, the job will abort, and abort all 
    steps that have not completed yet. The steps that did complete are not affected.
    
#### Job Step:
    
    @JOB_STEP_TIMEOUT=5 s;
    @RUN_IN_PARALLEL=true;
    @DATABASE_LOGIN=username;
    @DATABASE_PASSWORD=securepass;
    
Definitions:

    @RUN_IN_PARALLEL This annotation allows the step it's defined on to run in parallel with the 
    previous step (regardless of the annotations on the previous step).  You can set up some somewhat intricate job 
    flows with this.
    
    @JOB_STEP_TIMEOUT If the step takes longer than specified to complete, the step will abort leaving
    the rest of the job to finish normally.
    
    @DATABASE_LOGIN If specified, use this database login to connect instead of the connection info specified for jpgAdmin. Must be specified with @DATABASE_PASSWORD.
    
    @DATABASE_PASSWORD If specified, use this database password to connect instead of the connection info specified for jpgAdmin. Must be specified with @DATABASE_LOGIN.
   
    

## Config options:
     --help         : Help (default: false)
     --version      : Version (default: false)
     --port Integer : Database host port. (default: 5432)
     -d String      : jpgAgent database.
     -h String      : Database host address.
     -p String      : Database password.
     -r Integer     : Connection retry interval (ms). (default: 30000)
     -t Integer     : Job poll interval (ms). (default: 10000)
     -u String      : Database user.
     -w Integer     : Size of the thread pool to execute tasks.  Each job and job step can take up to a thread in the pool at once. (default: 40)
     
### Arguments file:
You can create a file which contains your arguments, and pass that into the program instead.  This will protect the password from showing up in logs.
The file can be created anywhere on your filesystem, and must contain the arguments in this format:

        -d=postgres
        -h=127.0.0.1
        -u=test
        -p=password

## Example run:

        java -server -jar /path/to/jar/jpgAgent.jar -d postgres -h 127.0.0.1 -u test -p password 
or        

        java -server -jar /path/to/jar/jpgAgent.jar @/usr/jpgagent/args