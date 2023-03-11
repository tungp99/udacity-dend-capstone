# THE CAPSTONE

### Everything was written in notebook.ipynb, I'll only put steps to run my pipeline here

```sh
# due to python's importing issue, you must change working directory to src/
$ cd src

$ pwd
.../the-capstone/src

$ ls -l
total 60K
-rw-r--r-- 1 <redacted> <redacted>    0 Jun 19 <redacted> __init__.py
-rw-r--r-- 1 <redacted> <redacted>  478 Jun 19 <redacted> config.ini
-rw-r--r-- 1 <redacted> <redacted> 1.8K Jun 19 <redacted> main.py
-rw-r--r-- 1 <redacted> <redacted>  44K Jun 19 <redacted> notebook.ipynb
drwxr-xr-x 3 <redacted> <redacted> 4.0K Jun 19 <redacted> pipelines

$ poetry run python main.py
22/06/19 <redacted> WARN Utils: Your hostname, <redacted> resolves to a loopback address: <redacted>; using <redacted> instead (on interface <redacted>)
22/06/19 <redacted> WARN Utils: Set SPARK_LOCAL_IP if you need to bind to another address
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
22/06/19 <redacted> WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
INFO:udacity_capstone.pipelines:saving data...
INFO:udacity_capstone.pipelines:saving data...
22/06/19 <redacted> WARN package: Truncated the string representation of a plan since it was too large. This behavior can be adjusted by setting 'spark.sql.debug.maxToStringFields'.
INFO:udacity_capstone.pipelines:saving data...
INFO:udacity_capstone.pipelines:saving data...
INFO:udacity_capstone.pipelines.pipeline_dim_temperatures_test:check passed!
INFO:udacity_capstone.pipelines.pipeline_dim_temperatures_test:check passed!
INFO:udacity_capstone.pipelines.pipeline_dim_temperatures_test:check passed!
INFO:udacity_capstone.pipelines.pipeline_dim_demographics_test:check passed!
INFO:udacity_capstone.pipelines.pipeline_dim_demographics_test:check passed!
INFO:udacity_capstone.pipelines.pipeline_dim_demographics_test:check passed!
INFO:udacity_capstone.pipelines.pipeline_dim_demographics_test:check passed!
INFO:udacity_capstone.pipelines.pipeline_dim_demographics_test:check passed!
INFO:udacity_capstone.pipelines.pipeline_dim_immigrations_test:check passed!
INFO:udacity_capstone.pipelines.pipeline_dim_immigrations_test:check passed!
INFO:udacity_capstone.pipelines.pipeline_dim_immigrations_test:check passed!
INFO:udacity_capstone.pipelines.pipeline_fact_immigrations_test:check passed!
INFO:udacity_capstone.pipelines.pipeline_fact_immigrations_test:check passed!
INFO:udacity_capstone.pipelines.pipeline_fact_immigrations_test:check passed!
```

### Project Structure

- the-capstone/in/ : input data, you will need all of these files
- the-capstone/out/ : output data, you can see after running the pipelines
- the-capstone/src/ : source code
- the-capstone/src/pipelines : my custom designed pipelines with tests
