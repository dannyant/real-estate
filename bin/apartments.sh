spark-submit --master yarn --queue OurQueue --py-files modules.zip
--conf "spark.pyspark.driver.python=/usr/lib/python3.8/bin/python3"
--conf "spark.pyspark.python=/usr/lib/python3.8/bin/python3"
pullhttp/pull_apartments.py