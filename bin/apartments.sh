sudo --preserve-env -u spark spark-submit --deploy-mode cluster --jars /opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p0.15945976/jars/phoenix5-spark-shaded-6.0.0.7.1.7.0-551.jar  --verbose --py-files pullhttp/base_http_pull.py --master yarn   pullhttp/pull_apartments.py