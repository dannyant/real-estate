sudo -u spark spark-submit --deploy-mode client  --verbose --py-files pullhttp/base_http_pull.py --master yarn   pullhttp/pull_apartments.py

