import traceback
from datetime import datetime

from pyspark.sql.functions import udf, collect_list, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
from pyspark.sql import SparkSession


schema_madr = StructType([
    StructField("CNTL-BYTE", StringType()),
    StructField("VOL", StringType()),
    StructField("KEY", StringType()),
    StructField("BLOCK", StringType()),
    StructField("BLOCK-N", StringType()),
    StructField("BLOCK-S", StringType()),
    StructField("LOT", StringType()),
    StructField("LOT-N", StringType()),
    StructField("LOT-S", StringType()),
    StructField("FILLER1", StringType()),
    StructField("SOURCE", StringType()),
    StructField("ADDRESS", StringType()),
    StructField("ADRS1", StringType()),
    StructField("ADRS2", StringType()),
    StructField("ADRS3", StringType()),
    StructField("ADRS4", StringType()),
    StructField("ZIP", StringType()),
    StructField("UPD-DATE", StringType()),
    StructField("U-DT-MM", StringType()),
    StructField("U-DT-DD", StringType()),
    StructField("U-DT-YY", StringType()),
    StructField("TRAN-TYPE", StringType())
])




schema_ownr = StructType([
    StructField("OWNR - CTL - BYTE", StringType()),
    StructField("OWNR - VOL", StringType()),
    StructField("OWNR - KEY", StringType()),
    StructField("OWNER - BLOCK", StringType()),
    StructField("OWNR - BLK - N", StringType()),
    StructField("OWNR - BLK - S", StringType()),
    StructField("OWNR - LOT", StringType()),
    StructField("OWNR - LOT - N", StringType()),
    StructField("OWNR - LOT - S", StringType()),
    StructField("OWNR - SEQUENCE", StringType()),
    StructField("OWNR - OWNER - NAME", StringType()),
    StructField("OWNR - ENTRY - DATE", StringType()),
    StructField("OWNR - ENTRY - YY", StringType()),
    StructField("OWNR - ENTRY - MM", StringType()),
    StructField("OWNR - ENTRY - DD", StringType()),
    StructField("OWNR - LOT - STATUS", StringType()),
    StructField("OWNR - LONG - NM - CD", StringType()),
    StructField("OWNR - NEW - LOT - CD", StringType()),
    StructField("OWNR - RECRDRS - NO", StringType()),
    StructField("OWNR - RECRDRS - BK", StringType()),
    StructField("OWNR - RECRDRS - PG", StringType()),
    StructField("OWNR - RCRDR - PG - R", StringType()),
    StructField("OWNR - TRANS - TYPE", StringType()),
    StructField("OWNR - SIGND - DATE", StringType()),
    StructField("OWNR - UPDAT - DATE", StringType())
])



schema_secd = StructType([
    StructField("CTLBYTE", StringType()),
    StructField("RP1VOLUME", StringType()),
    StructField("RP1PRCLID", StringType()),
    StructField("SITUS", StringType()),
    StructField("ROLLYEAR", StringType()),
    StructField("RP1STACDE", StringType()),
    StructField("RP1LNDVAL", StringType()),
    StructField("RP1IMPVAL", StringType()),
    StructField("RP1FXTVAL", StringType()),
    StructField("RP1PPTVAL", StringType()),
    StructField("RP1EXMVL1", StringType()),
    StructField("RP1EXMVL2", StringType()),
    StructField("EXEMPTYPE", StringType()),
    StructField("WORKYEAR", StringType()),
    StructField("WORKSTATUS", StringType()),
    StructField("WORKFVLAND", StringType()),
    StructField("WORKFVSTR", StringType()),
    StructField("WORKFVFIXT", StringType()),
    StructField("WORKFVOTH", StringType()),
    StructField("WORKEXHOME", StringType()),
    StructField("WORKEXOTH", StringType()),
    StructField("WORKEXMTYP", StringType()),
    StructField("VALDATE", StringType()),
    StructField("REBASEYRCD", StringType()),
    StructField("REBASEYR", StringType()),
    StructField("REPRISRC", StringType()),
    StructField("REPRIPRVAL", StringType()),
    StructField("REPRISDATE", StringType()),
    StructField("RECURRSOUR", StringType()),
    StructField("RECURRPRIC", StringType()),
    StructField("RECURRSALD", StringType()),
    StructField("WSRATIO", StringType()),
    StructField("REENTRY", StringType()),
    StructField("TRANCODE", StringType()),
    StructField("TRANDATE", StringType()),
    StructField("TXSALECDE", StringType()),
    StructField("TXSALEDAT", StringType()),
    StructField("TXSALENUM", StringType()),
    StructField("LEASEHOLD", StringType()),
    StructField("RP1CLACDE", StringType()),
    StructField("RP1NBRCDE", StringType()),
    StructField("QCLPRE", StringType()),
    StructField("REMARKFLA", StringType()),
    StructField("KITCHEN", StringType()),
    StructField("BUILTIN", StringType()),
    StructField("STOREYCODE", StringType()),
    StructField("SUBLEVEL", StringType()),
    StructField("ROOMSFX", StringType()),
    StructField("CONSTTYPE", StringType()),
    StructField("BASELOT", StringType()),
    StructField("BASELOTSFX", StringType()),
    StructField("ZONE", StringType())
])

def main():
    spark = SparkSession.builder.appName("ProcessRollsSanFrancisco").getOrCreate()

    files_map = [{"madr" : "aiamadr_2022_2023.txt", "ownr" : "aiaownr_2022_2023.txt", "secd" : "aiasecd_2022_2023.txt"}]
    for file_map in files_map:
        madr = file_map["madr"]
        ownr = file_map["ownr"]
        secd = file_map["secd"]

        madr_loc = "hdfs://namenode:8020/user/spark/apartments/rolls/sanfrancisco/" + madr

        df = spark.read.text(madr_loc)
        madr_df = df.select(
            df.value.substr(0, 1).alias('CNTL-BYTE'),
            df.value.substr(1, 3).alias('VOL'),
            df.value.substr(3, 12).alias('KEY'),
            df.value.substr(12, 17).alias('BLOCK'),
            df.value.substr(17, 21).alias('BLOCK-N'),
            df.value.substr(21, 22).alias('BLOCK-S')
        )
        madr_df.show(20)

        ownr_loc = "hdfs://namenode:8020/user/spark/apartments/rolls/sanfrancisco/" + ownr
        ownr_df = spark.read.csv(ownr_loc, sep='\t', schema=schema_ownr)
        ownr_df.show(20)

        secd_loc = "hdfs://namenode:8020/user/spark/apartments/rolls/sanfrancisco/" + secd
        secd_df = spark.read.csv(secd_loc, sep='\t', schema=schema_secd)
        secd_df.show(20)




main()