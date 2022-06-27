


#sc.stop()
import pyspark 
from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql.types import IntegerType
import warnings
import numpy as np
import pandas as pd
warnings.filterwarnings('ignore')
from pyspark.sql.functions import *
sc=SparkContext('local[16]')



path='/data/dataprocessing/interproscan/all_bacilli.tsv'
df = SQLContext(sc).read.csv(path, sep=r'\t', header=None)





#Renaming the columns
df=df.withColumnRenamed("_c0","Protein")\
   .withColumnRenamed("_c1","Sequence_MD5_digest")\
    .withColumnRenamed("_c2","Sequence_length")\
    .withColumnRenamed("_c3","Analysis")\
    .withColumnRenamed("_c4","Signature_accession")\
    .withColumnRenamed("_c5","Signature_description")\
    .withColumnRenamed("_c6","Start")\
    .withColumnRenamed("_c7","Stop")\
    .withColumnRenamed("_c8","Score")\
    .withColumnRenamed("_c9","Status")\
    .withColumnRenamed("_c10","Date")\
    .withColumnRenamed("_c11","Interpro_accession")\
    .withColumnRenamed("_c12","Interpro_description")\
    .withColumnRenamed("_c13","GO")\
    .withColumnRenamed("_c14","Pathway")
columns=df.columns   
df.show()




def explain(data):
    return data._sc._jvm.PythonSQLUtils.explainString(data._jdf.queryExecution(), 'simple')



#1. How many distinct protein annotations are found in the dataset? I.e. how many distinc InterPRO numbers are there?

prot_annot=df.select('Interpro_accession').filter(df['Interpro_accession'] != "-")\
.distinct()#.count()
#prot_annot
exp1 = explain(prot_annot)
q1=prot_annot.count()



#2  How many annotations does a protein have on average?
count=df.select("Protein",'Interpro_accession')\
            .filter(df['Interpro_accession'] != "-")\
            .groupBy("Protein")\
            .agg({'Interpro_accession':'count'})\
            .select(mean('count(Interpro_accession)').alias('mean'))#.collect()
exp2 = explain(count)        
q2 = count.collect()[0].__getitem__(0)


# ## 3. What is the most common GO Term found?

df3 = df.select(df['GO'], explode(split(col("GO"),"\|"))\
                    .alias("Split_col"))
df3 = df3.filter(df3.Split_col != "-")\
            .select("Split_col")\
            .groupby("Split_col")\
            .count()\
            .sort("count",ascending=False)
exp3 = explain(df3)
df3 = df3.take(1)
q3 = df3[0]


# ## 4. What is the average size of an InterPRO feature found in the dataset?

df4=df.withColumn("feat_len", (df['Stop']-df['Start'])/(df['Sequence_length'])).summary('mean')
exp4 = explain(df4)
q4 = df4.collect()[0].__getitem__(-1)


# ## 5. What is the top 10 most common InterPRO features?

df5=df.select('Interpro_accession')\
            .filter(df['Interpro_accession'] != "-")\
            .groupBy('Interpro_accession')\
            .count()\
            .sort("count",ascending=False)\
            .select("InterPro_accession")
            
exp5 = explain(df5)
q5 = df5.take(10)

            


# ## 6. If you select InterPRO features that are almost the same size (within 90-100%) as the protein itself, what is the top10 then?

df6=df.withColumn("feat_len", (df['Stop']-df['Start'])/(df['Sequence_length'])).filter(df['InterPro_accession'] != "-")
df6=df6.filter(df6['feat_len']>.9).sort('feat_len',ascending=False).select('Interpro_accession')#.take(10)

exp6 = explain(df6)

q6=[ x[0] for x in df6.take(10)]


# ## 7. If you look at those features which also have textual annotation, what is the top 10 most common word found in that annotation?

df7 = df.select(df.Interpro_description,explode(split(col("Interpro_description")," |,"))\
            .alias("Split_col"))
df7 = df7.select("Split_col")\
            .filter(df7.Split_col != "")\
            .filter(df7.Split_col != "-")\
            .groupby("Split_col")\
            .count()\
            .sort("count",ascending=False)\
            .select("Split_col")
exp7 = explain(df7)
q7 =  df7.take(10)


# ## 8. And the top 10 least common?

df8 = df.select(df.Interpro_description,explode(split(col("Interpro_description")," |,"))\
            .alias("Split_col"))
df8 = df8.select("Split_col")\
            .filter(df8.Split_col != "")\
            .filter(df8.Split_col != "-")\
            .groupby("Split_col")\
            .count()\
            .sort("count",ascending=True)\
            .select("Split_col")
exp8 = explain(df8)
q8 = df8.take(10)


# ## 9.Combining your answers for Q6 and Q7, what are the 10 most commons words found for the largest InterPRO features?

df9 = df.select(df['Interpro_accession'],df['Interpro_description'])\
            .filter(df.Interpro_accession.isin(q6))\
            .distinct()
df9 = df9.select(df9.Interpro_description,explode(split(col("Interpro_description")," |,")))\
                .groupby("col")\
                .count()
df9 = df9.select(df9["col"], df9["count"])\
                .filter(df9["col"] != "")\
                .sort("count",ascending=False)
exp9 = explain(df9)
q9 = [x[0] for x in df9.take(10)]


# ## 10. What is the coefficient of correlation ($R^2$) between the size of the protein and the number of features found?

df10=df.select('Protein','Interpro_accession','Sequence_length')\
            .filter(df.Interpro_accession != "-")\
            .groupby("Protein","Sequence_length")\
            .count()
df10 = df10.withColumn("Sequence_length", df10["Sequence_length"].cast(IntegerType()))
exp10 = explain(df10)

q10 = df10.corr('Sequence_length', 'count')**2


col1 = np.arange(1,11)
col2 = [q1,q2,q3,q4,q5,q6,q7,q8,q9,q10]
col3 = [exp1,exp2,exp3,exp4,exp5,exp6,exp7,exp8,exp9,exp10]
dict = {'Questions': col1, 'Answers': col2,"Explain":col3}
results_df = pd.DataFrame(dict)


results_df.to_csv('/homes/dthomas/Dilton_Hanze/Programming3/Assignment5/Results.csv')

