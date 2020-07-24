# Import modules
from pyspark.sql import SparkSession, functions as F
from pyspark.ml.feature import StandardScaler, VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator

# Create connection and load csv
spark = SparkSession.builder.getOrCreate()
file_path = "bank-full.csv"

df = spark.read.csv(file_path, header = True, inferSchema = True)

# Drop columns that are not needed for our analysis
df = df.drop("contact", "poutcome", "day")

# Discard rows with missing values
df = df.filter(df.education != "unknown").filter(df.job != "unknown")
df.count()

# Bucketize the pdays feature
df = df.withColumn("pdays", F.when(df.pdays < 0, "No contact")
                                 .when(df.pdays <= 7, "Within Week")
                                 .when(df.pdays <= 30, "Within Month")
                                 .when(df.pdays <= 90, "Within Trimester")
                                 .when(df.pdays <= 180, "Within Semester")
                                 .when(df.pdays <= 365, "Within Year")
                                 .when(df.pdays <= 730, "Within Two Years")
                                 .otherwise("More than Two Years") )

# Scale the data
feature_list = ["age", "balance", "duration", "campaign", "previous"]
assemblers = [VectorAssembler(inputCols=[col], outputCol=col + "_vec") for col in feature_list]
scalers = [StandardScaler(inputCol=col + "_vec", outputCol=col + "_scaled") for col in feature_list]

# String Indexers
labeled_cols = ["default", "housing", "loan", "y"]
encoded_cols = ["job", "marital", "education", "month", "pdays"]
indexers = [StringIndexer().setInputCol(col).setOutputCol(col + "_labeled") for col in labeled_cols + encoded_cols]

# One Hot Encoders
encoders = [OneHotEncoder(inputCol = col + "_labeled", outputCol = col + "_encoded") for col in encoded_cols]    

# Create the preprocessing timeline
input_stages = assemblers + scalers + indexers + encoders
pipeline = Pipeline(stages = input_stages)
preprocessor = pipeline.fit(df)
df = preprocessor.transform(df)

# Remove unnecessary columns
labeled_cols += [s + "_labeled" for s in encoded_cols]
feature_list += [s + "_vec" for s in feature_list]
df = df.drop(*feature_list, *labeled_cols, *encoded_cols, "features")

# Write the pre-processed data to a csv file
# df.write.csv("bank-processed.csv", header = True)
from datetime import datetime
date = datetime.now().strftime("%x").replace('/','-')
fname = "bank-processed-" + date
df.rdd.saveAsTextFile(fname)
