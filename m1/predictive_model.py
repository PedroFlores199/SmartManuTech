from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.evaluation import BinaryClassificationEvaluator

spark = SparkSession.builder \
    .appName('SmartManuTech-PredictiveModel') \
    .getOrCreate()

def load_historical_data():
    return spark.read \
        .parquet('s3://smartmanutech-data/historical/') \
        .na.drop()

def train_model(historical_data):
    feature_cols = [
        'avg_temp',
        'max_temp',
        'avg_vibration',
        'avg_speed',
        'energy_trend',
        'hours_since_maintenance'
    ]
    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol='features_raw'
    )
    scaler = StandardScaler(
        inputCol='features_raw',
        outputCol='features',
        withMean=True,
        withStd=True
    )
    rf = RandomForestClassifier(
        labelCol='failure_in_24h',
        featuresCol='features',
        numTrees=100,
        maxDepth=8,
        minInstancesPerNode=5,
        seed=42
    )

    pipeline = Pipeline(stages=[assembler, scaler, rf])
    train_df, test_df = historical_data.randomSplit([0.8, 0.2], seed=42)
    print(f"Train: {train_df.count()} registros | Test: {test_df.count()} registros")
    model = pipeline.fit(train_df)
    evaluator = BinaryClassificationEvaluator(
        labelCol='failure_in_24h',
        metricName='areaUnderROC'
    )
    predictions = model.transform(test_df)
    auc = evaluator.evaluate(predictions)
    print(f"AUC-ROC del modelo: {auc:.4f}")
    rf_model = model.stages[-1]
    importances = list(zip(feature_cols, rf_model.featureImportances))
    importances.sort(key=lambda x: x[1], reverse=True)
    print("\nImportancia de variables:")
    for feat, imp in importances:
        print(f"  {feat}: {imp:.4f}")
    return model

def save_model(model, path='s3://smartmanutech-models/rf_maintenance_v2/'):
    model.save(path)
    print(f"Modelo guardado en: {path}")
if __name__ == '__main__':
    print("Cargando datos historicos...")
    data = load_historical_data()
    print("Entrenando modelo predictivo...")
    model = train_model(data)
    print("Guardando modelo...")
    save_model(model)
    print("Proceso completado.")