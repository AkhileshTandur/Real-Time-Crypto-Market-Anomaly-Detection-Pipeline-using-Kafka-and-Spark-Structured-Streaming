$ErrorActionPreference = "Stop"

$pythonRoot = Join-Path $env:LOCALAPPDATA "Programs\Python\Python313"
$pythonExe = Join-Path $pythonRoot "python.exe"
$sparkHome = Join-Path $pythonRoot "Lib\site-packages\pyspark"
$sparkSubmit = Join-Path $pythonRoot "Scripts\spark-submit.cmd"
$javaHome = "C:\Program Files\Java\jdk-17"
$ivyHome = Join-Path (Get-Location) ".spark-ivy"
$userIvyJars = "C:\Users\Akhilesh Tandur\.ivy2\jars"

if (!(Test-Path $pythonExe)) {
    throw "Python executable not found: $pythonExe"
}

if (!(Test-Path $sparkSubmit)) {
    throw "spark-submit not found. Run: py -m pip install -r requirements.txt"
}

if (!(Test-Path (Join-Path $sparkHome "jars"))) {
    throw "PySpark jars not found. Run: py -m pip install --force-reinstall pyspark"
}

if (!(Test-Path (Join-Path $javaHome "bin\java.exe"))) {
    throw "Java 17 not found at $javaHome. Install JDK 17 or update this script's `$javaHome path."
}

$env:JAVA_HOME = $javaHome
$env:PATH = "$(Join-Path $javaHome 'bin');$env:PATH"
$env:SPARK_HOME = $sparkHome
$env:PYSPARK_PYTHON = $pythonExe
$env:PYSPARK_DRIVER_PYTHON = $pythonExe

New-Item -ItemType Directory -Force -Path $ivyHome | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $ivyHome "cache") | Out-Null

$submitArgs = @(
    "--conf", "spark.jars.ivy=$ivyHome"
)

$requiredJarPatterns = @(
    "org.apache.spark_spark-sql-kafka-0-10_2.12-3.5.0.jar",
    "org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.5.0.jar",
    "org.apache.kafka_kafka-clients-*.jar",
    "org.lz4_lz4-java-*.jar",
    "org.xerial.snappy_snappy-java-*.jar",
    "org.slf4j_slf4j-api-*.jar",
    "org.apache.commons_commons-pool2-*.jar"
)

$localJars = @()
if (Test-Path $userIvyJars) {
    foreach ($pattern in $requiredJarPatterns) {
        $localJars += Get-ChildItem -Path $userIvyJars -Filter $pattern -ErrorAction SilentlyContinue | Select-Object -ExpandProperty FullName
    }
}

if ($localJars.Count -gt 0) {
    $submitArgs += @("--jars", (($localJars | Select-Object -Unique) -join ","))
} else {
    $submitArgs += @("--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
}

$submitArgs += @(
    "streaming/spark_stream_kafka_crypto_clean_aggregate.py",
    "--bootstrap_servers", "localhost:9092",
    "--topic", "crypto.trades",
    "--out_path", "data\stream\aggregates_csv",
    "--checkpoint_path", "data\stream\checkpoints\agg",
    "--window_seconds", "10",
    "--watermark_seconds", "10",
    "--sink", "csv"
)

& $sparkSubmit @submitArgs
