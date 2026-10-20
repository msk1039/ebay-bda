@echo off
REM Recreate the project environment on Windows.
REM Usage:  setup.bat          (flexible versions from requirements.txt)
REM         setup.bat --lock   (exact versions from requirements-lock.txt; needs Python 3.14)
setlocal
cd /d "%~dp0"

set REQ=requirements.txt
if "%~1"=="--lock" set REQ=requirements-lock.txt

REM 1. Python 3.10+ is required by PySpark 4.2
where python >nul 2>nul || (echo ERROR: python not found. Install Python 3.10+ from python.org and tick "Add to PATH". & exit /b 1)
python -c "import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)" || (echo ERROR: Python 3.10 or newer is required. & exit /b 1)
python --version

REM 2. Java 17 or 21 is required by Spark (warn only)
where java >nul 2>nul && (java -version) || echo WARNING: Java not found. Install Java 17 or 21 (Eclipse Temurin) and set JAVA_HOME.

REM 3. Virtual environment + packages
python -m venv .venv || exit /b 1
call .venv\Scripts\activate.bat
python -m pip install --upgrade pip
pip install -r %REQ% || exit /b 1

REM 4. Register the Jupyter kernel the notebooks expect ("Python (fa2-olist)")
python -m ipykernel install --sys-prefix --name fa2-olist --display-name "Python (fa2-olist)"

REM 5. Dataset check
if not exist datasets\olist-dataset\olist_orders_dataset.csv (
  echo.
  echo NOTE: dataset missing. Download it from https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
  echo and unzip the 9 CSV files into datasets\olist-dataset\  ^(see README.md^).
)

echo.
echo Done. Next:  .venv\Scripts\activate  then  jupyter lab fa2\notebooks\fa2_olist_spark.ipynb
endlocal
