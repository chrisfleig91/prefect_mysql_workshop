FROM prefecthq/prefect:2.14.17-python3.10

# Installiere notwendige Pakete
RUN pip install pandas openpyxl mysql-connector-python
