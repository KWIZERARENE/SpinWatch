"""
Common PySpark JDBC Database Connection Config
"""

JDBC_URL = "jdbc:mysql://127.0.0.1:3306/laundry_ops"
JDBC_PROPERTIES = {
    "user": "root",
    "password": "",
    "driver": "com.mysql.cj.jdbc.Driver"
}
