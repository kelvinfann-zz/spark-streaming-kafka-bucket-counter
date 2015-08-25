from flask import Flask
from audit_utils.http_endpoint import set_site_mysql

def mysql_http(mysql_host, mysql_usr, mysql_pwd, mysql_db):
	app = Flask(__name__)
	set_site_mysql(app, mysql_host, mysql_usr, mysql_pwd, mysql_db)
	app.run(debug=True)


if __name__ == "__main__":
	mysql_http('localhost', 'root', '', 'logstash')
