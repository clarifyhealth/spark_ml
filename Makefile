.PHONY package
package:
	python setup.py sdist

.PHONY upload
upload:
	aws s3 cp dist/spark_ml-0.0.1.tar.gz s3://aws-logs-947634201780-us-west-2/code/

.PHONY uploadscripts
uploadscripts:
	aws s3 cp scripts/install_pip3_dependencies.sh s3://aws-logs-947634201780-us-west-2/scripts/
	aws s3 cp scripts/install_pipeline.sh s3://aws-logs-947634201780-us-west-2/scripts/