.PHONY package
package:
	python setup.py sdist

.PHONY upload
upload:
	aws s3 cp dist/spark_ml-0.0.1.tar.gz s3://aws-logs-947634201780-us-west-2/code/
