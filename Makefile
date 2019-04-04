package:
	python setup.py sdist

upload:
	aws s3 cp dist/spark_ml-0.0.1.tar.gz s3://workbenches-emr-misc/code/
