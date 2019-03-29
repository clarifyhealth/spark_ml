package:
	python setup.py sdist

upload:
	aws s3 cp dist/spark_ml-0.0.1.tar.gz s3://workbenches-emr-misc/code/

uploadsc:
	aws s3 cp scripts/install_pip3_dependencies.sh s3://workbenches-emr-misc/scripts/
	aws s3 cp scripts/install_pipeline.sh s3://workbenches-emr-misc/scripts/
	aws s3 cp scripts/install_user.sh s3://workbenches-emr-misc/scripts/
	aws s3 cp scripts/install_redshift_jars.sh s3://workbenches-emr-misc/scripts/