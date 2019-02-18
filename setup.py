from setuptools import setup, find_packages
setup(
    name='haas_agent',
    version='0.0.1',
    packages=find_packages(),
    zip_safe=False,
    platforms='any',
    install_requires=[
        'Flask==1.0.2',
        'requests==2.20.0',
        'Flask-Cors==3.0.6',
        'apscheduler==3.5.1',
        'SQLAlchemy==1.2.0',
        'flask_sqlalchemy',
        'kafka==1.3.5',
        'pymongo==3.7.1',
	'pyhive',
	'thrift',
	'sasl',
	'thrift_sasl',
	'azure==4.0.0',
	'azure-storage-file'
#'confluent-kafka==0.11.6'

    ],

    classifiers=[
        'Environment :: Web Environment'
           ]
)

