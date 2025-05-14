from setuptools import setup, find_packages

setup(
    name="data-generator",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "pyyaml>=6.0",
        "kafka-python>=2.0.2",
        "boto3>=1.24.0",
        "sqlalchemy>=1.4.0",
        "pandas>=1.4.0",
        "python-dateutil>=2.8.2",
        "tweepy>=4.10.0",
        "jsonschema>=4.16.0",
    ],
    entry_points={
        "console_scripts": [
            "data-generator=src.main:main",
        ],
    },
)
