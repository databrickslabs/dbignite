from setuptools import setup

setup(
    name="dbignite",
    version="0.0.1",
    author="Nathan Buesgens, Amir Kermany, Rachel Sim",
    author_email="labs@databricks.com",
    description="Package for ingesting FHIR bunldes in deltalake",
    url="https://github.com/databrickslabs/dbignite",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: Other/Proprietary License",
        "Operating System :: OS Independent",
    ],
    packages=['dbignite'],
    py_modules=['dbignite.data_model']
)
