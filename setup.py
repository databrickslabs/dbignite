
import setuptools

setuptools.setup(
    name="dbignite",
    version="0.0.1",
    author="Nathan Buesgens, Amir Kermany, Rachel Sim",
    author_email="labs@databricks.com",
    description="Package for ingesting FHIR bunldes in deltalake",
    url="https://github.com/databrickslabs/dbignite",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: databricks :: OSS",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "dbignite"},
    packages=setuptools.find_packages(where="dbignite"),
    python_requires=">=3.6",
)

# from setuptools import setup

# setup(
#     name='dbignite',
#     version='',
#     packages=['dbignite'],
#     py_modules=['dbignite.data_model']
# )
