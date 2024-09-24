from setuptools import setup
from io import open
from os import path
import sys

DESCRIPTION = "Package for ingesting FHIR bunldes in deltalake"
this_directory = path.abspath(path.dirname(__file__))

with open(path.join(this_directory, "README.md"), encoding="utf-8") as f:
    LONG_DESCRIPTION = f.read()

try:
    exec(open("dbignite/version.py").read())
except IOError:
    print("Failed to load version file for packaging.", file=sys.stderr)
    sys.exit(-1)
VERSION = __version__

setup(
    name="dbignite",
    version=VERSION,
    python_requires='>=3.9',
    author="Amir Kermany, Nathan Buesgens, Rachel Sim, Aaron Zavora, William Smith, Jesse Young",
    author_email="labs@databricks.com",
    description= DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    url="https://github.com/databrickslabs/dbignite",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: Other/Proprietary License",
        "Operating System :: OS Independent",
    ],
    packages=['dbignite', 'dbignite.omop', 'dbignite.hosp_feeds', 'dbignite.writer'],
    package_data={'': ["schemas/*.json"]},
    py_modules=['dbignite.data_model']
)
