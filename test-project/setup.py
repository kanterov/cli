# this file accompanies pyproject.toml and packages source code into
# a wheel, which is uploaded as /Workspace file

import datetime

from setuptools import setup

setup(
    # increasing version ensures that library is always
    # re-installed when using existing clusters
    version=datetime.datetime.utcnow().strftime("%Y%m%d.%H%M%S"),
)
