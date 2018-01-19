import os
from setuptools import setup, find_packages


NAME = 'pyspannerdb'
PACKAGES = find_packages()
DESCRIPTION = 'DB API 2.0 connector for Google Cloud Spanner'
URL = "https://github.com/potatolondon/pyspannerdb"
LONG_DESCRIPTION = open(os.path.join(os.path.dirname(__file__), 'README.md')).read()
AUTHOR = 'Potato London Ltd.'

REQUIREMENTS = [
    'six',
    'pytz',
    'certifi'
]

setup(
    name=NAME,
    version='0.15',
    packages=PACKAGES,

    # metadata for upload to PyPI
    author=AUTHOR,
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    keywords=["database", "Google App Engine", "Cloud Spanner", "GAE"],
    url=URL,
    classifiers=[
        'Development Status :: 1 - Alpha',
        'Topic :: Database',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
    ],

    include_package_data=True,
    # dependencies
    extras_require={ 'all' : REQUIREMENTS }
)
