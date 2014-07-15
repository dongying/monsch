# -*- coding: utf-8 -*-

from setuptools import setup

import monsch


setup(
    name=monsch.__name__,
    version=monsch.__version__,
    author="Dongying Zhang",
    author_email="zhdongying@gmail.com",
    description="A very simple MongoDB ORM based on PyMongo and a fork version of Schema.",
    license="MIT",
    keywords="mongodb orm monsch",
    url="https://github.com/Dongying/monsch",
    py_modules=['monsch'],
    long_description=open('README.md').read(),
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Development Status :: 3 - Alpha',
        'Environment :: Other Environment',
        'Intended Audience :: Developers',
        'Topic :: Utilities',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Database',
        'Programming Language :: Python :: 2.7',
    ],
    install_requires=[
        'pymongo>=2.5',
    ],
)
