from setuptools import setup

setup(
    name='pgdb2',
    description='pgdb2 database connector',
    author='Rick Albright',
    version="0.2.1",
    requires=['psycopg2', 'sqlalchemy'],
    py_modules=['pgdb2'],
    license='MIT License')
