from setuptools import setup

VERSION = '0.0.1'

install_requires=[
        'requests==2.22.0', 
        'argparse==1.4.0']

tests_require=['nose==1.3.7']


setup(
    name='flink-scrat',
    packages=['flink_scrat'],
    version=VERSION,
    description='Python client to deploy flink applications to remote clusters via the flink cluster API',
    url='https://github.com/Cobliteam/flink-scrat',
    download_url='https://github.com/Cobliteam/flink-scrat/archive/{}.tar.gz'.format(VERSION),
    author='Nicolau Tahan',
    author_email='nicolau.tahan@cobli.co',
    license='MIT',
    install_requires=install_requires,
    tests_require=tests_require,
    test_suite="nose.collector",
    entry_points={
        'console_scripts': ['flink-scrat=flink_scrat.main:main']
    },
    keywords='flink deploy emr aws')
