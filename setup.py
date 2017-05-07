# encoding=utf-8
from setuptools import setup

setup(
    name='zzzfs',
    version='0.1.2',
    description='Dataset management à la ZFS',
    long_description=open('README').read(),
    author='Daniel W. Steinbrook',
    author_email='steinbro@post.harvard.edu',
    url='https://github.com/steinbro/zzzfs',
    license="CDDL",
    keywords='zfs',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Topic :: System :: Filesystems',
    ],
    packages=['libzzzfs', 'libzzzfs.cmd'],
	test_suite='tests',
    entry_points={
        'console_scripts': [
            'zzzfs = libzzzfs.cmd.zzzfs:main',
            'zzzpool = libzzzfs.cmd.zzzpool:main',
        ],
    },
)
