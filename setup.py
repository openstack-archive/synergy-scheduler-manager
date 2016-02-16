# coding=utf-8
from setuptools import setup, find_packages


setup(
    name='synergy-scheduler-manager',
    version='0.1',
    description='Provide advanced scheduling (fairshare) capability for OpenStack',
    url='https://launchpad.net/synergy-scheduler-manager',
    author='Lisa Zangrando',
    license='Apache 2',
    packages=find_packages(),
    install_requires=[
        'synergy-service',
        'DBUtils',
        'mysql-connector-python-rf',
        'MySQL-python',
        'oslo.config<2.0.0',
        'oslo.messaging<2.0.0'],
    entry_points={
        'synergy.managers': [
            'fairshare = synergy_scheduler_manager.fairshare:FairShareManager',
            'queue = synergy_scheduler_manager.queue:QueueManager',
            'quota = synergy_scheduler_manager.quota:QuotaManager',
        ],
    },
)
