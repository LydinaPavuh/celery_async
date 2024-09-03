from setuptools import setup

if __name__ == '__main__':
    setup(
        name='celery_async',
        version='v1.0.0',
        description='Celery asyncio extension',
        packages=["celery_async"],
        install_requires=[
            "celery==5.4.0"
        ],
        entry_points={
            'console_scripts': [
                'celery=celery_async.cmd.worker:main',
            ]
        }
    )