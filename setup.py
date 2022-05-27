from pathlib import Path

from setuptools import find_packages, setup

long_description = Path("README.md").read_text()

setup(
    name="page-sender",
    version="0.0.1",
    description="Responsable for interacting with others storages infrastructures",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/thiagola92/page-sender",
    author="thiagola92",
    author_email="thiagola92@gmail.com",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.10",
        "Operating System :: OS Independent",
    ],
    keywords="database, amqp",
    license="MIT",
    packages=find_packages(exclude=["tests"]),
    install_requires=[
        "aio-pika>=8.0.0",
        "aioredis>=2.0.1",
        "motor>=3.0.0",
        "structlog>=21.5.0",
    ],
    python_requires=">=3.10",
)
