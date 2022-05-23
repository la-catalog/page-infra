from pathlib import Path

from setuptools import find_packages, setup

long_description = Path("README.md").read_text()

setup(
    name="package-name",
    version="0.0.1",
    description="Short description",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/thiagola92/package-name",
    author="thiagola92",
    author_email="thiagola92@gmail.com",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.10",
        "Operating System :: OS Independent",
    ],
    keywords="template, package, keywords",
    license="MIT",
    packages=find_packages(exclude=["tests"]),
    install_requires=[],
    python_requires=">=3.10",
)
