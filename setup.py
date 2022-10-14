#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

with open("README.md") as readme_file:
    readme = readme_file.read()

requirements = ["requests", "tqdm", "requests_toolbelt", "pydantic", "tabulate"]

EXTRAS_REQUIRE = {
    "models": ["pandas", "scikit-learn", "numpy"],
}

setup_requirements = []

test_requirements = []

setup(
    author="David Buchmann",
    author_email="david@ntropy.com",
    python_requires=">=3.6",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
    ],
    description="SDK for the Ntropy API",
    entry_points={
        "console_scripts": [
            "ntropy-benchmark = ntropy_sdk.benchmark:main",
        ],
    },
    extras_require=EXTRAS_REQUIRE,
    install_requires=requirements,
    license="MIT license",
    long_description=readme,
    long_description_content_type="text/markdown",
    include_package_data=True,
    keywords="ntropy_sdk",
    name="ntropy_sdk",
    packages=find_packages(include=["ntropy_sdk", "ntropy_sdk.*"]),
    setup_requires=setup_requirements,
    test_suite="tests",
    tests_require=test_requirements,
    url="https://github.com/ntropy-network/ntropy-sdk",
    version="4.11.1",
    zip_safe=False,
)
