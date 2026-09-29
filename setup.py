from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="temperature-prediction-pyspark",
    version="0.1.0",
    author="Data Science Team",
    description="Temperature prediction model using PySpark for smart manufacturing",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.9",
    install_requires=[
        "pyspark>=3.5.0",
        "numpy>=2.0.0",
        "pandas>=2.1.0",
        "scikit-learn>=1.3.0",
        "pyyaml>=6.0.0",
        "python-dotenv>=1.0.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.4.0",
            "pytest-cov>=4.1.0",
            "matplotlib>=3.8.0",
            "seaborn>=0.13.0",
        ],
    },
)