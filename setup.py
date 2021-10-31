from setuptools import setup


def readme():
    with open("README.md") as f:
        return f.read()


setup(
    name="pyspark_test",
    version="0.2.0",
    description="Check that left and right spark DataFrame are equal.",
    long_description=readme(),
    long_description_content_type="text/markdown",
    url="https://github.com/debugger24/pyspark-test",
    author="Rahul Kumar",
    author_email="rahulcomp24@gmail.com",
    keywords="assert pyspark unit test testing compare",
    license="Apache Software License (Apache 2.0)",
    py_modules=["pyspark_test"],
    package_dir={"": "src"},
    install_requires=["pyspark>=2.1.2"],
)
