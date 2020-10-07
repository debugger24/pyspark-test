from setuptools import setup

def readme():
    with open('README.md') as f:
        return f.read()
    
setup(
    name='assert_pyspark_df_equal',
    version='0.0.1',
    description='',
    long_description=readme(),
    long_description_content_type='text/markdown',
    url='https://github.com/debugger24/pyspark-df-assert',
    author='Rahul Kumar',
    author_email='rahulcomp24@gmail.com',
    keywords='assert pyspark unit test testing compare',
    license='Apache Software License',
    py_modules=['assert_pyspark_df_equal'],
    package_dir={'': 'src'},
    install_requires=[
        "pyspark>=2.1.2"
    ]
)