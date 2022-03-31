from setuptools import setup, find_packages

with open('README.md', 'r', encoding='utf8') as f:
    readme = f.read()

setup(
    name='gloss',
    version='0.0.1',
    packages=find_packages(exclude=('tests*',)),
    package_data={"gloss": ["py.typed"]},
    author='SparkApplicationMaster',
    author_email='glot.unltd@gmail.com',
    description='PySpark without Java using PyArrow',
    long_description=readme,
    long_description_content_type='text/markdown',
    url='https://github.com/SparkApplicationMaster/gloss',
    license='MIT',
    keywords='gloss pyspark',
    install_requires=[
        'pyarrow==7.0.0'
    ],
    python_requires='>=3.6',
    extras_require={
        'dev': [
        ]
    },
    include_package_data=True,
    #scripts=['publish.py']
)