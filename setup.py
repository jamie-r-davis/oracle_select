from setuptools import setup


def readme():
    with open('README.md') as f:
        return f.read()
        
        
setup(name='oracle_select',
      version='0.1',
      description='Easy interface for reading from Oracle databases',
      long_description=readme(),
      classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.6',
        'Topic :: Databases :: Oracle',
      ],
      keywords='oracle cx_Oracle',
      author='Jamie Davis',
      author_email='jamjam@umich.edu',
      license='MIT',
      packages=['oracle_select'],
      install_requires=[
          'cx_Oracle',
      ],
      extras_require={
        'utils': ['pandas', 'IPython']
      },
      include_package_data=True,
      zip_safe=False)