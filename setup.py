from distutils.core import setup
from Cython.Build import cythonize
import numpy as np

setup(
  name = 'postgres-binary-parser',
  ext_modules = cythonize("postgres_binary_parser/psql_binary.pyx"),
  include_dirs=[np.get_include()],
  install_requires=['pandas', 'numpy']
)
