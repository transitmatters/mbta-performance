#!/bin/bash

# Shrink the deployment package for the lambda layer https://stackoverflow.com/a/69355796
function shrink {
    # Note: zip -d returns non-zero if pattern doesn't match, so we use || true
    zip -d -qq cfn/layer-deployment.zip '*/__pycache__/*' || true
    zip -d -qq cfn/layer-deployment.zip '**/*.pyc' || true
    zip -d -qq cfn/layer-deployment.zip '**/LICENSE*' || true
    zip -d -qq cfn/layer-deployment.zip '**/AUTHOR*' || true
    zip -d -qq cfn/layer-deployment.zip '**/NOTICE*' || true
    zip -d -qq cfn/layer-deployment.zip '**/*.md' || true
    zip -d -qq cfn/layer-deployment.zip '**/*.c' || true
    zip -d -qq cfn/layer-deployment.zip '**/*.cpp' || true
    zip -d -qq cfn/layer-deployment.zip '**/*.h' || true
    zip -d -qq cfn/layer-deployment.zip '**/*.pyx' || true
    zip -d -qq cfn/layer-deployment.zip '**/*.pxd' || true
    # Numpy tests - delete test files but keep _natype.py (needed by numpy.testing at runtime)
    # Note: numpy/_core/tests/_natype.py must be preserved - it's imported by numpy/testing/_private/utils.py
    # Use explicit paths because wildcards with ** can match too broadly
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/numpy/tests/**/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/numpy/tests/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/numpy/_core/tests/test_*.py || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/numpy/_core/tests/_locales.py || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/numpy/_core/tests/data/**/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/numpy/_core/tests/data/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/numpy/_core/tests/examples/**/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/numpy/_core/tests/examples/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/numpy/_pyinstaller/tests/**/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/numpy/_pyinstaller/tests/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/numpy/compat/tests/**/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/numpy/compat/tests/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/numpy/f2py/tests/**/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/numpy/f2py/tests/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/numpy/fft/tests/**/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/numpy/fft/tests/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/numpy/lib/tests/**/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/numpy/lib/tests/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/numpy/linalg/tests/**/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/numpy/linalg/tests/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/numpy/ma/tests/**/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/numpy/ma/tests/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/numpy/matrixlib/tests/**/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/numpy/matrixlib/tests/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/numpy/polynomial/tests/**/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/numpy/polynomial/tests/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/numpy/random/tests/**/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/numpy/random/tests/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/numpy/typing/tests/**/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/numpy/typing/tests/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/numpy/testing/tests/**/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/numpy/testing/tests/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/numpy/conftest.py || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/numpy/_core/*_tests*.so || true
    # Pandas tests
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/pandas*/tests/**/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/pandas/tests/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/pandas/conftest.py || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/pandas/_libs/tslibs/src/datetime/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/pandas/io/formats/templates/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/pandas/io/tests/**/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/pandas/core/tests/**/* || true
    # Pyarrow tests and development files
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/pyarrow*/tests/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/pyarrow*/tests/**/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/pyarrow/include/**/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/pyarrow/include/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/pyarrow/src/**/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/pyarrow/src/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/benchmarks/**/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/benchmarks/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/examples/**/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/examples/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/cmake_modules/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/matplotlib/mpl-data/sample_data/**/* || true
    # SQLAlchemy testing utilities (not needed at runtime)
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/sqlalchemy*/testing/**/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/sqlalchemy*/testing/* || true
    # ddtrace test directories
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/ddtrace/**/test/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/ddtrace/**/tests/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/sqlalchemy/dialects/oracle/**/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/sqlalchemy/dialects/mssql/**/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/sqlalchemy/dialects/mysql/**/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/sqlalchemy/dialects/postgresql/**/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/scipy/datasets/**/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/*/zoneinfo/Africa/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/*/zoneinfo/Asia/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/*/zoneinfo/Atlantic/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/*/zoneinfo/Arctic/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/*/zoneinfo/Antarctica/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/*/zoneinfo/Australia/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/*/zoneinfo/Brazil/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/*/zoneinfo/Chile/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/*/zoneinfo/Europe/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/*/zoneinfo/Indian/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/*/zoneinfo/Pacific/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/*/zoneinfo/Mexico/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/*/zoneinfo/Pacific/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/*/zoneinfo/Singapore || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/*/zoneinfo/Turkey || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/*/zoneinfo/Poland || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/*/zoneinfo/Egypt || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/*/zoneinfo/Hongkong || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/*/zoneinfo/Portugal || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/*/zoneinfo/Libya || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/*/zoneinfo/Kwajalein || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/*/zoneinfo/Zulu || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/*/zoneinfo/I* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/*/zoneinfo/J* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/examples/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/a*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/b*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/co*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/ch*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/cu*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/clean*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/cloud9/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/clouddirectory/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/d*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/e*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/f*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/g*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/h*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/i*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/j*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/k*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/l*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/m*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/n*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/o*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/p*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/q*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/r*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/sa*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/se*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/sn*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/sm*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/sim*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/ss*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/st*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/su*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/t*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/u*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/v*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/w*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/x*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/y*/* || true
    zip -d -qq cfn/layer-deployment.zip python/lib/**/site-packages/boto*/data/z*/* || true
}

# Check if the layer deployment package is under the maximum size
function check_package_size {
    local zipfile="${1:-cfn/layer-deployment.zip}"
    local maximumsize=79100000
    
    if [ ! -f "$zipfile" ]; then
        echo "Error: $zipfile not found"
        exit 1
    fi
    
    actualsize=$(wc -c <"$zipfile")
    difference=$(expr $actualsize - $maximumsize)
    
    echo "$zipfile is $actualsize bytes"
    
    if [ $actualsize -ge $maximumsize ]; then
        echo ""
        echo "$zipfile is over $maximumsize bytes. Shrink the package by $difference bytes to be able to deploy"
        exit 1
    fi
    
    echo "$zipfile is under the maximum size of $maximumsize bytes, by $difference bytes"
}
