"""

m2wsgi.__main__:  allow m2wsgi to be executed directly by python -m
===================================================================

This is a simple script that calls the m2wsgi.main() function.  It allows
python 2.7 and later to execute the m2wsgi package with `python -m m2wsgi`.

"""
#  Copyright (c) 2011, Ryan Kelly.
#  All rights reserved; available under the terms of the MIT License.


import m2wsgi

if __name__ == "__main__":
    m2wsgi.main()

