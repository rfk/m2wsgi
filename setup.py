
import sys
setup_kwds = {}
if sys.version_info > (3,):
    from setuptools import setup
    setup_kwds["test_suite"] = "m2wsgi.tests.test_m2wsgi"
    setup_kwds["use_2to3"] = True
else:
    from distutils.core import setup

#  This awfulness is all in aid of grabbing the version number out
#  of the source code, rather than having to repeat it here.  Basically,
#  we parse out all lines starting with "__version__" and execute them.
try:
    next = next
except NameError:
    def next(i):
        return i.next()
info = {}
try:
    src = open("m2wsgi/__init__.py")
    lines = []
    ln = next(src)
    while "__version__" not in ln:
        lines.append(ln)
        ln = next(src)
    while "__version__" in ln:
        lines.append(ln)
        ln = next(src)
    exec("".join(lines),info)
except Exception:
    pass


NAME = "m2wsgi"
VERSION = info["__version__"]
DESCRIPTION = "a mongrel2 => wsgi gateway and helper tools"
LONG_DESC = info["__doc__"]
AUTHOR = "Ryan Kelly"
AUTHOR_EMAIL = "ryan@rfk.id.au"
URL = "http://github.com/rfk/m2wsgi/"
LICENSE = "MIT"
KEYWORDS = "wsgi mongrel2"
CLASSIFIERS = [
    "Programming Language :: Python",
    "Programming Language :: Python :: 2",
#   not tested yet, need to read up on PEP3333...
#    "Programming Language :: Python :: 3",
    "Development Status :: 4 - Beta",
    "License :: OSI Approved :: MIT License"
]


PACKAGES = ["m2wsgi","m2wsgi.tests","m2wsgi.device",
            "m2wsgi.io","m2wsgi.util","m2wsgi.middleware",]
SCRIPTS = ["scripts/m2wsgi",]

setup(name=NAME,
      version=VERSION,
      author=AUTHOR,
      author_email=AUTHOR_EMAIL,
      url=URL,
      description=DESCRIPTION,
      long_description=LONG_DESC,
      keywords=KEYWORDS,
      packages=PACKAGES,
      scripts=SCRIPTS,
      license=LICENSE,
      classifiers=CLASSIFIERS,
      **setup_kwds
     )

