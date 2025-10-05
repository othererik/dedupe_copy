from distutils.core import setup


def read_readme():
    """Read the README.md file and return its content."""
    with open("README.md", encoding="utf-8") as f:
        return f.read()


setup(
    name="DedupeCopy",
    version="1.0.0",
    author="Erik Schweller",
    author_email="othererik@gmail.com",
    packages=[
        "dedupe_copy",
        "dedupe_copy.test",
    ],
    url="http://pypi.python.org/pypi/DedupeCopy/",
    download_url="http://www.github.com/othererik/dedupe_copy",
    scripts=[
        "dedupe_copy/bin/dedupecopy",
    ],
    license="Simplified BSD License",
    long_description=read_readme(),
    description="Find duplicates / copy and restructure file layout "
    "command-line tool",
    platforms=["any"],
    keywords=[
        "de-duplication",
        "file management",
    ],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Environment :: Console",
        "Intended Audience :: End Users/Desktop",
        "Intended Audience :: Developers",
        "Programming Language :: Python",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Topic :: Utilities",
    ],
)
